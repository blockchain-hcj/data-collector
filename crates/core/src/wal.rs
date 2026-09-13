use crate::record::Record;
use crate::types::StreamId;
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

pub const MAGIC: u32 = u32::from_le_bytes(*b"DCLG");
pub const VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 8 + 4 + 4;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode: {0}")]
    Encode(String),
    #[error("corrupt sealed segment {path:?} at byte {at}: {msg}")]
    CorruptSealed { path: PathBuf, at: u64, msg: String },
    #[error("stream locked: {0}")]
    Locked(PathBuf),
}

pub type Result<T> = std::result::Result<T, WalError>;

pub struct WalWriter {
    pub stream: StreamId,
    dir: PathBuf,
    pub epoch: Uuid,
    _lock: File,
    current: File,
    current_path: PathBuf,
    current_bytes: u64,
    current_opened_ms: i64,
    next_offset: u64,
    written_offset: u64,
    durable_offset: u64,
    unsynced_bytes: u64,
    last_sync: Instant,
    segment_max_bytes: u64,
    segment_max_ms: u64,
    sync_bytes: u64,
    sync_interval: std::time::Duration,
}

pub struct WalReader {
    dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

impl WalRecord {
    pub fn decode(&self) -> anyhow::Result<Record> {
        Record::decode(&self.bytes)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn fsync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        fsync_dir(dir)?;
    }
    Ok(())
}

fn segment_path(dir: &Path, first_offset: u64) -> PathBuf {
    dir.join(format!("{first_offset:020}.log"))
}

pub fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for ent in fs::read_dir(dir)? {
        let ent = ent?;
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".log") {
            if let Ok(off) = stem.parse::<u64>() {
                out.push((off, ent.path()));
            }
        }
    }
    out.sort_by_key(|(o, _)| *o);
    Ok(out)
}

fn parse_header(buf: &[u8; HEADER_LEN]) -> Option<(u64, u32, u32)> {
    let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
    if magic != MAGIC {
        return None;
    }
    let ver = u16::from_le_bytes(buf[4..6].try_into().ok()?);
    if ver != VERSION {
        return None;
    }
    let offset = u64::from_le_bytes(buf[6..14].try_into().ok()?);
    let len = u32::from_le_bytes(buf[14..18].try_into().ok()?);
    let crc = u32::from_le_bytes(buf[18..22].try_into().ok()?);
    Some((offset, len, crc))
}

/// Scan a segment. If `sealed`, mid-file corruption is fatal.
/// If not sealed, truncate after the last good record.
fn scan_segment(path: &Path, sealed: bool) -> Result<(u64, u64, u64)> {
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = f.metadata()?.len();
    let mut pos = 0u64;
    let mut last_offset = None;
    let mut first_offset = None;
    loop {
        if pos + HEADER_LEN as u64 > file_len {
            if pos != file_len {
                if sealed && pos < file_len {
                    return Err(WalError::CorruptSealed {
                        path: path.to_path_buf(),
                        at: pos,
                        msg: "truncated header in sealed segment".into(),
                    });
                }
                f.set_len(pos)?;
            }
            break;
        }
        let mut hdr = [0u8; HEADER_LEN];
        f.seek(SeekFrom::Start(pos))?;
        match f.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if sealed {
                    return Err(WalError::CorruptSealed {
                        path: path.to_path_buf(),
                        at: pos,
                        msg: "eof reading header".into(),
                    });
                }
                f.set_len(pos)?;
                break;
            }
            Err(e) => return Err(e.into()),
        }
        let Some((offset, len, crc)) = parse_header(&hdr) else {
            if sealed {
                return Err(WalError::CorruptSealed {
                    path: path.to_path_buf(),
                    at: pos,
                    msg: "bad magic/version".into(),
                });
            }
            f.set_len(pos)?;
            break;
        };
        let payload_end = pos + HEADER_LEN as u64 + u64::from(len);
        if payload_end > file_len {
            if sealed {
                return Err(WalError::CorruptSealed {
                    path: path.to_path_buf(),
                    at: pos,
                    msg: "truncated payload".into(),
                });
            }
            f.set_len(pos)?;
            break;
        }
        let mut payload = vec![0u8; len as usize];
        f.read_exact(&mut payload)?;
        if crc32c::crc32c(&payload) != crc {
            if sealed {
                return Err(WalError::CorruptSealed {
                    path: path.to_path_buf(),
                    at: pos,
                    msg: "crc mismatch".into(),
                });
            }
            f.set_len(pos)?;
            break;
        }
        if first_offset.is_none() {
            first_offset = Some(offset);
        }
        last_offset = Some(offset);
        pos = payload_end;
    }
    let next = last_offset.map(|o| o + 1).unwrap_or(0);
    let durable = last_offset.unwrap_or(0);
    Ok((first_offset.unwrap_or(0), durable, next))
}

fn recover_segments(
    dir: &Path,
    segments: &mut Vec<(u64, PathBuf)>,
) -> Result<(u64, u64, PathBuf, u64)> {
    if segments.is_empty() {
        let p = segment_path(dir, 0);
        File::create(&p)?;
        fsync_dir(dir)?;
        return Ok((0, 0, p, 0));
    }
    let last_i = segments.len() - 1;
    for (i, (_, path)) in segments.iter().enumerate() {
        let sealed = i != last_i;
        match scan_segment(path, sealed) {
            Ok(_) => {}
            Err(e) if sealed => {
                // Kill during rotate can leave trailing bytes on the previous file.
                // Truncate the tail; a true hole in the middle still errors.
                tracing::warn!(path = %path.display(), error = %e, "sealed segment tail repair");
                let _ = scan_segment(path, false)?;
            }
            Err(e) => return Err(e),
        }
    }
    let last_path = segments[last_i].1.clone();
    let bytes = fs::metadata(&last_path)?.len();
    let (_first, dur, nxt) = scan_segment(&last_path, false)?;
    let durable = if nxt == 0 { 0 } else { dur };
    Ok((durable, nxt, last_path, bytes))
}

pub fn read_durable_offset(dir: &Path) -> Result<u64> {
    let p = dir.join("durable_offset");
    if !p.exists() {
        return Ok(0);
    }
    let s = fs::read_to_string(p)?;
    Ok(s.trim().parse().unwrap_or(0))
}

pub fn read_epoch(dir: &Path) -> Result<Option<Uuid>> {
    let p = dir.join("epoch");
    if !p.exists() {
        return Ok(None);
    }
    let s = fs::read_to_string(p)?;
    Ok(Some(Uuid::parse_str(s.trim()).map_err(|e| {
        WalError::Encode(format!("bad epoch: {e}"))
    })?))
}

impl WalWriter {
    pub fn open(
        wal_root: &Path,
        stream: StreamId,
        segment_max_bytes: u64,
        segment_max_ms: u64,
        sync_bytes: u64,
        sync_interval: std::time::Duration,
    ) -> Result<Self> {
        let dir = wal_root.join(stream.rel_dir());
        fs::create_dir_all(&dir)?;
        let lock_path = dir.join("stream.lock");
        let lock = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        lock.try_lock_exclusive()
            .map_err(|_| WalError::Locked(lock_path.clone()))?;

        let epoch_path = dir.join("epoch");
        let epoch = if epoch_path.exists() {
            let s = fs::read_to_string(&epoch_path)?;
            Uuid::parse_str(s.trim()).map_err(|e| WalError::Encode(format!("bad epoch: {e}")))?
        } else {
            let e = Uuid::new_v4();
            atomic_write(&epoch_path, e.to_string().as_bytes())?;
            e
        };

        let mut segments = list_segments(&dir)?;
        let (durable, next, current_path, current_bytes) =
            recover_segments(&dir, &mut segments)?;

        let current = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&current_path)?;

        let w = Self {
            stream,
            dir,
            epoch,
            _lock: lock,
            current,
            current_path,
            current_bytes,
            current_opened_ms: now_ms(),
            next_offset: next,
            written_offset: if next == 0 { 0 } else { next - 1 },
            durable_offset: durable,
            unsynced_bytes: 0,
            last_sync: Instant::now(),
            segment_max_bytes,
            segment_max_ms,
            sync_bytes,
            sync_interval,
        };
        w.write_durable_file()?;
        Ok(w)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn written_offset(&self) -> u64 {
        self.written_offset
    }

    pub fn durable_offset(&self) -> u64 {
        self.durable_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn active_path(&self) -> &Path {
        &self.current_path
    }

    pub fn append(&mut self, rec: &Record) -> Result<u64> {
        let payload = rec
            .encode()
            .map_err(|e| WalError::Encode(e.to_string()))?;
        let offset = self.next_offset;
        let crc = crc32c::crc32c(&payload);
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        hdr[4..6].copy_from_slice(&VERSION.to_le_bytes());
        hdr[6..14].copy_from_slice(&offset.to_le_bytes());
        hdr[14..18].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        hdr[18..22].copy_from_slice(&crc.to_le_bytes());
        self.current.write_all(&hdr)?;
        self.current.write_all(&payload)?;
        let n = (HEADER_LEN + payload.len()) as u64;
        self.current_bytes += n;
        self.unsynced_bytes += n;
        self.next_offset = offset + 1;
        self.written_offset = offset;
        if self.should_rotate() {
            self.sync()?;
            self.rotate()?;
        }
        Ok(offset)
    }

    fn should_rotate(&self) -> bool {
        if self.current_bytes >= self.segment_max_bytes {
            return true;
        }
        let age = (now_ms() - self.current_opened_ms) as u64;
        age >= self.segment_max_ms && self.current_bytes > 0
    }

    pub fn maybe_sync(&mut self) -> Result<()> {
        let due = self.last_sync.elapsed() >= self.sync_interval;
        if self.unsynced_bytes >= self.sync_bytes || (due && self.unsynced_bytes > 0) {
            self.sync()?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.unsynced_bytes == 0 {
            return Ok(());
        }
        self.current.sync_data()?;
        self.durable_offset = if self.next_offset == 0 {
            0
        } else {
            self.next_offset - 1
        };
        self.unsynced_bytes = 0;
        self.last_sync = Instant::now();
        self.write_durable_file()?;
        Ok(())
    }

    fn write_durable_file(&self) -> Result<()> {
        atomic_write(
            &self.dir.join("durable_offset"),
            self.durable_offset.to_string().as_bytes(),
        )
    }

    fn rotate(&mut self) -> Result<()> {
        self.current.sync_all()?;
        let new_name = segment_path(&self.dir, self.next_offset);
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&new_name)?;
        self.current = f;
        self.current_path = new_name;
        self.current_bytes = 0;
        self.current_opened_ms = now_ms();
        fsync_dir(&self.dir)?;
        Ok(())
    }
}

impl WalReader {
    pub fn open(wal_root: &Path, stream: StreamId) -> Result<Self> {
        Ok(Self {
            dir: wal_root.join(stream.rel_dir()),
        })
    }

    pub fn read_from(&self, from_offset: u64, max_offset_inclusive: u64) -> Result<Vec<WalRecord>> {
        let mut out = Vec::new();
        if !self.dir.exists() {
            return Ok(out);
        }
        let segments = list_segments(&self.dir)?;
        for (start, path) in segments {
            if start > max_offset_inclusive {
                break;
            }
            let recs = read_segment(&path, false)?;
            for r in recs {
                if r.offset >= from_offset && r.offset <= max_offset_inclusive {
                    out.push(r);
                }
            }
        }
        Ok(out)
    }
}

fn read_segment(path: &Path, sealed: bool) -> Result<Vec<WalRecord>> {
    let mut f = File::open(path)?;
    let file_len = f.metadata()?.len();
    let mut pos = 0u64;
    let mut out = Vec::new();
    while pos + HEADER_LEN as u64 <= file_len {
        let mut hdr = [0u8; HEADER_LEN];
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut hdr)?;
        let Some((offset, len, crc)) = parse_header(&hdr) else {
            if sealed {
                return Err(WalError::CorruptSealed {
                    path: path.to_path_buf(),
                    at: pos,
                    msg: "bad header".into(),
                });
            }
            break;
        };
        let payload_end = pos + HEADER_LEN as u64 + u64::from(len);
        if payload_end > file_len {
            break;
        }
        let mut payload = vec![0u8; len as usize];
        f.read_exact(&mut payload)?;
        if crc32c::crc32c(&payload) != crc {
            if sealed {
                return Err(WalError::CorruptSealed {
                    path: path.to_path_buf(),
                    at: pos,
                    msg: "crc".into(),
                });
            }
            break;
        }
        out.push(WalRecord {
            offset,
            bytes: payload,
        });
        pos = payload_end;
    }
    Ok(out)
}

pub fn is_active_segment(active: &Path, candidate: &Path) -> bool {
    active == candidate
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{Record, RecordBody};
    use crate::types::{AssetClass, Channel, Exchange, Market};
    use std::time::Duration;

    fn rec(seq: u64) -> Record {
        Record {
            exchange: Exchange::Binance,
            asset_class: AssetClass::Crypto,
            market: Market::Perp,
            channel: Channel::Bbo,
            instrument_id: "binance:BTCUSDT".into(),
            source_symbol: "BTCUSDT".into(),
            ts_ex: 1_000 + seq as i64,
            ts_in: 1_100 + seq as i64,
            seq,
            body: RecordBody::Bbo {
                bid: 1.0,
                ask: 1.1,
                bid_sz: 1.0,
                ask_sz: 1.0,
            },
        }
    }

    fn stream() -> StreamId {
        StreamId::new(Exchange::Binance, AssetClass::Crypto, Channel::Bbo)
    }

    #[test]
    fn append_sync_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WalWriter::open(
            dir.path(),
            stream(),
            1024 * 1024,
            60_000,
            1024,
            Duration::from_millis(10),
        )
        .unwrap();
        w.append(&rec(0)).unwrap();
        w.append(&rec(1)).unwrap();
        w.sync().unwrap();
        assert_eq!(w.durable_offset(), 1);
        let r = WalReader::open(dir.path(), stream()).unwrap();
        let got = r.read_from(0, w.durable_offset()).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].decode().unwrap().seq, 0);
        assert_eq!(got[1].decode().unwrap().seq, 1);
    }

    #[test]
    fn truncate_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = WalWriter::open(
            dir.path(),
            stream(),
            1024 * 1024,
            60_000,
            1,
            Duration::from_millis(1),
        )
        .unwrap();
        w.append(&rec(0)).unwrap();
        w.sync().unwrap();
        let path = w.active_path().to_path_buf();
        drop(w);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[1, 2, 3, 4]).unwrap();
        f.sync_all().unwrap();
        drop(f);
        let w = WalWriter::open(
            dir.path(),
            stream(),
            1024 * 1024,
            60_000,
            1,
            Duration::from_millis(1),
        )
        .unwrap();
        assert_eq!(w.next_offset(), 1);
        let r = WalReader::open(dir.path(), stream()).unwrap();
        assert_eq!(r.read_from(0, 10).unwrap().len(), 1);
    }

    #[test]
    fn exclusive_lock() {
        let dir = tempfile::tempdir().unwrap();
        let _w = WalWriter::open(
            dir.path(),
            stream(),
            1024 * 1024,
            60_000,
            1,
            Duration::from_millis(1),
        )
        .unwrap();
        let w2 = WalWriter::open(
            dir.path(),
            stream(),
            1024 * 1024,
            60_000,
            1,
            Duration::from_millis(1),
        );
        assert!(matches!(w2, Err(WalError::Locked(_))));
    }
}

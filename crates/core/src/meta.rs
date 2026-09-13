use crate::snapshot::AggSnapshot;
use crate::types::{AssetClass, Channel, Exchange};
use rusqlite::{params, Connection, OptionalExtension};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub struct MetaStore {
    path: PathBuf,
    conn: Connection,
    exchange: Exchange,
}

#[derive(Debug, Clone)]
pub struct BatchRow {
    pub batch_id: String,
    pub exchange: Exchange,
    pub asset_class: AssetClass,
    pub hour_utc: i64,
    pub status: String,
    pub oss_status: String,
    pub checksum: String,
    pub file_relpath: String,
    pub archived_at: i64,
    pub uploaded_at: Option<i64>,
}

impl MetaStore {
    pub fn open(meta_root: &Path, exchange: Exchange) -> anyhow::Result<Self> {
        fs::create_dir_all(meta_root)?;
        let path = meta_root.join(format!("{}.sqlite", exchange.as_str()));
        let conn = Connection::open(&path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS epochs (
                asset_class TEXT NOT NULL,
                channel TEXT NOT NULL,
                epoch TEXT NOT NULL,
                PRIMARY KEY (asset_class, channel)
            );
            CREATE TABLE IF NOT EXISTS offsets (
                asset_class TEXT NOT NULL,
                channel TEXT NOT NULL,
                next_offset INTEGER NOT NULL,
                PRIMARY KEY (asset_class, channel)
            );
            CREATE TABLE IF NOT EXISTS batches (
                batch_id TEXT PRIMARY KEY,
                asset_class TEXT NOT NULL,
                hour_utc INTEGER NOT NULL,
                status TEXT NOT NULL,
                oss_status TEXT NOT NULL,
                checksum TEXT NOT NULL,
                file_relpath TEXT NOT NULL,
                archived_at INTEGER NOT NULL,
                uploaded_at INTEGER,
                snapshot BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_batches_hour ON batches(asset_class, hour_utc);
            CREATE TABLE IF NOT EXISTS offset_log (
                asset_class TEXT NOT NULL,
                channel TEXT NOT NULL,
                next_offset INTEGER NOT NULL,
                archived_at INTEGER NOT NULL
            );
            ",
        )?;
        Ok(Self {
            path,
            conn,
            exchange,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn check_epoch(
        &self,
        asset_class: AssetClass,
        channel: Channel,
        disk_epoch: Uuid,
    ) -> anyhow::Result<()> {
        let existing: Option<String> = self
            .conn
            .query_row(
                "SELECT epoch FROM epochs WHERE asset_class=?1 AND channel=?2",
                params![asset_class.as_str(), channel.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        match existing {
            None => {
                self.conn.execute(
                    "INSERT INTO epochs(asset_class, channel, epoch) VALUES (?1,?2,?3)",
                    params![asset_class.as_str(), channel.as_str(), disk_epoch.to_string()],
                )?;
                Ok(())
            }
            Some(e) if e == disk_epoch.to_string() => Ok(()),
            Some(e) => anyhow::bail!(
                "epoch mismatch {}/{} sqlite={e} disk={disk_epoch}; delete meta+wal together",
                asset_class,
                channel
            ),
        }
    }

    pub fn next_offset(&self, asset_class: AssetClass, channel: Channel) -> anyhow::Result<u64> {
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT next_offset FROM offsets WHERE asset_class=?1 AND channel=?2",
                params![asset_class.as_str(), channel.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.unwrap_or(0) as u64)
    }

    pub fn latest_snapshot(
        &self,
        asset_class: AssetClass,
    ) -> anyhow::Result<Option<(i64, AggSnapshot)>> {
        let row: Option<(i64, Vec<u8>)> = self
            .conn
            .query_row(
                "SELECT hour_utc, snapshot FROM batches
                 WHERE asset_class=?1 AND status='committed'
                 ORDER BY hour_utc DESC LIMIT 1",
                params![asset_class.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match row {
            None => Ok(None),
            Some((hour, blob)) => Ok(Some((hour, AggSnapshot::decode(&blob)?))),
        }
    }

    pub fn commit_batch(
        &mut self,
        batch_id: &Uuid,
        asset_class: AssetClass,
        hour_utc: i64,
        offsets: &[(Channel, u64)],
        snapshot: &AggSnapshot,
        checksum: &str,
        file_relpath: &str,
        archived_at: i64,
    ) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        let snap = snapshot.encode()?;
        tx.execute(
            "INSERT INTO batches(batch_id, asset_class, hour_utc, status, oss_status, checksum, file_relpath, archived_at, snapshot)
             VALUES (?1,?2,?3,'committed','pending',?4,?5,?6,?7)",
            params![
                batch_id.to_string(),
                asset_class.as_str(),
                hour_utc,
                checksum,
                file_relpath,
                archived_at,
                snap
            ],
        )?;
        for (ch, next) in offsets {
            tx.execute(
                "INSERT INTO offsets(asset_class, channel, next_offset) VALUES (?1,?2,?3)
                 ON CONFLICT(asset_class, channel) DO UPDATE SET next_offset=excluded.next_offset",
                params![asset_class.as_str(), ch.as_str(), *next as i64],
            )?;
            tx.execute(
                "INSERT INTO offset_log(asset_class, channel, next_offset, archived_at) VALUES (?1,?2,?3,?4)",
                params![asset_class.as_str(), ch.as_str(), *next as i64, archived_at],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn pending_uploads(&self) -> anyhow::Result<Vec<BatchRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT batch_id, asset_class, hour_utc, status, oss_status, checksum, file_relpath, archived_at, uploaded_at
             FROM batches WHERE status='committed' AND oss_status!='uploaded' ORDER BY hour_utc ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(BatchRow {
                batch_id: r.get(0)?,
                exchange: self.exchange,
                asset_class: r.get::<_, String>(1)?.parse().unwrap_or(AssetClass::Crypto),
                hour_utc: r.get(2)?,
                status: r.get(3)?,
                oss_status: r.get(4)?,
                checksum: r.get(5)?,
                file_relpath: r.get(6)?,
                archived_at: r.get(7)?,
                uploaded_at: r.get(8)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn mark_uploaded(&self, batch_id: &str, uploaded_at: i64) -> anyhow::Result<()> {
        self.conn.execute(
            "UPDATE batches SET oss_status='uploaded', uploaded_at=?2 WHERE batch_id=?1",
            params![batch_id, uploaded_at],
        )?;
        Ok(())
    }

    pub fn committed_batches(&self, asset_class: AssetClass) -> anyhow::Result<Vec<BatchRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT batch_id, asset_class, hour_utc, status, oss_status, checksum, file_relpath, archived_at, uploaded_at
             FROM batches WHERE asset_class=?1 AND status='committed' ORDER BY hour_utc",
        )?;
        let rows = stmt.query_map(params![asset_class.as_str()], |r| {
            Ok(BatchRow {
                batch_id: r.get(0)?,
                exchange: self.exchange,
                asset_class,
                hour_utc: r.get(2)?,
                status: r.get(3)?,
                oss_status: r.get(4)?,
                checksum: r.get(5)?,
                file_relpath: r.get(6)?,
                archived_at: r.get(7)?,
                uploaded_at: r.get(8)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn archived_at_when_offset_reached(
        &self,
        asset_class: AssetClass,
        channel: Channel,
        offset_exclusive: u64,
    ) -> anyhow::Result<Option<i64>> {
        let v: Option<i64> = self.conn.query_row(
            "SELECT MIN(archived_at) FROM offset_log
             WHERE asset_class=?1 AND channel=?2 AND next_offset > ?3",
            params![asset_class.as_str(), channel.as_str(), offset_exclusive as i64],
            |r| r.get(0),
        )?;
        Ok(v)
    }

    pub fn uploaded_files(&self) -> anyhow::Result<Vec<(String, i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT file_relpath, uploaded_at, checksum FROM batches WHERE oss_status='uploaded'",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<i64>>(1)?.unwrap_or(0),
                r.get::<_, String>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::AggSnapshot;

    #[test]
    fn commit_offsets_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = MetaStore::open(dir.path(), Exchange::Binance).unwrap();
        let id = Uuid::new_v4();
        m.commit_batch(
            &id,
            AssetClass::Crypto,
            1_700_000_000,
            &[(Channel::Bbo, 10), (Channel::Trade, 4)],
            &AggSnapshot::default(),
            "abc",
            "lake/x.parquet",
            1_700_000_100,
        )
        .unwrap();
        assert_eq!(m.next_offset(AssetClass::Crypto, Channel::Bbo).unwrap(), 10);
        assert_eq!(m.next_offset(AssetClass::Crypto, Channel::Trade).unwrap(), 4);
        let snap = m.latest_snapshot(AssetClass::Crypto).unwrap();
        assert!(snap.is_some());
        m.mark_uploaded(&id.to_string(), 1_700_000_200).unwrap();
        assert!(m.pending_uploads().unwrap().is_empty());
    }

    #[test]
    fn epoch_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let m = MetaStore::open(dir.path(), Exchange::Binance).unwrap();
        let a = Uuid::new_v4();
        m.check_epoch(AssetClass::Crypto, Channel::Bbo, a).unwrap();
        let b = Uuid::new_v4();
        assert!(m.check_epoch(AssetClass::Crypto, Channel::Bbo, b).is_err());
    }
}

use crate::parquet_out;
use collector_core::{
    crc64_hex, read_durable_offset, AggSnapshot, AssetClass, Bar1s, Channel, CollectorConfig,
    ControlKind, Exchange, Instrument, LastMark, LastQuote, MetaStore, Record, RecordBody,
    StreamId, TradeSide, WalReader,
};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
use uuid::Uuid;

struct SecAcc {
    n_bbo: u32,
    n_trade_msgs: u32,
    n_trades: u64,
    volume: f64,
    quote_volume: f64,
    taker_buy_volume: f64,
    px_open: Option<f64>,
    px_high: Option<f64>,
    px_low: Option<f64>,
    px_close: Option<f64>,
    last_bbo: Option<LastQuote>,
    last_mark: Option<LastMark>,
    gap: bool,
}

impl Default for SecAcc {
    fn default() -> Self {
        Self {
            n_bbo: 0,
            n_trade_msgs: 0,
            n_trades: 0,
            volume: 0.0,
            quote_volume: 0.0,
            taker_buy_volume: 0.0,
            px_open: None,
            px_high: None,
            px_low: None,
            px_close: None,
            last_bbo: None,
            last_mark: None,
            gap: false,
        }
    }
}

struct HourBuf {
    #[allow(dead_code)]
    hour_start: i64,
    acc: HashMap<(String, i64), SecAcc>,
    start_quotes: HashMap<String, LastQuote>,
    snap: AggSnapshot,
    next_off: HashMap<Channel, u64>,
}

impl HourBuf {
    fn new(hour_start: i64, snap: AggSnapshot, next_off: HashMap<Channel, u64>) -> Self {
        Self {
            hour_start,
            acc: HashMap::new(),
            start_quotes: snap.quotes.clone(),
            snap,
            next_off,
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn hour_start_sec(ts_ms: i64) -> i64 {
    (ts_ms / 1000).div_euclid(3600) * 3600
}

pub struct Archiver {
    exchange: Exchange,
    asset_class: AssetClass,
    cfg: CollectorConfig,
    meta: MetaStore,
    hours: HashMap<i64, HourBuf>,
    sealed: HashMap<i64, bool>,
    late: u32,
    consumed: HashMap<Channel, u64>,
}

impl Archiver {
    pub fn open(
        exchange: Exchange,
        asset_class: AssetClass,
        cfg: CollectorConfig,
    ) -> anyhow::Result<Self> {
        let meta = MetaStore::open(&cfg.meta_root(), exchange)?;
        for ch in Channel::ALL {
            let dir = cfg.wal_root().join(StreamId::new(exchange, asset_class, ch).rel_dir());
            if let Some(ep) = collector_core::read_epoch(&dir)? {
                meta.check_epoch(asset_class, ch, ep)?;
            }
        }
        let mut consumed = HashMap::new();
        for ch in Channel::ALL {
            consumed.insert(ch, meta.next_offset(asset_class, ch)?);
        }
        Ok(Self {
            exchange,
            asset_class,
            cfg,
            meta,
            hours: HashMap::new(),
            sealed: HashMap::new(),
            late: 0,
            consumed,
        })
    }

    pub fn tick(&mut self, live: bool) -> anyhow::Result<()> {
        let watermark = self.cfg.archive.bar_1s.hour_watermark_ms;
        for ch in Channel::ALL {
            let sid = StreamId::new(self.exchange, self.asset_class, ch);
            let dir = self.cfg.wal_root().join(sid.rel_dir());
            if !dir.exists() {
                continue;
            }
            let durable = read_durable_offset(&dir)?;
            let from = *self.consumed.get(&ch).unwrap_or(&0);
            if from > durable {
                continue;
            }
            let reader = WalReader::open(&self.cfg.wal_root(), sid)?;
            let recs = reader.read_from(from, durable)?;
            for wr in recs {
                let rec = wr.decode()?;
                self.consumed.insert(ch, wr.offset + 1);
                self.apply(rec, wr.offset + 1)?;
            }
        }
        self.maybe_seal(live, watermark)?;
        Ok(())
    }

    fn apply(&mut self, rec: Record, next_off: u64) -> anyhow::Result<()> {
        if rec.asset_class != self.asset_class {
            return Ok(());
        }
        let hour = hour_start_sec(rec.ts_ex);
        if *self.sealed.get(&hour).unwrap_or(&false) {
            self.late += 1;
            return Ok(());
        }
        if !self.hours.contains_key(&hour) {
            let mut snap = self
                .meta
                .latest_snapshot(self.asset_class)?
                .map(|(_, s)| s)
                .unwrap_or_default();
            if snap.instruments.is_empty() {
                for i in collector_core::load_universe(&self.cfg.data_dir, self.exchange)? {
                    if i.asset_class == self.asset_class {
                        snap.instruments.insert(i.instrument_id.clone(), i);
                    }
                }
            }
            let mut next = HashMap::new();
            for ch in Channel::ALL {
                next.insert(ch, self.meta.next_offset(self.asset_class, ch)?);
            }
            self.hours.insert(hour, HourBuf::new(hour, snap, next));
        }
        let buf = self.hours.get_mut(&hour).unwrap();
        buf.next_off.insert(rec.channel, next_off);
        match &rec.body {
            RecordBody::Control { kind, .. } => match kind {
                ControlKind::Disconnect | ControlKind::SubscribeFail => {
                    buf.snap.disconnected = true;
                    buf.snap.disconnect_since_ms = Some(rec.ts_ex);
                }
                ControlKind::Reconnect => {
                    buf.snap.disconnected = false;
                    buf.snap.disconnect_since_ms = None;
                }
            },
            _ => {
                if !rec.instrument_id.ends_with(":*") {
                    buf.snap.instruments.entry(rec.instrument_id.clone()).or_insert(Instrument {
                        instrument_id: rec.instrument_id.clone(),
                        exchange: rec.exchange,
                        asset_class: rec.asset_class,
                        source_symbol: rec.source_symbol.clone(),
                        base_asset: rec.source_symbol.clone(),
                        quote_asset: String::new(),
                        settle_asset: String::new(),
                        contract_multiplier: 1.0,
                        listed_at_ms: rec.ts_ex,
                    });
                }
            }
        }
        let ts_sec = rec.ts_ex / 1000;
        let key = (rec.instrument_id.clone(), ts_sec);
        let acc = buf.acc.entry(key).or_default();
        if buf.snap.disconnected {
            acc.gap = true;
        }
        match rec.body {
            RecordBody::Bbo {
                bid,
                ask,
                bid_sz,
                ask_sz,
            } => {
                acc.n_bbo += 1;
                let q = LastQuote {
                    bid,
                    ask,
                    bid_sz,
                    ask_sz,
                    ts_ex: rec.ts_ex,
                };
                acc.last_bbo = Some(q.clone());
                buf.snap.quotes.insert(rec.instrument_id, q);
            }
            RecordBody::Trade {
                px,
                sz,
                side,
                n_raw_trades,
                ..
            } => {
                acc.n_trade_msgs += 1;
                acc.n_trades += u64::from(n_raw_trades);
                acc.volume += sz;
                acc.quote_volume += px * sz;
                if side == TradeSide::Buy {
                    acc.taker_buy_volume += sz;
                }
                if acc.px_open.is_none() {
                    acc.px_open = Some(px);
                }
                acc.px_close = Some(px);
                acc.px_high = Some(acc.px_high.map(|h| h.max(px)).unwrap_or(px));
                acc.px_low = Some(acc.px_low.map(|l| l.min(px)).unwrap_or(px));
            }
            RecordBody::Funding {
                rate,
                mark,
                index,
                next_ts,
            } => {
                let m = LastMark {
                    rate,
                    mark,
                    index,
                    next_ts,
                    ts_ex: rec.ts_ex,
                };
                acc.last_mark = Some(m.clone());
                buf.snap.marks.insert(rec.instrument_id, m);
            }
            RecordBody::Control { .. } => {}
        }
        Ok(())
    }

    fn maybe_seal(&mut self, live: bool, watermark_ms: i64) -> anyhow::Result<()> {
        let hours: Vec<i64> = self.hours.keys().copied().collect();
        let now = now_ms();
        for h in hours {
            if *self.sealed.get(&h).unwrap_or(&false) {
                continue;
            }
            let hour_end_ms = (h + 3600) * 1000;
            let event_ok = self.max_ts_ex(h) >= hour_end_ms + watermark_ms
                || self.hours.keys().any(|o| *o > h);
            let wall_ok = live && now >= hour_end_ms + watermark_ms;
            if event_ok || wall_ok {
                self.seal(h)?;
            }
        }
        Ok(())
    }

    fn max_ts_ex(&self, hour: i64) -> i64 {
        self.hours
            .get(&hour)
            .map(|b| {
                b.acc
                    .keys()
                    .map(|(_, s)| *s * 1000)
                    .max()
                    .unwrap_or(hour * 1000)
            })
            .unwrap_or(0)
    }

    fn seal(&mut self, hour: i64) -> anyhow::Result<()> {
        let buf = match self.hours.remove(&hour) {
            Some(b) => b,
            None => return Ok(()),
        };
        let rows = materialize(self.exchange, self.asset_class, hour, &buf);
        let batch_id = Uuid::new_v4();
        let date = chrono::DateTime::from_timestamp(hour, 0)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap())
            .format("%Y-%m-%d");
        let hour_s = chrono::DateTime::from_timestamp(hour, 0)
            .unwrap()
            .format("%H");
        let rel = format!(
            "exchange={}/asset_class={}/channel=bar_1s/date={date}/hour={hour_s}/part-{batch_id}.parquet",
            self.exchange.as_str(),
            self.asset_class.as_str()
        );
        let path = self.cfg.lake_root().join(&rel);
        let (_sz, checksum) = if rows.is_empty() {
            std::fs::create_dir_all(path.parent().unwrap())?;
            std::fs::write(&path, b"")?;
            (0u64, crc64_hex(b""))
        } else {
            parquet_out::write_bars(&path, rows)?
        };
        let mut offs: Vec<(Channel, u64)> = buf.next_off.into_iter().collect();
        offs.sort_by_key(|(c, _)| c.as_str());
        let archived_at = now_ms() / 1000;
        self.meta.commit_batch(
            &batch_id,
            self.asset_class,
            hour,
            &offs,
            &buf.snap,
            &checksum,
            &rel,
            archived_at,
        )?;
        self.sealed.insert(hour, true);
        info!(
            exchange = %self.exchange,
            asset_class = %self.asset_class,
            hour,
            batch_id = %batch_id,
            checksum = %checksum,
            "sealed hour"
        );
        Ok(())
    }
}

fn materialize(
    exchange: Exchange,
    asset_class: AssetClass,
    hour: i64,
    buf: &HourBuf,
) -> Vec<Bar1s> {
    let mut ids: Vec<String> = buf.snap.instruments.keys().cloned().collect();
    ids.sort();
    let mut out = Vec::new();
    for id in ids {
        let inst = buf.snap.instruments.get(&id).cloned();
        let Some(inst) = inst else { continue };
        let mut carry = buf.start_quotes.get(&id).cloned();
        let mut mark = buf.snap.marks.get(&id).cloned();
        for sec in hour..hour + 3600 {
            if let Some(acc) = buf.acc.get(&(id.clone(), sec)) {
                if let Some(q) = &acc.last_bbo {
                    carry = Some(q.clone());
                }
                if let Some(m) = &acc.last_mark {
                    mark = Some(m.clone());
                }
            }
            let acc = buf.acc.get(&(id.clone(), sec));
            let n_bbo = acc.map(|a| a.n_bbo).unwrap_or(0);
            let quote_valid = carry.is_some() && !buf.snap.disconnected && acc.map(|a| !a.gap).unwrap_or(!buf.snap.disconnected);
            let quote_age = carry.as_ref().map(|q| sec * 1000 - q.ts_ex);
            let gap = acc.map(|a| a.gap).unwrap_or(buf.snap.disconnected);
            out.push(Bar1s {
                exchange: exchange.as_str().into(),
                asset_class: asset_class.as_str().into(),
                instrument_id: inst.instrument_id.clone(),
                source_symbol: inst.source_symbol.clone(),
                base_asset: inst.base_asset.clone(),
                quote_asset: inst.quote_asset.clone(),
                settle_asset: inst.settle_asset.clone(),
                contract_multiplier: inst.contract_multiplier,
                ts_sec: sec,
                bid: carry.as_ref().map(|q| q.bid),
                ask: carry.as_ref().map(|q| q.ask),
                bid_sz: carry.as_ref().map(|q| q.bid_sz),
                ask_sz: carry.as_ref().map(|q| q.ask_sz),
                quote_valid,
                quote_age_ms: quote_age,
                n_bbo,
                px_open: acc.and_then(|a| a.px_open),
                px_high: acc.and_then(|a| a.px_high),
                px_low: acc.and_then(|a| a.px_low),
                px_close: acc.and_then(|a| a.px_close),
                volume: acc.map(|a| a.volume).unwrap_or(0.0),
                quote_volume: acc.map(|a| a.quote_volume).unwrap_or(0.0),
                taker_buy_volume: acc.map(|a| a.taker_buy_volume).unwrap_or(0.0),
                n_trades: acc.map(|a| a.n_trades).unwrap_or(0),
                n_trade_msgs: acc.map(|a| a.n_trade_msgs).unwrap_or(0),
                mark: mark.as_ref().and_then(|m| m.mark),
                funding_rate: mark.as_ref().and_then(|m| m.rate),
                gap,
                late_count: 0,
            });
        }
    }
    out
}

pub fn run_loop(cfg: CollectorConfig) -> anyhow::Result<()> {
    let mut workers = Vec::new();
    for ex in [Exchange::Binance, Exchange::Hyperliquid] {
        for ac in [AssetClass::Crypto, AssetClass::Equity] {
            workers.push(Archiver::open(ex, ac, cfg.clone())?);
        }
    }
    loop {
        for w in workers.iter_mut() {
            if let Err(e) = w.tick(true) {
                warn!(error = %e, "archive tick");
            }
        }
        if let Err(e) = crate::reaper::reap(&cfg) {
            warn!(error = %e, "reaper");
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}



use collector_core::{
    wal::list_segments, AssetClass, Channel, CollectorConfig, Exchange, MetaStore, StreamId,
};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

fn now_s() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub fn reap(cfg: &CollectorConfig) -> anyhow::Result<()> {
    let ret = cfg.reaper.wal_retention.as_secs() as i64;
    let local_ret = cfg.reaper.local_parquet_retention.as_secs() as i64;
    let now = now_s();
    for ex in [Exchange::Binance, Exchange::Hyperliquid] {
        let meta = match MetaStore::open(&cfg.meta_root(), ex) {
            Ok(m) => m,
            Err(_) => continue,
        };
        for ac in [AssetClass::Crypto, AssetClass::Equity] {
            for ch in Channel::ALL {
                let sid = StreamId::new(ex, ac, ch);
                let dir = cfg.wal_root().join(sid.rel_dir());
                if !dir.exists() {
                    continue;
                }
                let segs = list_segments(&dir)?;
                if segs.len() <= 1 {
                    continue;
                }
                let last_i = segs.len() - 1;
                for (i, (_start, path)) in segs.iter().enumerate() {
                    if i == last_i {
                        continue;
                    }
                    let end_excl = segs.get(i + 1).map(|(s, _)| *s).unwrap_or(u64::MAX);
                    let Some(archived_at) =
                        meta.archived_at_when_offset_reached(ac, ch, end_excl.saturating_sub(1))?
                    else {
                        continue;
                    };
                    if now >= archived_at + ret {
                        match fs::remove_file(path) {
                            Ok(()) => info!(path = %path.display(), "reaper deleted wal segment"),
                            Err(e) => warn!(path = %path.display(), error = %e, "reaper delete failed"),
                        }
                    }
                }
            }
        }
        for (rel, uploaded_at, _) in meta.uploaded_files()? {
            if uploaded_at > 0 && now >= uploaded_at + local_ret {
                let p = cfg.lake_root().join(&rel);
                if p.exists() {
                    let _ = fs::remove_file(&p);
                    info!(path = %p.display(), "reaper deleted local parquet");
                }
            }
        }
    }
    Ok(())
}

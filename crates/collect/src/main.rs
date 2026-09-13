use anyhow::Context;
use collector_core::{
    load_exchange_file, AssetClass, Channel, CollectorConfig, Exchange, Record, StreamId, WalWriter,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Clone)]
struct Stats {
    recv: u64,
    last: Instant,
    lag_sum: i64,
    lag_n: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let mut exchange = None;
    let mut config_path = PathBuf::from("config/collector.yaml");
    while let Some(a) = args.next() {
        match a.as_str() {
            "--exchange" => exchange = args.next(),
            "--config" => {
                config_path = PathBuf::from(args.next().context("--config value")?);
            }
            other => anyhow::bail!("unknown arg {other}"),
        }
    }
    let exchange = Exchange::from_str(&exchange.context("--exchange required")?)?;
    let cfg = CollectorConfig::load(&config_path).with_context(|| format!("load {config_path:?}"))?;
    let ex_path = config_path
        .parent()
        .unwrap_or(std::path::Path::new("config"))
        .join("exchanges")
        .join(format!("{}.yaml", exchange.as_str()));
    let ex_cfg = load_exchange_file(&ex_path).with_context(|| format!("load {ex_path:?}"))?;
    if !ex_cfg.enabled {
        anyhow::bail!("{exchange} disabled in {ex_path:?}");
    }
    let asset_classes: Vec<AssetClass> = ex_cfg
        .asset_classes
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    let channels: Vec<Channel> = ex_cfg
        .channels
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    let (tx, mut rx) = mpsc::channel::<Record>(cfg.wal.queue_capacity);
    std::fs::create_dir_all(&cfg.data_dir)?;

    match exchange {
        Exchange::Binance => {
            let bcfg = source_binance::BinanceConfig {
                asset_classes: asset_classes.clone(),
                quote_assets: ex_cfg.quote_assets.clone(),
                channels: channels.clone(),
                symbol_allow: ex_cfg.symbols.clone(),
            };
            let uni = source_binance::discover(&bcfg).await?;
            collector_core::save_universe(&cfg.data_dir, exchange, &uni)?;
            info!(n = uni.len(), "binance universe");
            log_universe_split(&uni);
            let health = source_binance::Health::new();
            tokio::spawn(async move {
                if let Err(e) = source_binance::run(bcfg, uni, tx, health).await {
                    warn!(error = %e, "binance run ended");
                }
            });
        }
        Exchange::Hyperliquid => {
            let hcfg = source_hyperliquid::HlConfig {
                asset_classes: asset_classes.clone(),
                channels: channels.clone(),
                hip3_dexes: ex_cfg.hip3_dexes.clone(),
                symbol_allow: ex_cfg.symbols.clone(),
            };
            let uni = source_hyperliquid::discover(&hcfg).await?;
            collector_core::save_universe(&cfg.data_dir, exchange, &uni)?;
            info!(n = uni.len(), "hyperliquid universe");
            log_universe_split(&uni);
            let health = source_hyperliquid::Health::new();
            tokio::spawn(async move {
                if let Err(e) = source_hyperliquid::run(hcfg, uni, tx, health).await {
                    warn!(error = %e, "hyperliquid run ended");
                }
            });
        }
    }

    let mut writers: HashMap<(AssetClass, Channel), WalWriter> = HashMap::new();
    let mut stats: HashMap<(AssetClass, Channel), Stats> = HashMap::new();
    let mut last_report = Instant::now();
    loop {
        tokio::select! {
            rec = rx.recv() => {
                let Some(rec) = rec else { break; };
                let key = (rec.asset_class, rec.channel);
                if !writers.contains_key(&key) {
                    let sid = StreamId::new(exchange, rec.asset_class, rec.channel);
                    let w = WalWriter::open(
                        &cfg.wal_root(),
                        sid,
                        cfg.wal.segment_max_bytes,
                        cfg.wal.segment_max_ms,
                        cfg.wal.sync_bytes,
                        Duration::from_millis(cfg.wal.sync_interval_ms),
                    )?;
                    writers.insert(key, w);
                }
                let w = writers.get_mut(&key).unwrap();
                w.append(&rec)?;
                w.maybe_sync()?;
                let s = stats.entry(key).or_insert_with(|| Stats {
                    recv: 0,
                    last: Instant::now(),
                    lag_sum: 0,
                    lag_n: 0,
                });
                s.recv += 1;
                s.last = Instant::now();
                s.lag_sum += (rec.ts_in - rec.ts_ex).max(0);
                s.lag_n += 1;
            }
            _ = tokio::time::sleep(Duration::from_millis(cfg.wal.sync_interval_ms)) => {
                for w in writers.values_mut() {
                    w.maybe_sync()?;
                }
            }
        }
        if last_report.elapsed() >= Duration::from_secs(5) {
            for ((ac, ch), s) in &stats {
                let avg = if s.lag_n == 0 { 0 } else { s.lag_sum / s.lag_n as i64 };
                info!(
                    exchange = %exchange,
                    asset_class = %ac,
                    channel = %ch,
                    recv = s.recv,
                    avg_lag_ms = avg,
                    stale_ms = s.last.elapsed().as_millis() as u64,
                    "collect stats"
                );
            }
            last_report = Instant::now();
        }
    }
    for w in writers.values_mut() {
        w.sync()?;
    }
    Ok(())
}

fn log_universe_split(uni: &[collector_core::Instrument]) {
    let mut c = 0u64;
    let mut e = 0u64;
    for i in uni {
        match i.asset_class {
            AssetClass::Crypto => c += 1,
            AssetClass::Equity => e += 1,
        }
    }
    info!(crypto = c, equity = e, "universe split");
}



use anyhow::Context;
use collector_core::{
    AssetClass, Channel, ControlKind, Exchange, Instrument, Market, Record, RecordBody, TradeDedup,
    TradeSide,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

const INFO_URLS: &[&str] = &[
    "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "https://www.binance.com/fapi/v1/exchangeInfo",
];
const WS_BASE: &str = "wss://fstream.binance.com/market/ws";
const WS_BOOK: &str = "wss://fstream.binance.com/ws/!bookTicker";
const MAX_STREAMS: usize = 150;

#[derive(Clone, Debug)]
pub struct BinanceConfig {
    pub asset_classes: Vec<AssetClass>,
    pub quote_assets: Vec<String>,
    pub channels: Vec<Channel>,
    pub symbol_allow: Vec<String>,
}

pub struct Health {
    pub subscribed: AtomicU64,
    pub recv: AtomicU64,
    pub reconnects: AtomicU64,
    pub last_lag_ms: AtomicU64,
}

impl Health {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            subscribed: AtomicU64::new(0),
            recv: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            last_lag_ms: AtomicU64::new(0),
        })
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub async fn discover(cfg: &BinanceConfig) -> anyhow::Result<Vec<Instrument>> {
    let client = reqwest::Client::builder()
        .user_agent("data-collector/0.1")
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut last_err = None;
    for url in INFO_URLS {
        match client.get(*url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let v: Value = resp.json().await.context("exchangeInfo json")?;
                return Ok(parse_exchange_info(&v, cfg));
            }
            Ok(resp) => last_err = Some(anyhow::anyhow!("HTTP {} from {url}", resp.status())),
            Err(e) => last_err = Some(e.into()),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no exchangeInfo")))
}

fn parse_exchange_info(v: &Value, cfg: &BinanceConfig) -> Vec<Instrument> {
    let want_crypto = cfg.asset_classes.contains(&AssetClass::Crypto);
    let want_equity = cfg.asset_classes.contains(&AssetClass::Equity);
    let quotes: Vec<String> = if cfg.quote_assets.is_empty() {
        vec!["USDT".into()]
    } else {
        cfg.quote_assets.clone()
    };
    let allow: Option<std::collections::HashSet<String>> = if cfg.symbol_allow.is_empty() {
        None
    } else {
        Some(cfg.symbol_allow.iter().cloned().collect())
    };
    let mut out = Vec::new();
    let Some(syms) = v.get("symbols").and_then(|s| s.as_array()) else {
        return out;
    };
    for s in syms {
        let symbol = s.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
        if symbol.is_empty() {
            continue;
        }
        if let Some(a) = &allow {
            if !a.contains(symbol) {
                continue;
            }
        }
        let ctype = s.get("contractType").and_then(|x| x.as_str()).unwrap_or("");
        let status = s.get("status").and_then(|x| x.as_str()).unwrap_or("");
        if status != "TRADING" {
            continue;
        }
        let quote = s.get("quoteAsset").and_then(|x| x.as_str()).unwrap_or("");
        let base = s.get("baseAsset").and_then(|x| x.as_str()).unwrap_or("");
        let ut = s.get("underlyingType").and_then(|x| x.as_str()).unwrap_or("");
        let asset_class = if ctype == "PERPETUAL" && quotes.iter().any(|q| q == quote) && ut != "INDEX"
        {
            if !want_crypto {
                continue;
            }
            AssetClass::Crypto
        } else if ctype == "TRADIFI_PERPETUAL" && ut == "EQUITY" {
            if !want_equity {
                continue;
            }
            AssetClass::Equity
        } else {
            continue;
        };
        let mult = s
            .get("contractSize")
            .and_then(|x| x.as_f64())
            .or_else(|| {
                s.get("filters")
                    .and_then(|f| f.as_array())
                    .and_then(|_| Some(1.0))
            })
            .unwrap_or(1.0);
        out.push(Instrument {
            instrument_id: Instrument::id_for(Exchange::Binance, symbol),
            exchange: Exchange::Binance,
            asset_class,
            source_symbol: symbol.to_string(),
            base_asset: base.to_string(),
            quote_asset: quote.to_string(),
            settle_asset: quote.to_string(),
            contract_multiplier: if mult == 0.0 { 1.0 } else { mult },
            listed_at_ms: now_ms(),
        });
    }
    out
}

pub async fn run(
    cfg: BinanceConfig,
    universe: Vec<Instrument>,
    tx: mpsc::Sender<Record>,
    health: Arc<Health>,
) -> anyhow::Result<()> {
    health.subscribed.store(universe.len() as u64, Ordering::Relaxed);
    let by_sym: HashMap<String, Instrument> = universe
        .iter()
        .cloned()
        .map(|i| (i.source_symbol.clone(), i))
        .collect();
    let chans = if cfg.channels.is_empty() {
        vec![Channel::Bbo, Channel::Trade, Channel::Funding]
    } else {
        cfg.channels.clone()
    };
    let mut tasks = tokio::task::JoinSet::new();
    if chans.contains(&Channel::Bbo) {
        let tx = tx.clone();
        let map = by_sym.clone();
        let h = health.clone();
        tasks.spawn(async move { loop_ws("bookTicker", WS_BOOK, map, tx, h, parse_book).await });
    }
    if chans.contains(&Channel::Funding) {
        let tx = tx.clone();
        let map = by_sym.clone();
        let h = health.clone();
        tasks.spawn(async move {
            loop_ws_sub(
                "markPrice",
                WS_BASE,
                vec!["!markPrice@arr@1s".into()],
                map,
                tx,
                h,
                parse_mark,
            )
            .await
        });
    }
    if chans.contains(&Channel::Trade) {
        let mut names: Vec<String> = by_sym.keys().map(|s| s.to_lowercase()).collect();
        names.sort();
        for chunk in names.chunks(MAX_STREAMS) {
            let streams: Vec<String> = chunk.iter().map(|s| format!("{s}@aggTrade")).collect();
            let tx = tx.clone();
            let map = by_sym.clone();
            let h = health.clone();
            tasks.spawn(async move {
                loop_ws_sub("aggTrade", WS_BASE, streams, map, tx, h, parse_trade).await
            });
        }
    }
    info!(
        instruments = universe.len(),
        tasks = tasks.len(),
        "binance collectors started"
    );
    while let Some(res) = tasks.join_next().await {
        res??;
    }
    Ok(())
}

async fn loop_ws<F>(
    name: &'static str,
    url: &str,
    map: HashMap<String, Instrument>,
    tx: mpsc::Sender<Record>,
    health: Arc<Health>,
    parse: F,
) -> anyhow::Result<()>
where
    F: Fn(&Value, &HashMap<String, Instrument>, i64) -> Vec<Record> + Send + 'static,
{
    let mut attempt = 0u32;
    loop {
        match connect_async(url).await {
            Ok((ws, _)) => {
                info!(name, "ws connected");
                if attempt > 0 {
                    health.reconnects.fetch_add(1, Ordering::Relaxed);
                    for r in controls(&map, ControlKind::Reconnect, name) {
                        let _ = tx.send(r).await;
                    }
                }
                attempt = 0;
                let (mut sink, mut stream) = ws.split();
                let mut last_msg = Instant::now();
                loop {
                    tokio::select! {
                        msg = stream.next() => {
                            let Some(msg) = msg else { break; };
                            last_msg = Instant::now();
                            let Ok(msg) = msg else { break; };
                            let Ok(text) = msg.to_text() else { continue; };
                            let ts_in = now_ms();
                            let Ok(v) = serde_json::from_str::<Value>(text) else { continue; };
                            let recs = parse(&v, &map, ts_in);
                            for r in recs {
                                let lag = (r.ts_in - r.ts_ex).max(0) as u64;
                                health.last_lag_ms.store(lag, Ordering::Relaxed);
                                health.recv.fetch_add(1, Ordering::Relaxed);
                                if tx.send(r).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_secs(15)) => {
                            if last_msg.elapsed() > Duration::from_secs(20) {
                                warn!(name, "ws idle, reconnect");
                                break;
                            }
                            let _ = sink.send(tokio_tungstenite::tungstenite::Message::Pong(vec![].into())).await;
                        }
                    }
                }
                for r in controls(&map, ControlKind::Disconnect, name) {
                    let _ = tx.send(r).await;
                }
            }
            Err(e) => {
                warn!(name, error = %e, "ws connect failed");
                for r in controls(&map, ControlKind::SubscribeFail, &e.to_string()) {
                    let _ = tx.send(r).await;
                }
            }
        }
        attempt += 1;
        let delay = Duration::from_millis((250u64 << attempt.min(6)).min(10_000));
        tokio::time::sleep(delay).await;
    }
}

async fn loop_ws_sub<F>(
    name: &'static str,
    url: &str,
    streams: Vec<String>,
    map: HashMap<String, Instrument>,
    tx: mpsc::Sender<Record>,
    health: Arc<Health>,
    parse: F,
) -> anyhow::Result<()>
where
    F: Fn(&Value, &HashMap<String, Instrument>, i64) -> Vec<Record> + Send + 'static,
{
    let mut attempt = 0u32;
    loop {
        match connect_async(url).await {
            Ok((ws, _)) => {
                info!(name, n = streams.len(), "ws connected");
                if attempt > 0 {
                    health.reconnects.fetch_add(1, Ordering::Relaxed);
                    for r in controls(&map, ControlKind::Reconnect, name) {
                        let _ = tx.send(r).await;
                    }
                }
                attempt = 0;
                let (mut sink, mut stream) = ws.split();
                for chunk in streams.chunks(50) {
                    let sub = serde_json::json!({
                        "method": "SUBSCRIBE",
                        "params": chunk,
                        "id": 1
                    });
                    sink.send(tokio_tungstenite::tungstenite::Message::Text(
                        sub.to_string().into(),
                    ))
                    .await?;
                }
                let mut last_msg = Instant::now();
                loop {
                    tokio::select! {
                        msg = stream.next() => {
                            let Some(msg) = msg else { break; };
                            last_msg = Instant::now();
                            let Ok(msg) = msg else { break; };
                            let Ok(text) = msg.to_text() else { continue; };
                            let ts_in = now_ms();
                            let Ok(v) = serde_json::from_str::<Value>(text) else { continue; };
                            if v.get("result").is_some() && v.get("id").is_some() {
                                continue;
                            }
                            let recs = parse(&v, &map, ts_in);
                            for r in recs {
                                let lag = (r.ts_in - r.ts_ex).max(0) as u64;
                                health.last_lag_ms.store(lag, Ordering::Relaxed);
                                health.recv.fetch_add(1, Ordering::Relaxed);
                                if tx.send(r).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_secs(15)) => {
                            if last_msg.elapsed() > Duration::from_secs(30) {
                                warn!(name, "ws idle, reconnect");
                                break;
                            }
                            let _ = sink.send(tokio_tungstenite::tungstenite::Message::Pong(vec![].into())).await;
                        }
                    }
                }
                for r in controls(&map, ControlKind::Disconnect, name) {
                    let _ = tx.send(r).await;
                }
            }
            Err(e) => {
                warn!(name, error = %e, "ws connect failed");
                for r in controls(&map, ControlKind::SubscribeFail, &e.to_string()) {
                    let _ = tx.send(r).await;
                }
            }
        }
        attempt += 1;
        let delay = Duration::from_millis((250u64 << attempt.min(6)).min(10_000));
        tokio::time::sleep(delay).await;
    }
}

fn controls(map: &HashMap<String, Instrument>, kind: ControlKind, reason: &str) -> Vec<Record> {
    let mut classes = Vec::new();
    for i in map.values() {
        if !classes.contains(&i.asset_class) {
            classes.push(i.asset_class);
        }
    }
    if classes.is_empty() {
        classes.push(AssetClass::Crypto);
    }
    classes
        .into_iter()
        .map(|asset_class| Record {
            exchange: Exchange::Binance,
            asset_class,
            market: Market::Perp,
            channel: Channel::Control,
            instrument_id: format!("binance:{asset_class}:*"),
            source_symbol: "*".into(),
            ts_ex: now_ms(),
            ts_in: now_ms(),
            seq: 0,
            body: RecordBody::Control {
                kind,
                reason: reason.into(),
            },
        })
        .collect()
}

fn parse_f64(v: Option<&Value>) -> Option<f64> {
    v.and_then(|x| {
        x.as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| x.as_f64())
    })
}

pub fn parse_book(v: &Value, map: &HashMap<String, Instrument>, ts_in: i64) -> Vec<Record> {
    let data = v.get("data").unwrap_or(v);
    let sym = data.get("s").and_then(|s| s.as_str()).unwrap_or("");
    let Some(inst) = map.get(sym) else {
        return Vec::new();
    };
    let bid = parse_f64(data.get("b"));
    let ask = parse_f64(data.get("a"));
    let (Some(bid), Some(ask)) = (bid, ask) else {
        return Vec::new();
    };
    if bid <= 0.0 || ask <= 0.0 {
        return Vec::new();
    }
    let ts_ex = data
        .get("E")
        .or_else(|| data.get("T"))
        .and_then(|x| x.as_i64())
        .unwrap_or(ts_in);
    let seq = data.get("u").and_then(|x| x.as_u64()).unwrap_or(0);
    vec![Record {
        exchange: Exchange::Binance,
        asset_class: inst.asset_class,
        market: Market::Perp,
        channel: Channel::Bbo,
        instrument_id: inst.instrument_id.clone(),
        source_symbol: inst.source_symbol.clone(),
        ts_ex,
        ts_in,
        seq,
        body: RecordBody::Bbo {
            bid,
            ask,
            bid_sz: parse_f64(data.get("B")).unwrap_or(0.0),
            ask_sz: parse_f64(data.get("A")).unwrap_or(0.0),
        },
    }]
}

pub fn parse_trade(v: &Value, map: &HashMap<String, Instrument>, ts_in: i64) -> Vec<Record> {
    let data = v.get("data").unwrap_or(v);
    let sym = data.get("s").and_then(|s| s.as_str()).unwrap_or("");
    let Some(inst) = map.get(sym) else {
        return Vec::new();
    };
    let px = parse_f64(data.get("p"));
    let sz = parse_f64(data.get("q"));
    let (Some(px), Some(sz)) = (px, sz) else {
        return Vec::new();
    };
    let first = data.get("f").and_then(|x| x.as_i64()).unwrap_or(0);
    let last = data.get("l").and_then(|x| x.as_i64()).unwrap_or(first);
    let n_raw = (last - first + 1).max(1) as u32;
    let ts_ex = data
        .get("T")
        .or_else(|| data.get("E"))
        .and_then(|x| x.as_i64())
        .unwrap_or(ts_in);
    let m = data.get("m").and_then(|x| x.as_bool()).unwrap_or(false);
    vec![Record {
        exchange: Exchange::Binance,
        asset_class: inst.asset_class,
        market: Market::Perp,
        channel: Channel::Trade,
        instrument_id: inst.instrument_id.clone(),
        source_symbol: inst.source_symbol.clone(),
        ts_ex,
        ts_in,
        seq: data.get("a").and_then(|x| x.as_u64()).unwrap_or(0),
        body: RecordBody::Trade {
            px,
            sz,
            side: if m { TradeSide::Sell } else { TradeSide::Buy },
            dedup: TradeDedup::BinanceAgg {
                agg_id: data.get("a").and_then(|x| x.as_i64()).unwrap_or(0),
                first_id: first,
                last_id: last,
            },
            n_raw_trades: n_raw,
        },
    }]
}

pub fn parse_mark(v: &Value, map: &HashMap<String, Instrument>, ts_in: i64) -> Vec<Record> {
    let arr = if let Some(a) = v.as_array() {
        a.clone()
    } else if let Some(a) = v.get("data").and_then(|d| d.as_array()) {
        a.clone()
    } else {
        vec![v.clone()]
    };
    let mut out = Vec::new();
    for data in arr {
        let sym = data.get("s").and_then(|s| s.as_str()).unwrap_or("");
        let Some(inst) = map.get(sym) else {
            continue;
        };
        let ts_ex = data
            .get("E")
            .or_else(|| data.get("T"))
            .and_then(|x| x.as_i64())
            .unwrap_or(ts_in);
        out.push(Record {
            exchange: Exchange::Binance,
            asset_class: inst.asset_class,
            market: Market::Perp,
            channel: Channel::Funding,
            instrument_id: inst.instrument_id.clone(),
            source_symbol: inst.source_symbol.clone(),
            ts_ex,
            ts_in,
            seq: 0,
            body: RecordBody::Funding {
                rate: parse_f64(data.get("r")),
                mark: parse_f64(data.get("p")),
                index: parse_f64(data.get("i")),
                next_ts: data.get("T").and_then(|x| x.as_i64()),
            },
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_info_crypto_and_equity() {
        let v = serde_json::json!({
            "symbols": [
                {"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","baseAsset":"BTC","underlyingType":"COIN"},
                {"symbol":"TSLAUSDT","status":"TRADING","contractType":"TRADIFI_PERPETUAL","quoteAsset":"USDT","baseAsset":"TSLA","underlyingType":"EQUITY"},
                {"symbol":"XAUUSDT","status":"TRADING","contractType":"TRADIFI_PERPETUAL","quoteAsset":"USDT","baseAsset":"XAU","underlyingType":"COMMODITY"},
                {"symbol":"ETHUSDC","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDC","baseAsset":"ETH","underlyingType":"COIN"}
            ]
        });
        let cfg = BinanceConfig {
            asset_classes: vec![AssetClass::Crypto, AssetClass::Equity],
            quote_assets: vec!["USDT".into()],
            channels: vec![],
            symbol_allow: vec![],
        };
        let inst = parse_exchange_info(&v, &cfg);
        let ids: Vec<_> = inst.iter().map(|i| i.source_symbol.as_str()).collect();
        assert!(ids.contains(&"BTCUSDT"));
        assert!(ids.contains(&"TSLAUSDT"));
        assert!(!ids.contains(&"XAUUSDT"));
        assert!(!ids.contains(&"ETHUSDC"));
    }
}

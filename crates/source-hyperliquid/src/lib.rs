use anyhow::Context;
use collector_core::{
    AssetClass, Channel, ControlKind, Exchange, Instrument, Market, Record, RecordBody, TradeDedup,
    TradeSide,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

const INFO: &str = "https://api.hyperliquid.xyz/info";
const WS: &str = "wss://api.hyperliquid.xyz/ws";

#[derive(Clone, Debug)]
pub struct HlConfig {
    pub asset_classes: Vec<AssetClass>,
    pub channels: Vec<Channel>,
    pub hip3_dexes: Vec<String>,
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

pub async fn discover(cfg: &HlConfig) -> anyhow::Result<Vec<Instrument>> {
    let client = reqwest::Client::builder()
        .user_agent("data-collector/0.1")
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut out = Vec::new();
    if cfg.asset_classes.contains(&AssetClass::Crypto) {
        let meta = post_info(&client, json!({"type": "meta"})).await?;
        out.extend(parse_universe(&meta, AssetClass::Crypto, None, cfg));
    }
    if cfg.asset_classes.contains(&AssetClass::Equity) {
        let dexs = if cfg.hip3_dexes.is_empty() {
            vec!["xyz".to_string()]
        } else {
            cfg.hip3_dexes.clone()
        };
        for dex in dexs {
            let meta = post_info(&client, json!({"type": "meta", "dex": dex})).await?;
            out.extend(parse_universe(&meta, AssetClass::Equity, Some(&dex), cfg));
        }
    }
    Ok(out)
}

async fn post_info(client: &reqwest::Client, body: Value) -> anyhow::Result<Value> {
    let resp = client.post(INFO).json(&body).send().await?;
    resp.error_for_status()?.json().await.context("hl info json")
}

fn parse_universe(
    v: &Value,
    class: AssetClass,
    _dex: Option<&str>,
    cfg: &HlConfig,
) -> Vec<Instrument> {
    let allow: Option<std::collections::HashSet<String>> = if cfg.symbol_allow.is_empty() {
        None
    } else {
        Some(cfg.symbol_allow.iter().cloned().collect())
    };
    let mut out = Vec::new();
    let Some(rows) = v.get("universe").and_then(|u| u.as_array()) else {
        return out;
    };
    for c in rows {
        if c.get("isDelisted").and_then(|x| x.as_bool()).unwrap_or(false) {
            continue;
        }
        let name = c.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if name.is_empty() {
            continue;
        }
        if let Some(a) = &allow {
            if !a.contains(name) {
                continue;
            }
        }
        let base = name.rsplit(':').next().unwrap_or(name);
        out.push(Instrument {
            instrument_id: Instrument::id_for(Exchange::Hyperliquid, name),
            exchange: Exchange::Hyperliquid,
            asset_class: class,
            source_symbol: name.to_string(),
            base_asset: base.to_string(),
            quote_asset: "USDC".into(),
            settle_asset: "USDC".into(),
            contract_multiplier: 1.0,
            listed_at_ms: now_ms(),
        });
    }
    out
}

pub async fn run(
    cfg: HlConfig,
    universe: Vec<Instrument>,
    tx: mpsc::Sender<Record>,
    health: Arc<Health>,
) -> anyhow::Result<()> {
    health
        .subscribed
        .store(universe.len() as u64, Ordering::Relaxed);
    let by_coin: HashMap<String, Instrument> = universe
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
    {
        let tx = tx.clone();
        let map = by_coin.clone();
        let h = health.clone();
        let chans = chans.clone();
        tasks.spawn(async move { ws_loop(map, chans, tx, h).await });
    }
    if chans.contains(&Channel::Funding) {
        let tx = tx.clone();
        let map = by_coin.clone();
        let h = health.clone();
        let hip3 = cfg.hip3_dexes.clone();
        tasks.spawn(async move { mark_loop(map, hip3, tx, h).await });
    }
    info!(instruments = universe.len(), "hyperliquid collectors started");
    while let Some(r) = tasks.join_next().await {
        r??;
    }
    Ok(())
}

async fn ws_loop(
    map: HashMap<String, Instrument>,
    chans: Vec<Channel>,
    tx: mpsc::Sender<Record>,
    health: Arc<Health>,
) -> anyhow::Result<()> {
    let mut attempt = 0u32;
    loop {
        match connect_async(WS).await {
            Ok((ws, _)) => {
                info!("hl ws connected");
                if attempt > 0 {
                    health.reconnects.fetch_add(1, Ordering::Relaxed);
                    let _ = tx
                        .send(ctrl(&map, ControlKind::Reconnect, "ws"))
                        .await;
                }
                attempt = 0;
                let (mut sink, mut stream) = ws.split();
                let coins: Vec<String> = map.keys().cloned().collect();
                for chunk in coins.chunks(50) {
                    for coin in chunk {
                        if chans.contains(&Channel::Bbo) {
                            let sub = json!({"method":"subscribe","subscription":{"type":"bbo","coin": coin}});
                            sink.send(tokio_tungstenite::tungstenite::Message::Text(
                                sub.to_string().into(),
                            ))
                            .await?;
                        }
                        if chans.contains(&Channel::Trade) {
                            let sub = json!({"method":"subscribe","subscription":{"type":"trades","coin": coin}});
                            sink.send(tokio_tungstenite::tungstenite::Message::Text(
                                sub.to_string().into(),
                            ))
                            .await?;
                        }
                    }
                }
                while let Some(msg) = stream.next().await {
                    let Ok(msg) = msg else { break; };
                    let Ok(text) = msg.to_text() else { continue; };
                    let ts_in = now_ms();
                    let Ok(v) = serde_json::from_str::<Value>(text) else {
                        continue;
                    };
                    for r in parse_ws(&v, &map, ts_in) {
                        if r.channel == Channel::Trade && r.ts_ex + 30_000 < ts_in {
                            continue;
                        }
                        let lag = (r.ts_in - r.ts_ex).max(0) as u64;
                        health.last_lag_ms.store(lag, Ordering::Relaxed);
                        health.recv.fetch_add(1, Ordering::Relaxed);
                        if tx.send(r).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                let _ = tx.send(ctrl(&map, ControlKind::Disconnect, "ws")).await;
            }
            Err(e) => {
                warn!(error = %e, "hl ws connect failed");
                let _ = tx
                    .send(ctrl(&map, ControlKind::SubscribeFail, &e.to_string()))
                    .await;
            }
        }
        attempt += 1;
        tokio::time::sleep(Duration::from_millis((250u64 << attempt.min(6)).min(10_000))).await;
    }
}

async fn mark_loop(
    map: HashMap<String, Instrument>,
    hip3: Vec<String>,
    tx: mpsc::Sender<Record>,
    health: Arc<Health>,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    loop {
        tick.tick().await;
        let ts_in = now_ms();
        if let Ok(v) = post_info(&client, json!({"type":"metaAndAssetCtxs"})).await {
            emit_ctxs(&v, &map, AssetClass::Crypto, ts_in, &tx, &health).await;
        }
        for dex in &hip3 {
            if let Ok(v) = post_info(
                &client,
                json!({"type":"metaAndAssetCtxs","dex": dex}),
            )
            .await
            {
                emit_ctxs(&v, &map, AssetClass::Equity, ts_in, &tx, &health).await;
            }
        }
    }
}

async fn emit_ctxs(
    v: &Value,
    map: &HashMap<String, Instrument>,
    class: AssetClass,
    ts_in: i64,
    tx: &mpsc::Sender<Record>,
    health: &Health,
) {
    let Some(arr) = v.as_array() else {
        return;
    };
    if arr.len() < 2 {
        return;
    }
    let names: Vec<String> = arr[0]
        .get("universe")
        .and_then(|u| u.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let Some(ctxs) = arr[1].as_array() else {
        return;
    };
    for (i, ctx) in ctxs.iter().enumerate() {
        let Some(name) = names.get(i) else { continue };
        let Some(inst) = map.get(name) else { continue };
        if inst.asset_class != class {
            continue;
        }
        let rec = Record {
            exchange: Exchange::Hyperliquid,
            asset_class: inst.asset_class,
            market: Market::Perp,
            channel: Channel::Funding,
            instrument_id: inst.instrument_id.clone(),
            source_symbol: inst.source_symbol.clone(),
            ts_ex: ts_in,
            ts_in,
            seq: 0,
            body: RecordBody::Funding {
                rate: ctx
                    .get("funding")
                    .and_then(|x| x.as_str())
                    .and_then(|s| s.parse().ok()),
                mark: ctx
                    .get("markPx")
                    .and_then(|x| x.as_str())
                    .and_then(|s| s.parse().ok()),
                index: ctx
                    .get("oraclePx")
                    .and_then(|x| x.as_str())
                    .and_then(|s| s.parse().ok()),
                next_ts: None,
            },
        };
        health.recv.fetch_add(1, Ordering::Relaxed);
        if tx.send(rec).await.is_err() {
            return;
        }
    }
}

fn ctrl(map: &HashMap<String, Instrument>, kind: ControlKind, reason: &str) -> Record {
    let inst = map.values().next();
    Record {
        exchange: Exchange::Hyperliquid,
        asset_class: inst.map(|i| i.asset_class).unwrap_or(AssetClass::Crypto),
        market: Market::Perp,
        channel: Channel::Control,
        instrument_id: inst
            .map(|i| i.instrument_id.clone())
            .unwrap_or_else(|| "hyperliquid:*".into()),
        source_symbol: "*".into(),
        ts_ex: now_ms(),
        ts_in: now_ms(),
        seq: 0,
        body: RecordBody::Control {
            kind,
            reason: reason.into(),
        },
    }
}

pub fn parse_ws(v: &Value, map: &HashMap<String, Instrument>, ts_in: i64) -> Vec<Record> {
    let ch = v.get("channel").and_then(|c| c.as_str()).unwrap_or("");
    match ch {
        "bbo" => parse_bbo(v.get("data").unwrap_or(v), map, ts_in),
        "trades" => parse_trades(v.get("data").unwrap_or(v), map, ts_in),
        _ => Vec::new(),
    }
}

fn parse_px(v: Option<&Value>) -> Option<f64> {
    v.and_then(|x| x.as_str().and_then(|s| s.parse().ok()).or_else(|| x.as_f64()))
}

fn parse_bbo(data: &Value, map: &HashMap<String, Instrument>, ts_in: i64) -> Vec<Record> {
    let coin = data.get("coin").and_then(|c| c.as_str()).unwrap_or("");
    let Some(inst) = map.get(coin) else {
        return Vec::new();
    };
    let bbo = data.get("bbo").and_then(|b| b.as_array());
    let Some(bbo) = bbo else {
        return Vec::new();
    };
    let bid = bbo.first().and_then(|x| parse_px(x.get("px")));
    let ask = bbo.get(1).and_then(|x| parse_px(x.get("px")));
    let (Some(bid), Some(ask)) = (bid, ask) else {
        return Vec::new();
    };
    let ts_ex = data.get("time").and_then(|t| t.as_i64()).unwrap_or(ts_in);
    vec![Record {
        exchange: Exchange::Hyperliquid,
        asset_class: inst.asset_class,
        market: Market::Perp,
        channel: Channel::Bbo,
        instrument_id: inst.instrument_id.clone(),
        source_symbol: inst.source_symbol.clone(),
        ts_ex,
        ts_in,
        seq: 0,
        body: RecordBody::Bbo {
            bid,
            ask,
            bid_sz: bbo
                .first()
                .and_then(|x| parse_px(x.get("sz")))
                .unwrap_or(0.0),
            ask_sz: bbo
                .get(1)
                .and_then(|x| parse_px(x.get("sz")))
                .unwrap_or(0.0),
        },
    }]
}

fn parse_trades(data: &Value, map: &HashMap<String, Instrument>, ts_in: i64) -> Vec<Record> {
    let arr = data.as_array().cloned().unwrap_or_else(|| vec![data.clone()]);
    let mut out = Vec::new();
    for t in arr {
        let coin = t.get("coin").and_then(|c| c.as_str()).unwrap_or("");
        let Some(inst) = map.get(coin) else { continue };
        let px = parse_px(t.get("px"));
        let sz = parse_px(t.get("sz"));
        let (Some(px), Some(sz)) = (px, sz) else { continue };
        let ts_ex = t.get("time").and_then(|x| x.as_i64()).unwrap_or(ts_in);
        let tid = t.get("tid").and_then(|x| x.as_i64()).unwrap_or(0);
        let side = match t.get("side").and_then(|s| s.as_str()) {
            Some("B") => TradeSide::Buy,
            Some("A") | Some("S") => TradeSide::Sell,
            _ => TradeSide::Unknown,
        };
        out.push(Record {
            exchange: Exchange::Hyperliquid,
            asset_class: inst.asset_class,
            market: Market::Perp,
            channel: Channel::Trade,
            instrument_id: inst.instrument_id.clone(),
            source_symbol: inst.source_symbol.clone(),
            ts_ex,
            ts_in,
            seq: tid as u64,
            body: RecordBody::Trade {
                px,
                sz,
                side,
                dedup: TradeDedup::Hyperliquid {
                    ts: ts_ex,
                    coin: coin.to_string(),
                    tid,
                },
                n_raw_trades: 1,
            },
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bbo_native() {
        let inst = Instrument {
            instrument_id: "hyperliquid:BTC".into(),
            exchange: Exchange::Hyperliquid,
            asset_class: AssetClass::Crypto,
            source_symbol: "BTC".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDC".into(),
            settle_asset: "USDC".into(),
            contract_multiplier: 1.0,
            listed_at_ms: 0,
        };
        let mut map = HashMap::new();
        map.insert("BTC".into(), inst);
        let v = serde_json::json!({
            "channel":"bbo",
            "data":{"coin":"BTC","time":1700000000000u64,"bbo":[{"px":"1","sz":"2"},{"px":"3","sz":"4"}]}
        });
        let recs = parse_ws(&v, &map, 1);
        assert_eq!(recs.len(), 1);
        match recs[0].body {
            RecordBody::Bbo { bid, ask, .. } => {
                assert_eq!(bid, 1.0);
                assert_eq!(ask, 3.0);
            }
            _ => panic!(),
        }
    }
}

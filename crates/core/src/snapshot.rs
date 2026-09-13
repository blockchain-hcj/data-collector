use crate::types::Instrument;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastQuote {
    pub bid: f64,
    pub ask: f64,
    pub bid_sz: f64,
    pub ask_sz: f64,
    pub ts_ex: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastMark {
    pub rate: Option<f64>,
    pub mark: Option<f64>,
    pub index: Option<f64>,
    pub next_ts: Option<i64>,
    pub ts_ex: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AggSnapshot {
    pub quotes: HashMap<String, LastQuote>,
    pub marks: HashMap<String, LastMark>,
    pub instruments: HashMap<String, Instrument>,
    pub disconnected: bool,
    pub disconnect_since_ms: Option<i64>,
}

impl AggSnapshot {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(postcard::to_stdvec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar1s {
    pub exchange: String,
    pub asset_class: String,
    pub instrument_id: String,
    pub source_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub settle_asset: String,
    pub contract_multiplier: f64,
    pub ts_sec: i64,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bid_sz: Option<f64>,
    pub ask_sz: Option<f64>,
    pub quote_valid: bool,
    pub quote_age_ms: Option<i64>,
    pub n_bbo: u32,
    pub px_open: Option<f64>,
    pub px_high: Option<f64>,
    pub px_low: Option<f64>,
    pub px_close: Option<f64>,
    pub volume: f64,
    pub quote_volume: f64,
    pub taker_buy_volume: f64,
    pub n_trades: u64,
    pub n_trade_msgs: u32,
    pub mark: Option<f64>,
    pub funding_rate: Option<f64>,
    pub gap: bool,
    pub late_count: u32,
}

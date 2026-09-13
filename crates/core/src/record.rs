use crate::types::{AssetClass, Channel, Exchange, Market};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub exchange: Exchange,
    pub asset_class: AssetClass,
    pub market: Market,
    pub channel: Channel,
    pub instrument_id: String,
    pub source_symbol: String,
    pub ts_ex: i64,
    pub ts_in: i64,
    pub seq: u64,
    pub body: RecordBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RecordBody {
    Bbo {
        bid: f64,
        ask: f64,
        bid_sz: f64,
        ask_sz: f64,
    },
    Trade {
        px: f64,
        sz: f64,
        side: TradeSide,
        dedup: TradeDedup,
        n_raw_trades: u32,
    },
    Funding {
        rate: Option<f64>,
        mark: Option<f64>,
        index: Option<f64>,
        next_ts: Option<i64>,
    },
    Control {
        kind: ControlKind,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TradeDedup {
    BinanceAgg { agg_id: i64, first_id: i64, last_id: i64 },
    Hyperliquid { ts: i64, coin: String, tid: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlKind {
    Disconnect,
    SubscribeFail,
    Reconnect,
}

impl Record {
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(postcard::to_stdvec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

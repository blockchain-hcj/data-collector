use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    Binance,
    Hyperliquid,
}

impl Exchange {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Hyperliquid => "hyperliquid",
        }
    }
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Exchange {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "binance" => Ok(Self::Binance),
            "hyperliquid" => Ok(Self::Hyperliquid),
            other => anyhow::bail!("unknown exchange {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetClass {
    Crypto,
    Equity,
}

impl AssetClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crypto => "crypto",
            Self::Equity => "equity",
        }
    }
}

impl fmt::Display for AssetClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AssetClass {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "crypto" => Ok(Self::Crypto),
            "equity" => Ok(Self::Equity),
            other => anyhow::bail!("unknown asset_class {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Bbo,
    Trade,
    Funding,
    Control,
}

impl Channel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bbo => "bbo",
            Self::Trade => "trade",
            Self::Funding => "funding",
            Self::Control => "control",
        }
    }

    pub const ALL: [Channel; 4] = [Self::Bbo, Self::Trade, Self::Funding, Self::Control];
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Channel {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "bbo" => Ok(Self::Bbo),
            "trade" => Ok(Self::Trade),
            "funding" => Ok(Self::Funding),
            "control" => Ok(Self::Control),
            other => anyhow::bail!("unknown channel {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Market {
    Perp,
}

impl Market {
    pub fn as_str(self) -> &'static str {
        "perp"
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instrument {
    pub instrument_id: String,
    pub exchange: Exchange,
    pub asset_class: AssetClass,
    pub source_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub settle_asset: String,
    pub contract_multiplier: f64,
    pub listed_at_ms: i64,
}

impl Instrument {
    pub fn id_for(exchange: Exchange, source_symbol: &str) -> String {
        format!("{}:{source_symbol}", exchange.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId {
    pub exchange: Exchange,
    pub asset_class: AssetClass,
    pub channel: Channel,
}

impl StreamId {
    pub fn new(exchange: Exchange, asset_class: AssetClass, channel: Channel) -> Self {
        Self {
            exchange,
            asset_class,
            channel,
        }
    }

    pub fn rel_dir(&self) -> String {
        format!(
            "{}/{}/{}",
            self.exchange.as_str(),
            self.asset_class.as_str(),
            self.channel.as_str()
        )
    }
}

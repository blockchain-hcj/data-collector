pub mod config;
pub mod meta;
pub mod record;
pub mod snapshot;
pub mod types;
pub mod universe;
pub mod wal;

pub use config::{load_exchange_file, CollectorConfig, ExchangeFileConfig};
pub use meta::{BatchRow, MetaStore};
pub use record::{ControlKind, Record, RecordBody, TradeDedup, TradeSide};
pub use snapshot::{AggSnapshot, Bar1s, LastMark, LastQuote};
pub use types::{AssetClass, Channel, Exchange, Instrument, Market, StreamId};
pub use universe::{load_universe, save_universe};
pub use wal::{read_durable_offset, read_epoch, WalReader, WalWriter};

pub fn crc64_iso(data: &[u8]) -> u64 {
    // ISO 3309 CRC-64 (also used by Redis). Good enough for content checksum.
    const POLY: u64 = 0x42F0_E1EB_A9EA_3693;
    let mut crc = 0u64;
    for &b in data {
        crc ^= (b as u64) << 56;
        for _ in 0..8 {
            if crc & 0x8000_0000_0000_0000 != 0 {
                crc = (crc << 1) ^ POLY;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

pub fn crc64_hex(data: &[u8]) -> String {
    format!("{:016x}", crc64_iso(data))
}

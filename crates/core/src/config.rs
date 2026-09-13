use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub reaper: ReaperConfig,
    #[serde(default)]
    pub archive: ArchiveConfig,
    #[serde(default)]
    pub oss: OssConfig,
    #[serde(default)]
    pub wal: WalConfig,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    #[serde(default = "default_sync_interval")]
    pub sync_interval_ms: u64,
    #[serde(default = "default_sync_bytes")]
    pub sync_bytes: u64,
    #[serde(default = "default_segment_bytes")]
    pub segment_max_bytes: u64,
    #[serde(default = "default_segment_ms")]
    pub segment_max_ms: u64,
    #[serde(default = "default_queue")]
    pub queue_capacity: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            sync_interval_ms: 10,
            sync_bytes: 64 * 1024,
            segment_max_bytes: 256 * 1024 * 1024,
            segment_max_ms: 60_000,
            queue_capacity: 65_536,
        }
    }
}

fn default_sync_interval() -> u64 {
    10
}
fn default_sync_bytes() -> u64 {
    64 * 1024
}
fn default_segment_bytes() -> u64 {
    256 * 1024 * 1024
}
fn default_segment_ms() -> u64 {
    60_000
}
fn default_queue() -> usize {
    65_536
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaperConfig {
    #[serde(default = "default_reaper_interval", with = "humantime_serde_opt")]
    pub interval: Duration,
    #[serde(default = "default_wal_retention", with = "humantime_serde_opt")]
    pub wal_retention: Duration,
    #[serde(default = "default_local_parquet", with = "humantime_serde_opt")]
    pub local_parquet_retention: Duration,
    #[serde(default = "default_watermark")]
    pub disk_high_watermark: f64,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            wal_retention: Duration::from_secs(24 * 3600),
            local_parquet_retention: Duration::from_secs(24 * 3600),
            disk_high_watermark: 0.80,
        }
    }
}

fn default_reaper_interval() -> Duration {
    Duration::from_secs(60)
}
fn default_wal_retention() -> Duration {
    Duration::from_secs(24 * 3600)
}
fn default_local_parquet() -> Duration {
    Duration::from_secs(24 * 3600)
}
fn default_watermark() -> f64 {
    0.80
}

mod humantime_serde_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        let secs = d.as_secs();
        if secs % 3600 == 0 && secs >= 3600 {
            s.serialize_str(&format!("{}h", secs / 3600))
        } else if secs % 60 == 0 {
            s.serialize_str(&format!("{}m", secs / 60))
        } else {
            s.serialize_str(&format!("{secs}s"))
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let s = String::deserialize(d)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }

    fn parse_duration(s: &str) -> Result<Duration, String> {
        let s = s.trim();
        let (n, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
        let n: u64 = n.parse().map_err(|_| format!("bad duration {s}"))?;
        match unit {
            "s" | "" => Ok(Duration::from_secs(n)),
            "m" => Ok(Duration::from_secs(n * 60)),
            "h" => Ok(Duration::from_secs(n * 3600)),
            "d" => Ok(Duration::from_secs(n * 86400)),
            _ => Err(format!("bad duration unit in {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveConfig {
    #[serde(default)]
    pub bar_1s: Bar1sConfig,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            bar_1s: Bar1sConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar1sConfig {
    #[serde(default = "default_closed_seconds")]
    pub max_closed_seconds: u32,
    #[serde(default = "default_hour_watermark")]
    pub hour_watermark_ms: i64,
    #[serde(default = "default_true")]
    pub rotate_on_utc_date_change: bool,
}

impl Default for Bar1sConfig {
    fn default() -> Self {
        Self {
            max_closed_seconds: 3600,
            hour_watermark_ms: 2000,
            rotate_on_utc_date_change: true,
        }
    }
}

fn default_closed_seconds() -> u32 {
    3600
}
fn default_hour_watermark() -> i64 {
    2000
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OssConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default = "default_role")]
    pub role_name: String,
    #[serde(default = "default_upload")]
    pub upload: String,
    #[serde(default = "default_poll", with = "humantime_serde_opt")]
    pub poll_interval: Duration,
}

impl Default for OssConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: String::new(),
            bucket: String::new(),
            prefix: default_prefix(),
            role_name: default_role(),
            upload: default_upload(),
            poll_interval: Duration::from_secs(1),
        }
    }
}

fn default_prefix() -> String {
    "lake".into()
}
fn default_role() -> String {
    "ecs-oss-writer".into()
}
fn default_upload() -> String {
    "on_part_close".into()
}
fn default_poll() -> Duration {
    Duration::from_secs(1)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeFileConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub asset_classes: Vec<String>,
    #[serde(default)]
    pub quote_assets: Vec<String>,
    #[serde(default)]
    pub hip3_dexes: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            reaper: ReaperConfig::default(),
            archive: ArchiveConfig::default(),
            oss: OssConfig::default(),
            wal: WalConfig::default(),
        }
    }
}

impl CollectorConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&text)?)
    }

    pub fn wal_root(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    pub fn lake_root(&self) -> PathBuf {
        self.data_dir.join("lake")
    }

    pub fn meta_root(&self) -> PathBuf {
        self.data_dir.join("meta")
    }
}

pub fn load_exchange_file(path: impl AsRef<Path>) -> anyhow::Result<ExchangeFileConfig> {
    let text = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&text)?)
}

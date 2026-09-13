use crate::types::{Exchange, Instrument};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UniverseFile {
    pub exchange: String,
    pub instruments: Vec<Instrument>,
}

pub fn save_universe(data_dir: &Path, exchange: Exchange, inst: &[Instrument]) -> anyhow::Result<()> {
    let dir = data_dir.join("universe");
    fs::create_dir_all(&dir)?;
    let f = UniverseFile {
        exchange: exchange.as_str().into(),
        instruments: inst.to_vec(),
    };
    let path = dir.join(format!("{}.json", exchange.as_str()));
    fs::write(path, serde_json::to_vec_pretty(&f)?)?;
    Ok(())
}

pub fn load_universe(data_dir: &Path, exchange: Exchange) -> anyhow::Result<Vec<Instrument>> {
    let path = data_dir.join("universe").join(format!("{}.json", exchange.as_str()));
    if !path.exists() {
        return Ok(Vec::new());
    }
    let f: UniverseFile = serde_json::from_slice(&fs::read(path)?)?;
    Ok(f.instruments)
}

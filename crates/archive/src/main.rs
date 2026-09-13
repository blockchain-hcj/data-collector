use anyhow::Context;
use collector_archive::agg;
use collector_core::CollectorConfig;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let mut config_path = PathBuf::from("config/collector.yaml");
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" => {
                config_path = PathBuf::from(args.next().context("config")?);
            }
            other => anyhow::bail!("unknown {other}"),
        }
    }
    let cfg = CollectorConfig::load(&config_path)?;
    agg::run_loop(cfg)
}

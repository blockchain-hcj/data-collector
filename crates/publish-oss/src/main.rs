use anyhow::Context;
use collector_core::{crc64_hex, CollectorConfig, Exchange, MetaStore};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue, DATE};
use sha1::Sha1;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

type HmacSha1 = Hmac<Sha1>;

#[derive(Clone)]
struct Creds {
    ak: String,
    sk: String,
    token: Option<String>,
}

async fn ecs_creds(role: &str) -> anyhow::Result<Creds> {
    let url = format!(
        "http://100.100.100.200/latest/meta-data/ram/security-credentials/{role}"
    );
    let v: serde_json::Value = reqwest::Client::new()
        .get(url)
        .timeout(Duration::from_secs(3))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(Creds {
        ak: v["AccessKeyId"].as_str().unwrap_or("").into(),
        sk: v["AccessKeySecret"].as_str().unwrap_or("").into(),
        token: v["SecurityToken"].as_str().map(|s| s.to_string()),
    })
}

fn env_creds() -> Option<Creds> {
    let ak = std::env::var("OSS_ACCESS_KEY_ID").ok()?;
    let sk = std::env::var("OSS_ACCESS_KEY_SECRET").ok()?;
    Some(Creds {
        ak,
        sk,
        token: std::env::var("OSS_SECURITY_TOKEN").ok(),
    })
}

fn gmt_date() -> String {
    chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn sign_put(creds: &Creds, bucket: &str, object: &str, date: &str, content_md5: &str) -> String {
    let mut headers = String::new();
    if let Some(tok) = &creds.token {
        headers.push_str(&format!("x-oss-security-token:{tok}\n"));
    }
    let canonical = format!(
        "PUT\n{content_md5}\napplication/octet-stream\n{date}\n{headers}/{bucket}/{object}"
    );
    let mut mac = HmacSha1::new_from_slice(creds.sk.as_bytes()).expect("hmac");
    mac.update(canonical.as_bytes());
    let sig = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, mac.finalize().into_bytes());
    format!("OSS {}:{}", creds.ak, sig)
}

async fn put_object(
    client: &reqwest::Client,
    cfg: &collector_core::config::OssConfig,
    creds: &Creds,
    object: &str,
    body: Vec<u8>,
) -> anyhow::Result<()> {
    let date = gmt_date();
    let md5 = md5_base64(&body);
    let auth = sign_put(creds, &cfg.bucket, object, &date, &md5);
    let host = cfg
        .endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let url = format!("https://{}.{}/{}", cfg.bucket, host, object);
    let mut headers = HeaderMap::new();
    headers.insert(DATE, HeaderValue::from_str(&date)?);
    headers.insert("Authorization", HeaderValue::from_str(&auth)?);
    headers.insert("Content-MD5", HeaderValue::from_str(&md5)?);
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Some(tok) = &creds.token {
        headers.insert("x-oss-security-token", HeaderValue::from_str(tok)?);
    }
    let resp = client
        .put(url)
        .headers(headers)
        .body(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        let st = resp.status();
        let t = resp.text().await.unwrap_or_default();
        anyhow::bail!("oss put {object} {st}: {t}");
    }
    Ok(())
}

fn md5_base64(data: &[u8]) -> String {
    // RFC 1321 MD5 via md-5 crate would be better; approximate with hex of crc if no md5.
    // We add md-5 in Cargo.toml
    use md5::{Digest, Md5};
    let d = Md5::digest(data);
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, d)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
            "--config" => config_path = PathBuf::from(args.next().context("config")?),
            other => anyhow::bail!("unknown {other}"),
        }
    }
    let cfg = CollectorConfig::load(&config_path)?;
    if !cfg.oss.enabled {
        anyhow::bail!("oss.enabled=false");
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    loop {
        let creds = match ecs_creds(&cfg.oss.role_name).await {
            Ok(c) if !c.ak.is_empty() => c,
            _ => env_creds().context("no ECS role creds and no OSS_ACCESS_KEY_ID")?,
        };
        for ex in [Exchange::Binance, Exchange::Hyperliquid] {
            let meta = match MetaStore::open(&cfg.meta_root(), ex) {
                Ok(m) => m,
                Err(_) => continue,
            };
            for mut batch in meta.pending_uploads()? {
                batch.exchange = ex;
                let path = cfg.lake_root().join(&batch.file_relpath);
                if !path.exists() {
                    warn!(path = %path.display(), "missing parquet");
                    continue;
                }
                let bytes = std::fs::read(&path)?;
                let sum = crc64_hex(&bytes);
                if sum != batch.checksum {
                    warn!(
                        batch = %batch.batch_id,
                        expected = %batch.checksum,
                        got = %sum,
                        "checksum mismatch, skip"
                    );
                    continue;
                }
                let object = if cfg.oss.prefix.is_empty() {
                    batch.file_relpath.clone()
                } else {
                    format!("{}/{}", cfg.oss.prefix.trim_end_matches('/'), batch.file_relpath)
                };
                match put_object(&client, &cfg.oss, &creds, &object, bytes).await {
                    Ok(()) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        meta.mark_uploaded(&batch.batch_id, now)?;
                        info!(batch = %batch.batch_id, object, "uploaded");
                    }
                    Err(e) => warn!(batch = %batch.batch_id, error = %e, "upload failed"),
                }
            }
        }
        tokio::time::sleep(cfg.oss.poll_interval).await;
    }
}

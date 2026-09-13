#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
HOST="${HOST:-root@8.216.24.192}"
KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"

rustup target add x86_64-unknown-linux-gnu >/dev/null
cargo build --release --target x86_64-unknown-linux-gnu \
  -p collector-collect -p collector-archive -p collector-publish-oss

BIN=target/x86_64-unknown-linux-gnu/release
ssh -i "$KEY" "$HOST" "mkdir -p /opt/data-collector/config/exchanges /opt/data-collector/sql /opt/data-collector/data"

scp -i "$KEY" "$BIN/collect" "$BIN/archive" "$BIN/publish-oss" "$HOST:/opt/data-collector/"
scp -i "$KEY" deploy/collector.yaml "$HOST:/opt/data-collector/config/collector.yaml"
scp -i "$KEY" config/exchanges/binance.yaml config/exchanges/hyperliquid.yaml \
  "$HOST:/opt/data-collector/config/exchanges/"
scp -i "$KEY" sql/bar_1s.sql "$HOST:/opt/data-collector/sql/"
scp -i "$KEY" deploy/collect@.service "$HOST:/etc/systemd/system/data-collector-collect@.service"
scp -i "$KEY" deploy/archive.service "$HOST:/etc/systemd/system/data-collector-archive.service"
scp -i "$KEY" deploy/publish-oss.service "$HOST:/etc/systemd/system/data-collector-publish-oss.service"

ssh -i "$KEY" "$HOST" 'chmod +x /opt/data-collector/collect /opt/data-collector/archive /opt/data-collector/publish-oss
systemctl daemon-reload
systemctl enable --now data-collector-collect@binance data-collector-collect@hyperliquid data-collector-archive data-collector-publish-oss
systemctl restart data-collector-collect@binance data-collector-collect@hyperliquid data-collector-archive data-collector-publish-oss
sleep 2
systemctl --no-pager --full status data-collector-collect@binance data-collector-collect@hyperliquid data-collector-archive data-collector-publish-oss | cat'
echo "deployed to $HOST:/opt/data-collector"

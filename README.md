# data-collector

Perp collector: Binance + Hyperliquid, crypto and equity. Ticks go to a local WAL, archive writes 1-second bars as Parquet, optional OSS upload.

```bash
# collect (one process per exchange)
cargo run -p collector-collect --release -- --exchange binance --config config/collector.yaml
cargo run -p collector-collect --release -- --exchange hyperliquid --config config/collector.yaml

# aggregate WAL → hourly 1s parquet
cargo run -p collector-archive --release -- --config config/collector.yaml

# upload committed parquet (set oss.enabled and bucket)
cargo run -p collector-publish-oss --release -- --config config/collector.yaml
```

Query:

```sql
-- sql/bar_1s.sql
SELECT exchange, asset_class, count(DISTINCT instrument_id)
FROM read_parquet('data/lake/exchange=*/asset_class=*/channel=bar_1s/date=*/hour=*/*.parquet', hive_partitioning := true)
GROUP BY 1, 2;
```

CREATE VIEW IF NOT EXISTS bar_1s AS
SELECT * FROM read_parquet(
  'data/lake/exchange=*/asset_class=*/channel=bar_1s/date=*/hour=*/*.parquet',
  hive_partitioning := true
);

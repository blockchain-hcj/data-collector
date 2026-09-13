use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use collector_core::Bar1s;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("exchange", DataType::Utf8, false),
        Field::new("asset_class", DataType::Utf8, false),
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("source_symbol", DataType::Utf8, false),
        Field::new("base_asset", DataType::Utf8, false),
        Field::new("quote_asset", DataType::Utf8, false),
        Field::new("settle_asset", DataType::Utf8, false),
        Field::new("contract_multiplier", DataType::Float64, false),
        Field::new("ts_sec", DataType::Int64, false),
        Field::new("bid", DataType::Float64, true),
        Field::new("ask", DataType::Float64, true),
        Field::new("bid_sz", DataType::Float64, true),
        Field::new("ask_sz", DataType::Float64, true),
        Field::new("quote_valid", DataType::Boolean, false),
        Field::new("quote_age_ms", DataType::Int64, true),
        Field::new("n_bbo", DataType::UInt32, false),
        Field::new("px_open", DataType::Float64, true),
        Field::new("px_high", DataType::Float64, true),
        Field::new("px_low", DataType::Float64, true),
        Field::new("px_close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, false),
        Field::new("quote_volume", DataType::Float64, false),
        Field::new("taker_buy_volume", DataType::Float64, false),
        Field::new("n_trades", DataType::UInt64, false),
        Field::new("n_trade_msgs", DataType::UInt32, false),
        Field::new("mark", DataType::Float64, true),
        Field::new("funding_rate", DataType::Float64, true),
        Field::new("gap", DataType::Boolean, false),
        Field::new("late_count", DataType::UInt32, false),
    ])
}

fn opt_f64(b: &mut Float64Builder, v: Option<f64>) {
    match v {
        Some(x) => b.append_value(x),
        None => b.append_null(),
    }
}

pub fn write_bars(path: &Path, mut rows: Vec<Bar1s>) -> anyhow::Result<(u64, String)> {
    rows.sort_by(|a, b| {
        a.instrument_id
            .cmp(&b.instrument_id)
            .then(a.ts_sec.cmp(&b.ts_sec))
    });
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let schema = Arc::new(schema());
    let n = rows.len();
    let mut exchange = StringBuilder::with_capacity(n, n * 8);
    let mut asset_class = StringBuilder::with_capacity(n, n * 8);
    let mut instrument_id = StringBuilder::with_capacity(n, n * 16);
    let mut source_symbol = StringBuilder::with_capacity(n, n * 8);
    let mut base_asset = StringBuilder::with_capacity(n, n * 8);
    let mut quote_asset = StringBuilder::with_capacity(n, n * 8);
    let mut settle_asset = StringBuilder::with_capacity(n, n * 8);
    let mut contract_multiplier = Float64Builder::with_capacity(n);
    let mut ts_sec = Int64Builder::with_capacity(n);
    let mut bid = Float64Builder::with_capacity(n);
    let mut ask = Float64Builder::with_capacity(n);
    let mut bid_sz = Float64Builder::with_capacity(n);
    let mut ask_sz = Float64Builder::with_capacity(n);
    let mut quote_valid = BooleanBuilder::with_capacity(n);
    let mut quote_age_ms = Int64Builder::with_capacity(n);
    let mut n_bbo = UInt32Builder::with_capacity(n);
    let mut px_open = Float64Builder::with_capacity(n);
    let mut px_high = Float64Builder::with_capacity(n);
    let mut px_low = Float64Builder::with_capacity(n);
    let mut px_close = Float64Builder::with_capacity(n);
    let mut volume = Float64Builder::with_capacity(n);
    let mut quote_volume = Float64Builder::with_capacity(n);
    let mut taker_buy_volume = Float64Builder::with_capacity(n);
    let mut n_trades = UInt64Builder::with_capacity(n);
    let mut n_trade_msgs = UInt32Builder::with_capacity(n);
    let mut mark = Float64Builder::with_capacity(n);
    let mut funding_rate = Float64Builder::with_capacity(n);
    let mut gap = BooleanBuilder::with_capacity(n);
    let mut late_count = UInt32Builder::with_capacity(n);

    for r in &rows {
        exchange.append_value(&r.exchange);
        asset_class.append_value(&r.asset_class);
        instrument_id.append_value(&r.instrument_id);
        source_symbol.append_value(&r.source_symbol);
        base_asset.append_value(&r.base_asset);
        quote_asset.append_value(&r.quote_asset);
        settle_asset.append_value(&r.settle_asset);
        contract_multiplier.append_value(r.contract_multiplier);
        ts_sec.append_value(r.ts_sec);
        opt_f64(&mut bid, r.bid);
        opt_f64(&mut ask, r.ask);
        opt_f64(&mut bid_sz, r.bid_sz);
        opt_f64(&mut ask_sz, r.ask_sz);
        quote_valid.append_value(r.quote_valid);
        match r.quote_age_ms {
            Some(x) => quote_age_ms.append_value(x),
            None => quote_age_ms.append_null(),
        }
        n_bbo.append_value(r.n_bbo);
        opt_f64(&mut px_open, r.px_open);
        opt_f64(&mut px_high, r.px_high);
        opt_f64(&mut px_low, r.px_low);
        opt_f64(&mut px_close, r.px_close);
        volume.append_value(r.volume);
        quote_volume.append_value(r.quote_volume);
        taker_buy_volume.append_value(r.taker_buy_volume);
        n_trades.append_value(r.n_trades);
        n_trade_msgs.append_value(r.n_trade_msgs);
        opt_f64(&mut mark, r.mark);
        opt_f64(&mut funding_rate, r.funding_rate);
        gap.append_value(r.gap);
        late_count.append_value(r.late_count);
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(exchange.finish()),
        Arc::new(asset_class.finish()),
        Arc::new(instrument_id.finish()),
        Arc::new(source_symbol.finish()),
        Arc::new(base_asset.finish()),
        Arc::new(quote_asset.finish()),
        Arc::new(settle_asset.finish()),
        Arc::new(contract_multiplier.finish()),
        Arc::new(ts_sec.finish()),
        Arc::new(bid.finish()),
        Arc::new(ask.finish()),
        Arc::new(bid_sz.finish()),
        Arc::new(ask_sz.finish()),
        Arc::new(quote_valid.finish()),
        Arc::new(quote_age_ms.finish()),
        Arc::new(n_bbo.finish()),
        Arc::new(px_open.finish()),
        Arc::new(px_high.finish()),
        Arc::new(px_low.finish()),
        Arc::new(px_close.finish()),
        Arc::new(volume.finish()),
        Arc::new(quote_volume.finish()),
        Arc::new(taker_buy_volume.finish()),
        Arc::new(n_trades.finish()),
        Arc::new(n_trade_msgs.finish()),
        Arc::new(mark.finish()),
        Arc::new(funding_rate.finish()),
        Arc::new(gap.finish()),
        Arc::new(late_count.finish()),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols)?;
    {
        let file = File::create(&tmp)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
            .set_dictionary_enabled(true)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    let bytes = fs::read(&tmp)?;
    let checksum = collector_core::crc64_hex(&bytes);
    let f = File::open(&tmp)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        File::open(dir)?.sync_all()?;
    }
    Ok((bytes.len() as u64, checksum))
}

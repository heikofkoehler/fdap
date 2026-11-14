#![allow(deprecated)]

use anyhow::{Context, Result, bail};
use arrow::array::{AsArray, BooleanArray, Float64Array};
use arrow::datatypes::Float64Type;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::{AsyncArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnPath;
use serde::{Deserialize, Serialize};

use crate::storage::MinioStorage;

/// Summary returned after writing Arrow `RecordBatch`es to a Parquet object on MinIO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetWriteSummary {
    pub bucket: String,
    pub object_key: String,
    pub num_batches: usize,
    pub num_rows: usize,
    pub num_row_groups: usize,
    pub object_size_bytes: u64,
}

/// Summary of a single Parquet row group's metadata and column statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupReport {
    pub index: usize,
    pub num_rows: i64,
    pub total_byte_size: i64,
    pub compressed_size: i64,
    pub columns: Vec<ColumnChunkReport>,
}

/// Summary of a single column chunk within a Parquet row group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChunkReport {
    pub column_path: String,
    pub compression: String,
    pub uncompressed_bytes: i64,
    pub compressed_bytes: i64,
    pub null_count: Option<u64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

/// Complete metadata report extracted from a Parquet footer on MinIO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetMetadataReport {
    pub object_key: String,
    pub object_size_bytes: u64,
    pub created_by: Option<String>,
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub row_groups: Vec<RowGroupReport>,
}

/// Builds tuned Parquet [`WriterProperties`] with ZSTD compression, page-level statistics,
/// bloom filters on key string columns, and configurable row group sizing.
pub fn build_writer_properties(max_row_group_size: usize) -> WriterProperties {
    let zstd_level = ZstdLevel::try_new(3).unwrap_or_default();
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(max_row_group_size))
        .set_column_bloom_filter_enabled(ColumnPath::from("device_id"), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("service"), true)
        .set_created_by("fdap-engine-rs".to_string())
        .build()
}

/// Streams a slice of Arrow [`RecordBatch`]es directly to a Parquet object in MinIO
/// using [`ParquetObjectWriter`] and [`AsyncArrowWriter`].
pub async fn write_parquet_to_minio(
    storage: &MinioStorage,
    object_key: &str,
    batches: &[RecordBatch],
    max_row_group_size: usize,
) -> Result<ParquetWriteSummary> {
    if batches.is_empty() {
        bail!("Cannot write empty RecordBatch slice to Parquet");
    }

    let schema = batches[0].schema();
    let props = build_writer_properties(max_row_group_size);
    let path = ObjectPath::from(object_key);

    let object_writer = ParquetObjectWriter::new(storage.as_object_store(), path.clone());
    let mut writer = AsyncArrowWriter::try_new(object_writer, schema, Some(props))
        .context("Failed to initialize AsyncArrowWriter for MinIO")?;

    let mut total_rows = 0usize;
    for batch in batches {
        total_rows += batch.num_rows();
        writer
            .write(batch)
            .await
            .context("Failed writing RecordBatch to MinIO Parquet stream")?;
    }

    let file_meta = writer
        .close()
        .await
        .context("Failed finalizing Parquet footer on MinIO")?;

    let head = storage
        .as_object_store()
        .head(&path)
        .await
        .context("Failed to HEAD written Parquet object on MinIO")?;

    Ok(ParquetWriteSummary {
        bucket: storage.config().bucket.clone(),
        object_key: object_key.to_string(),
        num_batches: batches.len(),
        num_rows: total_rows,
        num_row_groups: file_meta.row_groups().len(),
        object_size_bytes: head.size,
    })
}

/// Reads only the Parquet footer from MinIO using HTTP Range requests and produces a
/// detailed [`ParquetMetadataReport`].
pub async fn inspect_parquet_on_minio(
    storage: &MinioStorage,
    object_key: &str,
) -> Result<ParquetMetadataReport> {
    let path = ObjectPath::from(object_key);
    let head = storage
        .as_object_store()
        .head(&path)
        .await
        .with_context(|| format!("Object '{}' not found in MinIO", object_key))?;

    let reader =
        ParquetObjectReader::new(storage.as_object_store(), path).with_file_size(head.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .context("Failed to read Parquet footer from MinIO")?;

    Ok(extract_metadata_report(
        object_key,
        head.size,
        builder.metadata(),
    ))
}

/// Extracts a structured [`ParquetMetadataReport`] from [`ParquetMetaData`].
pub fn extract_metadata_report(
    object_key: &str,
    object_size_bytes: u64,
    metadata: &ParquetMetaData,
) -> ParquetMetadataReport {
    let file_meta = metadata.file_metadata();
    let mut row_groups = Vec::with_capacity(metadata.num_row_groups());

    for (idx, rg) in metadata.row_groups().iter().enumerate() {
        let mut columns = Vec::with_capacity(rg.num_columns());
        for col in rg.columns() {
            let (null_count, min_value, max_value) = match col.statistics() {
                Some(stats) => (
                    stats.null_count_opt(),
                    format_stat_min(stats),
                    format_stat_max(stats),
                ),
                None => (None, None, None),
            };
            columns.push(ColumnChunkReport {
                column_path: col.column_path().string(),
                compression: format!("{:?}", col.compression()),
                uncompressed_bytes: col.uncompressed_size(),
                compressed_bytes: col.compressed_size(),
                null_count,
                min_value,
                max_value,
            });
        }

        row_groups.push(RowGroupReport {
            index: idx,
            num_rows: rg.num_rows(),
            total_byte_size: rg.total_byte_size(),
            compressed_size: rg.compressed_size(),
            columns,
        });
    }

    ParquetMetadataReport {
        object_key: object_key.to_string(),
        object_size_bytes,
        created_by: file_meta.created_by().map(ToString::to_string),
        num_rows: file_meta.num_rows(),
        num_row_groups: metadata.num_row_groups(),
        row_groups,
    }
}

/// Reads a Parquet object from MinIO using async HTTP Range reads with optional:
/// - **Column projection pushdown** (`projected_columns`)
/// - **Row group pruning** via column chunk statistics (`min_latency_ms`)
/// - **Row-level predicate pushdown** (`RowFilter` on `latency_ms >= min_latency_ms`)
pub async fn read_parquet_from_minio_with_pushdown(
    storage: &MinioStorage,
    object_key: &str,
    projected_columns: Option<&[&str]>,
    min_latency_ms: Option<f64>,
) -> Result<(Vec<RecordBatch>, usize, usize)> {
    let path = ObjectPath::from(object_key);
    let head = storage.as_object_store().head(&path).await?;
    let reader =
        ParquetObjectReader::new(storage.as_object_store(), path).with_file_size(head.size);

    let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let total_row_groups = builder.metadata().num_row_groups();
    let parquet_schema = builder.parquet_schema().clone();
    let arrow_schema = builder.schema().clone();

    // 1. Row-group pruning using Parquet statistics on `latency_ms`
    let mut selected_row_groups: Vec<usize> = (0..total_row_groups).collect();
    if let Some(threshold) = min_latency_ms {
        if let Some(lat_col_idx) = parquet_schema
            .columns()
            .iter()
            .position(|c| c.name() == "latency_ms")
        {
            selected_row_groups.retain(|&rg_idx| {
                let rg = builder.metadata().row_group(rg_idx);
                if let Some(Statistics::Double(stats)) = rg.column(lat_col_idx).statistics() {
                    if let Some(&max_val) = stats.max_opt() {
                        return max_val >= threshold;
                    }
                }
                true
            });
        }

        // 2. Fine-grained RowFilter predicate pushdown on `latency_ms`
        if let Some(lat_col_idx) = parquet_schema
            .columns()
            .iter()
            .position(|c| c.name() == "latency_ms")
        {
            let predicate_mask = ProjectionMask::leaves(&parquet_schema, [lat_col_idx]);
            let predicate = ArrowPredicateFn::new(predicate_mask, move |batch: RecordBatch| {
                let col: &Float64Array = batch.column(0).as_primitive::<Float64Type>();
                let mask: BooleanArray = col
                    .iter()
                    .map(|opt| opt.map(|v| v >= threshold))
                    .collect();
                Ok(mask)
            });
            builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
        }
    }

    let kept_row_groups = selected_row_groups.len();
    builder = builder.with_row_groups(selected_row_groups);

    // 3. Column projection pushdown (only fetch required column chunks from MinIO)
    if let Some(cols) = projected_columns {
        let root_indices: Vec<usize> = cols
            .iter()
            .filter_map(|name| arrow_schema.index_of(name).ok())
            .collect();
        let mask = ProjectionMask::roots(&parquet_schema, root_indices);
        builder = builder.with_projection(mask);
    }

    let stream = builder.build()?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    Ok((batches, kept_row_groups, total_row_groups))
}

fn format_stat_min(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::Boolean(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Int32(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Int64(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Int96(s) => s.min_opt().map(|v| format!("{:?}", v)),
        Statistics::Float(s) => s.min_opt().map(|v| format!("{:.2}", v)),
        Statistics::Double(s) => s.min_opt().map(|v| format!("{:.2}", v)),
        Statistics::ByteArray(s) => s
            .min_opt()
            .map(|v| String::from_utf8_lossy(v.data()).to_string()),
        Statistics::FixedLenByteArray(s) => s
            .min_opt()
            .map(|v| String::from_utf8_lossy(v.data()).to_string()),
    }
}

fn format_stat_max(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::Boolean(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int32(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int64(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int96(s) => s.max_opt().map(|v| format!("{:?}", v)),
        Statistics::Float(s) => s.max_opt().map(|v| format!("{:.2}", v)),
        Statistics::Double(s) => s.max_opt().map(|v| format!("{:.2}", v)),
        Statistics::ByteArray(s) => s
            .max_opt()
            .map(|v| String::from_utf8_lossy(v.data()).to_string()),
        Statistics::FixedLenByteArray(s) => s
            .max_opt()
            .map(|v| String::from_utf8_lossy(v.data()).to_string()),
    }
}

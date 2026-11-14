//! # Layer 2: Apache Parquet — Columnar Storage, Statistics, Pushdown & MinIO Object Storage
//!
//! Run with:
//! ```bash
//! cargo run --example 02_parquet
//! ```
//!
//! This example explores Apache Parquet backed by local **MinIO** S3 object storage:
//! 1. Streaming Arrow `RecordBatch`es into a compressed Parquet file on MinIO (`ParquetObjectWriter`)
//!    with ZSTD compression, bloom filters, and multiple row groups
//! 2. Fetching only the Parquet footer from MinIO via HTTP Range requests (`ParquetObjectReader`)
//!    to inspect row groups, column chunk compression ratios, and min/max statistics
//! 3. Reading from MinIO with **column projection pushdown**, **row group pruning**, and
//!    **row-level predicate pushdown** (`RowFilter` / `ArrowPredicateFn`)

use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use fdap::config::FdapConfig;
use fdap::dataset::generate_telemetry_batches;
use fdap::parquet_io::{
    inspect_parquet_on_minio, read_parquet_from_minio_with_pushdown, write_parquet_to_minio,
};
use fdap::storage::MinioStorage;

#[tokio::main]
async fn main() -> Result<()> {
    println!("================================================================================");
    println!("  FDAP Layer 2: Apache Parquet on Local MinIO Object Storage");
    println!("================================================================================\n");

    let config = FdapConfig::from_env();
    let storage = MinioStorage::connect(config.clone()).await?;
    let object_key = "examples/telemetry_demo.parquet";

    println!(
        "[1] Connected to MinIO at {} (bucket: s3://{})\n",
        config.minio_endpoint, config.bucket
    );

    // -------------------------------------------------------------------------
    // 1. Generate Arrow RecordBatches & Write to MinIO as Multi-Row-Group Parquet
    // -------------------------------------------------------------------------
    let batches = generate_telemetry_batches(3, 50)?;
    let raw_arrow_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

    println!(
        "[2] Writing {} RecordBatches ({} total rows, {} bytes in Arrow memory) to s3://{}/{}...",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        raw_arrow_bytes,
        config.bucket,
        object_key
    );

    // Set max_row_group_size = 50 so 150 rows produce 3 distinct row groups
    let write_summary = write_parquet_to_minio(&storage, object_key, &batches, 50).await?;
    println!(
        "  - Wrote Parquet object: {} rows across {} row groups -> {} bytes on MinIO ({:.1}% of in-memory size)",
        write_summary.num_rows,
        write_summary.num_row_groups,
        write_summary.object_size_bytes,
        (write_summary.object_size_bytes as f64 / raw_arrow_bytes as f64) * 100.0
    );

    // -------------------------------------------------------------------------
    // 2. Inspect Parquet Footer Metadata & Column Statistics via HTTP Range Read
    // -------------------------------------------------------------------------
    println!("\n[3] Inspecting Parquet Footer on MinIO via Async Range Reader...");
    let report = inspect_parquet_on_minio(&storage, object_key).await?;
    println!(
        "  - Object: s3://{}/{} ({} bytes, created_by={:?})",
        config.bucket, report.object_key, report.object_size_bytes, report.created_by
    );
    for rg in &report.row_groups {
        println!(
            "  - RowGroup #{}: {} rows, uncompressed={} B, compressed={} B",
            rg.index, rg.num_rows, rg.total_byte_size, rg.compressed_size
        );
        for col in rg.columns.iter().take(5) {
            println!(
                "      col `{:<12}` | {:<10} | {:>4} B -> {:>4} B | min={:?}, max={:?}",
                col.column_path,
                col.compression,
                col.uncompressed_bytes,
                col.compressed_bytes,
                col.min_value,
                col.max_value
            );
        }
    }

    // -------------------------------------------------------------------------
    // 3. Projection Pushdown + Row Group Pruning + RowFilter Predicate Pushdown
    // -------------------------------------------------------------------------
    let projected_cols = ["event_id", "device_id", "region", "service", "latency_ms"];
    let min_latency = 380.0;
    println!(
        "\n[4] Reading Parquet from MinIO with Projection {:?} & Predicate `latency_ms >= {:.1}`...",
        projected_cols, min_latency
    );

    let (filtered_batches, kept_rgs, total_rgs) = read_parquet_from_minio_with_pushdown(
        &storage,
        object_key,
        Some(&projected_cols),
        Some(min_latency),
    )
    .await?;

    let matched_rows: usize = filtered_batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "  - Row groups scanned after statistics pruning: {} / {}",
        kept_rgs, total_rgs
    );
    println!(
        "  - Rows returned after RowFilter pushdown: {} rows (with only {}/10 columns projected)\n{}",
        matched_rows,
        projected_cols.len(),
        pretty_format_batches(&filtered_batches)?
    );

    Ok(())
}

//! # Complete FDAP Stack: Functional Query Engine Backed by Parquet on MinIO
//!
//! Run with:
//! ```bash
//! cargo run --example 05_fdap_query_engine
//! ```
//!
//! Architecture:
//! ```text
//!   [ Arrow Flight Client ]
//!         │  ▲
//!         │  │  Flight gRPC (DoPut / GetFlightInfo / DoGet / DoAction)
//!         ▼  │  Zero-copy Arrow IPC RecordBatch streams
//!   [ Arrow Flight Server (FdapFlightService) ]
//!         │
//!         ▼
//!   [ Apache DataFusion Query Engine (FdapEngine) ]
//!     - SQL Planner, Cost Optimizer, Custom Vectorized UDFs
//!     - ListingTable Catalog + Predicate/Projection/Limit Pushdown
//!         │
//!         ▼  Async S3 HTTP Range Reads & Multipart Parquet Writes
//!   [ Apache Parquet on MinIO Object Storage (s3://fdap-warehouse/tables/*) ]
//! ```

use std::sync::Arc;

use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use fdap::config::FdapConfig;
use fdap::dataset::generate_telemetry_batches;
use fdap::engine::FdapEngine;
use fdap::flight::{FdapFlightClient, spawn_flight_server};

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<()> {
    run_full_fdap_showcase().await
}

pub async fn run_full_fdap_showcase() -> Result<()> {
    println!("================================================================================");
    println!("  FDAP Unified Query Engine: Flight + DataFusion + Arrow + Parquet on MinIO");
    println!("================================================================================\n");

    let config = FdapConfig::from_env();
    println!(
        "[1] Initializing FDAP Query Engine (MinIO: {}, Bucket: s3://{})...",
        config.minio_endpoint, config.bucket
    );
    let engine = Arc::new(FdapEngine::new(config.clone()).await?);

    let (flight_addr, server_handle) = spawn_flight_server(engine.clone(), "127.0.0.1:0").await?;
    let flight_endpoint = format!("http://{}", flight_addr);
    println!(
        "[2] FDAP Arrow Flight gRPC Server listening at {}\n",
        flight_endpoint
    );

    let mut client = FdapFlightClient::connect(&flight_endpoint).await?;

    // -------------------------------------------------------------------------
    // Step 1: Seed initial Parquet warehouse in MinIO via Flight Action
    // -------------------------------------------------------------------------
    println!("[3] Seeding MinIO Parquet Warehouse via Flight Action `seed_warehouse`...");
    let seeded = client.seed_warehouse().await?;
    for item in &seeded {
        println!(
            "  - Wrote s3://{}/{} | {} rows | {} row groups | {} bytes",
            item.bucket,
            item.object_key,
            item.num_rows,
            item.num_row_groups,
            item.object_size_bytes
        );
    }

    // -------------------------------------------------------------------------
    // Step 2: Stream Live Arrow RecordBatches via Flight `DoPut` into MinIO
    // -------------------------------------------------------------------------
    println!(
        "\n[4] Ingesting Live Arrow RecordBatches into `telemetry` via Flight `DoPut`..."
    );
    let live_batches = generate_telemetry_batches(1, 50)?;
    let ingested = client.put_batches("telemetry", live_batches).await?;
    println!(
        "  - Appended new Parquet partition via `DoPut`: s3://{}/{} ({} rows, {} bytes)",
        ingested.bucket, ingested.object_key, ingested.num_rows, ingested.object_size_bytes
    );

    // -------------------------------------------------------------------------
    // Step 3: Inspect MinIO Catalog & Table Statistics via Flight
    // -------------------------------------------------------------------------
    println!("\n[5] Inspecting MinIO Warehouse Catalog (`list_tables` Flight Action)...");
    let tables = client.list_tables().await?;
    for tbl in &tables {
        println!(
            "  - Table `{:<10}` | URI: {:<38} | Parquet files: {} | Size: {:>6} B | Rows: {}",
            tbl.table_name, tbl.s3_uri, tbl.parquet_files, tbl.total_bytes, tbl.row_count
        );
    }

    // -------------------------------------------------------------------------
    // Step 4: Execute Analytical SQL Queries over Arrow Flight
    // -------------------------------------------------------------------------
    println!(
        "\n[6] Query #1 over Flight: Regional Fleet Health & Custom Vectorized UDFs (`saturation_score`, `sla_tier`)..."
    );
    let q1 = r#"
        SELECT
            t.region,
            t.service,
            COUNT(*) AS events,
            ROUND(AVG(t.cpu_pct), 1) AS avg_cpu_pct,
            ROUND(AVG(saturation_score(t.cpu_pct, t.memory_mb)), 1) AS avg_saturation,
            ROUND(MAX(t.latency_ms), 1) AS max_latency_ms,
            sla_tier(MAX(t.latency_ms), BOOL_OR(t.is_error)) AS region_sla_status
        FROM telemetry t
        GROUP BY t.region, t.service
        ORDER BY avg_saturation DESC
    "#;
    let q1_batches = client.execute_sql(q1).await?;
    println!("{}", pretty_format_batches(&q1_batches)?);

    println!(
        "\n[7] Query #2 over Flight: 3-Way Join (`telemetry` + `devices` + `incidents`) for Incident Impact Analysis..."
    );
    let q2 = r#"
        SELECT
            i.incident_id,
            i.severity,
            d.device_id,
            d.instance_type,
            d.tier,
            i.duration_min,
            COUNT(t.event_id) AS telemetry_samples,
            SUM(CASE WHEN t.is_error THEN 1 ELSE 0 END) AS error_count,
            ROUND(MAX(t.latency_ms), 2) AS max_latency_ms,
            i.summary
        FROM incidents i
        JOIN devices d ON i.device_id = d.device_id
        JOIN telemetry t ON d.device_id = t.device_id
        GROUP BY
            i.incident_id, i.severity, d.device_id, d.instance_type, d.tier, i.duration_min, i.summary
        ORDER BY i.severity ASC, error_count DESC
    "#;
    let q2_batches = client.execute_sql(q2).await?;
    println!("{}", pretty_format_batches(&q2_batches)?);

    println!(
        "\n[8] Query #3 over Flight: Window Function (Top Latency Outlier per Region)..."
    );
    let q3 = r#"
        WITH ranked AS (
            SELECT
                region,
                device_id,
                service,
                event_id,
                latency_ms,
                status_code,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY latency_ms DESC, event_id ASC) AS rn
            FROM telemetry
        )
        SELECT region, device_id, service, event_id, latency_ms, status_code
        FROM ranked
        WHERE rn = 1
        ORDER BY latency_ms DESC
    "#;
    let q3_batches = client.execute_sql(q3).await?;
    println!("{}", pretty_format_batches(&q3_batches)?);

    // -------------------------------------------------------------------------
    // Step 5: Verify Physical Execution Plan & MinIO Parquet Pushdown via Flight
    // -------------------------------------------------------------------------
    println!("\n[9] Verifying Physical Execution Plan & Parquet Pushdown via Flight `explain`...");
    let explain_report = client
        .explain(
            "SELECT device_id, latency_ms FROM telemetry WHERE latency_ms > 300.0 AND is_error = true",
        )
        .await?;
    println!("Physical Plan:\n{}", explain_report.physical_plan);

    server_handle.abort();
    Ok(())
}

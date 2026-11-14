//! # Layer 3: Apache DataFusion — Vectorized SQL, DataFrame API, UDFs & MinIO ListingTables
//!
//! Run with:
//! ```bash
//! cargo run --example 03_datafusion
//! ```
//!
//! This example explores Apache DataFusion querying Parquet tables on local **MinIO**:
//! 1. Registering MinIO (`s3://fdap-warehouse`) with DataFusion and discovering Parquet `ListingTable`s
//! 2. Composing programmatic pipelines with the DataFusion **DataFrame API**
//! 3. Running analytical **SQL** (CTEs, multi-table JOINs, window functions, and custom vectorized UDFs
//!    `sla_tier()` and `saturation_score()`)
//! 4. Inspecting the **Logical, Optimized, and Physical Execution Plans** to verify predicate and
//!    projection pushdown into the MinIO Parquet reader

use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use datafusion::functions_aggregate::expr_fn::{avg, count, max};
use datafusion::prelude::*;
use fdap::config::FdapConfig;
use fdap::engine::FdapEngine;

#[tokio::main]
async fn main() -> Result<()> {
    println!("================================================================================");
    println!("  FDAP Layer 3: Apache DataFusion (SQL, DataFrame API, UDFs & Plan Pushdown)");
    println!("================================================================================\n");

    let config = FdapConfig::from_env();
    let engine = FdapEngine::new(config).await?;
    let seeded = engine.seed_default_tables().await?;

    println!("[1] Seeded MinIO Parquet Warehouse Tables:");
    for s in &seeded {
        println!(
            "  - s3://{}/{} ({} rows, {} row groups, {} bytes)",
            s.bucket, s.object_key, s.num_rows, s.num_row_groups, s.object_size_bytes
        );
    }

    // -------------------------------------------------------------------------
    // 2. DataFusion DataFrame API
    // -------------------------------------------------------------------------
    println!("\n[2] Querying MinIO Parquet Table via DataFusion DataFrame API...");
    let ctx = engine.session_context();
    let df = ctx
        .table("telemetry")
        .await?
        .filter(col("cpu_pct").gt(lit(50.0)))?
        .aggregate(
            vec![col("service")],
            vec![
                count(col("event_id")).alias("high_cpu_events"),
                avg(col("cpu_pct")).alias("avg_cpu_pct"),
                max(col("latency_ms")).alias("max_latency_ms"),
            ],
        )?
        .sort(vec![col("max_latency_ms").sort(false, true)])?;

    let df_batches = df.collect().await?;
    println!("{}", pretty_format_batches(&df_batches)?);

    // -------------------------------------------------------------------------
    // 3. Analytical SQL with Multi-Table JOINs, Window Functions & Custom UDFs
    // -------------------------------------------------------------------------
    println!(
        "\n[3] Executing Analytical SQL (JOIN `telemetry` + `devices` + `incidents` + Custom UDFs)..."
    );
    let sql = r#"
        SELECT
            t.device_id,
            d.tier,
            d.instance_type,
            COUNT(*) AS total_events,
            SUM(CASE WHEN t.is_error THEN 1 ELSE 0 END) AS error_events,
            ROUND(AVG(saturation_score(t.cpu_pct, t.memory_mb)), 2) AS avg_saturation,
            ROUND(MAX(t.latency_ms), 2) AS peak_latency_ms,
            sla_tier(MAX(t.latency_ms), BOOL_OR(t.is_error)) AS worst_sla_tier,
            COALESCE(MAX(i.severity), 'NONE') AS active_incident
        FROM telemetry t
        JOIN devices d ON t.device_id = d.device_id
        LEFT JOIN incidents i ON t.device_id = i.device_id
        GROUP BY t.device_id, d.tier, d.instance_type
        ORDER BY avg_saturation DESC
    "#;

    let sql_batches = engine.query(sql).await?;
    println!("{}", pretty_format_batches(&sql_batches)?);

    // -------------------------------------------------------------------------
    // 4. Inspect Query Optimizer & Physical Execution Plan (Pushdown Verification)
    // -------------------------------------------------------------------------
    println!("\n[4] Inspecting DataFusion Optimizer & Physical Execution Plan...");
    let pushdown_sql = r#"
        SELECT event_id, device_id, service, latency_ms
        FROM telemetry
        WHERE latency_ms > 200.0 AND is_error = true
        ORDER BY latency_ms DESC
        LIMIT 5
    "#;

    let explain = engine.explain(pushdown_sql).await?;
    println!("--- Unoptimized Logical Plan ---\n{}", explain.unoptimized_logical_plan);
    println!("--- Optimized Logical Plan ---\n{}", explain.optimized_logical_plan);
    println!("--- Physical Execution Plan ---\n{}", explain.physical_plan);

    let top_errors = engine.query(pushdown_sql).await?;
    println!(
        "--- Execution Result (Top 5 Error Spikes) ---\n{}",
        pretty_format_batches(&top_errors)?
    );

    Ok(())
}

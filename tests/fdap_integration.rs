use std::sync::Arc;

use anyhow::Result;
use arrow::array::Int64Array;
use fdap::config::FdapConfig;
use fdap::dataset::generate_telemetry_batches;
use fdap::engine::FdapEngine;
use fdap::flight::{FdapFlightClient, spawn_flight_server};
use fdap::parquet_io::{
    inspect_parquet_on_minio, read_parquet_from_minio_with_pushdown, write_parquet_to_minio,
};
use fdap::storage::MinioStorage;

#[tokio::test]
async fn test_full_fdap_stack_on_minio() -> Result<()> {
    let config = FdapConfig::from_env();
    let storage = MinioStorage::connect(config.clone()).await?;

    // 1. Test Layer 2: Parquet write, footer inspection, and pushdown on MinIO
    let test_key = "tests/parquet_layer_test.parquet";
    let batches = generate_telemetry_batches(2, 40)?;
    let write_res = write_parquet_to_minio(&storage, test_key, &batches, 40).await?;
    assert_eq!(write_res.num_rows, 80);
    assert_eq!(write_res.num_row_groups, 2);

    let meta = inspect_parquet_on_minio(&storage, test_key).await?;
    assert_eq!(meta.num_rows, 80);
    assert_eq!(meta.num_row_groups, 2);

    let (projected, kept_rgs, total_rgs) = read_parquet_from_minio_with_pushdown(
        &storage,
        test_key,
        Some(&["event_id", "latency_ms"]),
        Some(200.0),
    )
    .await?;
    assert_eq!(total_rgs, 2);
    assert!(kept_rgs >= 1);
    assert!(!projected.is_empty());
    assert_eq!(projected[0].num_columns(), 2);

    // 2. Test Layer 3 + 4: Unified FDAP Query Engine (Flight + DataFusion + Arrow + Parquet on MinIO)
    let engine = Arc::new(FdapEngine::new(config).await?);
    let (addr, server_handle) = spawn_flight_server(engine.clone(), "127.0.0.1:0").await?;
    let mut client = FdapFlightClient::connect(&format!("http://{}", addr)).await?;

    // Seed warehouse (400 telemetry rows + 8 devices + 5 incidents)
    let seeded = client.seed_warehouse().await?;
    assert_eq!(seeded.len(), 4);

    // Ingest 25 more telemetry rows via Flight DoPut
    let new_batch = generate_telemetry_batches(1, 25)?;
    let put_res = client.put_batches("telemetry", new_batch).await?;
    assert_eq!(put_res.num_rows, 25);

    // Verify table catalog reflects 425 rows across 3 Parquet files in `telemetry`
    let tables = client.list_tables().await?;
    let telemetry_tbl = tables
        .iter()
        .find(|t| t.table_name == "telemetry")
        .expect("telemetry table should be registered");
    assert_eq!(telemetry_tbl.row_count, 425);
    assert_eq!(telemetry_tbl.parquet_files, 3);

    // Execute SQL with JOIN and custom UDFs over Flight DoGet
    let query_batches = client
        .execute_sql(
            r#"
            SELECT
                COUNT(*) AS cnt,
                sla_tier(MAX(t.latency_ms), BOOL_OR(t.is_error)) AS tier
            FROM telemetry t
            JOIN devices d ON t.device_id = d.device_id
            "#,
        )
        .await?;
    assert_eq!(query_batches.len(), 1);
    let cnt = query_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 425);

    // Verify EXPLAIN plan contains Parquet scan
    let explain = client
        .explain("SELECT device_id FROM telemetry WHERE latency_ms > 250.0")
        .await?;
    assert!(
        explain.physical_plan.contains("Parquet")
            || explain.physical_plan.contains("DataSourceExec")
    );

    server_handle.abort();
    Ok(())
}

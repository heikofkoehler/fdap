//! # Layer 4: Apache Arrow Flight — High-Performance gRPC Columnar Wire Protocol
//!
//! Run with:
//! ```bash
//! cargo run --example 04_flight
//! ```
//!
//! This example explores the Apache Arrow Flight gRPC protocol:
//! 1. Starting an Arrow Flight gRPC server (`FlightServiceServer`) on a local port
//! 2. Discovering server actions via `ListActions` and executing `DoAction("ping")`
//! 3. Streaming Arrow `RecordBatch`es from client to server with `DoPut`
//! 4. Listing available Flight datasets and their schemas via `ListFlights`
//! 5. Planning a request with `GetFlightInfo` and streaming Arrow batches back via `DoGet`

use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use arrow_flight::{Action, Criteria, Empty, FlightDescriptor};
use bytes::Bytes;
use fdap::config::FdapConfig;
use fdap::engine::FdapEngine;
use fdap::flight::{FdapFlightClient, spawn_flight_server};
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    println!("================================================================================");
    println!("  FDAP Layer 4: Apache Arrow Flight (gRPC Columnar Streaming Protocol)");
    println!("================================================================================\n");

    let config = FdapConfig::from_env();
    let engine = Arc::new(FdapEngine::new(config).await?);

    // Bind on an ephemeral localhost port (127.0.0.1:0)
    let (addr, server_handle) = spawn_flight_server(engine.clone(), "127.0.0.1:0").await?;
    let endpoint = format!("http://{}", addr);
    println!("[1] Started Arrow Flight gRPC Server at {}\n", endpoint);

    let mut client = FdapFlightClient::connect(&endpoint).await?;

    // -------------------------------------------------------------------------
    // 1. ListActions & DoAction("ping")
    // -------------------------------------------------------------------------
    println!("[2] Querying Flight Server Actions (`ListActions` & `DoAction`)...");
    let actions: Vec<_> = client
        .inner_mut()
        .list_actions(Empty {})
        .await?
        .into_inner()
        .try_collect()
        .await?;
    for act in &actions {
        println!("  - Action `{:<16}`: {}", act.r#type, act.description);
    }

    let mut ping_stream = client
        .inner_mut()
        .do_action(Action::new("ping", Bytes::new()))
        .await?
        .into_inner();
    if let Some(res) = ping_stream.message().await? {
        println!(
            "  - DoAction(\"ping\") response: {}\n",
            String::from_utf8_lossy(&res.body)
        );
    }

    // -------------------------------------------------------------------------
    // 2. Stream RecordBatches to the Server via `DoPut`
    // -------------------------------------------------------------------------
    println!("[3] Streaming Custom Arrow RecordBatches to Server via `DoPut`...");
    let benchmark_schema = Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Int64, false),
        Field::new("transport", DataType::Utf8, false),
        Field::new("throughput_gbps", DataType::Float64, false),
        Field::new("cpu_overhead_pct", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        benchmark_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "REST JSON over HTTP/1.1",
                "gRPC Protobuf Row-by-Row",
                "JDBC / ODBC Row Cursor",
                "Apache Arrow Flight (IPC over gRPC)",
            ])),
            Arc::new(Float64Array::from(vec![0.42, 1.85, 1.10, 18.75])),
            Arc::new(Float64Array::from(vec![78.5, 46.2, 54.0, 6.8])),
        ],
    )?;

    // Clean any previous `flight_benchmarks` table and stream the new batch via DoPut
    engine
        .storage()
        .delete_prefix("tables/flight_benchmarks/")
        .await?;
    let put_summary = client.put_batches("flight_benchmarks", vec![batch]).await?;
    println!(
        "  - Server persisted `DoPut` stream to s3://{}/{} ({} rows, {} bytes)\n",
        put_summary.bucket,
        put_summary.object_key,
        put_summary.num_rows,
        put_summary.object_size_bytes
    );

    // -------------------------------------------------------------------------
    // 3. Discover Registered Datasets via `ListFlights`
    // -------------------------------------------------------------------------
    println!("[4] Discovering Available Datasets via `ListFlights`...");
    let flights: Vec<_> = client
        .inner_mut()
        .list_flights(Criteria::default())
        .await?
        .into_inner()
        .try_collect()
        .await?;
    for f in &flights {
        let path = f
            .flight_descriptor
            .as_ref()
            .map(|d| d.path.join("/"))
            .unwrap_or_default();
        println!(
            "  - Flight `{:<18}` | total_records={} | total_bytes={} B",
            path, f.total_records, f.total_bytes
        );
    }

    // -------------------------------------------------------------------------
    // 4. Plan & Fetch Results via `GetFlightInfo` + `DoGet`
    // -------------------------------------------------------------------------
    println!("\n[5] Planning & Streaming Query Results via `GetFlightInfo` -> `DoGet`...");
    let sql = "SELECT run_id, transport, throughput_gbps, cpu_overhead_pct FROM flight_benchmarks ORDER BY throughput_gbps DESC";
    let descriptor = FlightDescriptor::new_cmd(sql.as_bytes().to_vec());
    let flight_info = client
        .inner_mut()
        .get_flight_info(descriptor)
        .await?
        .into_inner();
    let decoded_schema = flight_info.clone().try_decode_schema()?;
    println!(
        "  - `GetFlightInfo` returned schema with {} fields: {:?}",
        decoded_schema.fields().len(),
        decoded_schema
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );

    let result_batches = client.execute_sql(sql).await?;
    println!(
        "  - `DoGet` streamed {} batch(es):\n{}",
        result_batches.len(),
        pretty_format_batches(&result_batches)?
    );

    server_handle.abort();
    Ok(())
}

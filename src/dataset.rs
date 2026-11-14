use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

/// Schema for the `telemetry` fact table.
pub fn telemetry_schema() -> SchemaRef {
    let mut metadata = HashMap::new();
    metadata.insert("dataset".to_string(), "cloud_telemetry".to_string());
    metadata.insert("layer".to_string(), "fdap_warehouse".to_string());

    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new(
                "timestamp_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("device_id", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("service", DataType::Utf8, false),
            Field::new("cpu_pct", DataType::Float64, false),
            Field::new("memory_mb", DataType::Float64, false),
            Field::new("latency_ms", DataType::Float64, false),
            Field::new("status_code", DataType::Int32, false),
            Field::new("is_error", DataType::Boolean, false),
        ],
        metadata,
    ))
}

/// Schema for the `devices` dimension table.
pub fn devices_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("device_id", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("instance_type", DataType::Utf8, false),
        Field::new("vcpu_count", DataType::Int32, false),
        Field::new("ram_gb", DataType::Int32, false),
        Field::new("tier", DataType::Utf8, false),
    ]))
}

/// Schema for the `incidents` operational table.
pub fn incidents_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("incident_id", DataType::Utf8, false),
        Field::new("device_id", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("summary", DataType::Utf8, false),
        Field::new("duration_min", DataType::Int32, false),
    ]))
}

/// Generates multiple `RecordBatch`es of synthetic telemetry events suitable for demonstrating
/// Parquet row groups, statistics pruning, DataFusion joins/aggregations, and Flight streaming.
pub fn generate_telemetry_batches(num_batches: usize, rows_per_batch: usize) -> Result<Vec<RecordBatch>> {
    let schema = telemetry_schema();
    let devices = [
        ("dev-us-east-01", "us-east-1", "query-router"),
        ("dev-us-east-02", "us-east-1", "storage-node"),
        ("dev-us-west-01", "us-west-2", "ingest-gateway"),
        ("dev-us-west-02", "us-west-2", "storage-node"),
        ("dev-eu-central-01", "eu-central-1", "query-router"),
        ("dev-eu-central-02", "eu-central-1", "compactor"),
        ("dev-ap-south-01", "ap-southeast-1", "ingest-gateway"),
        ("dev-ap-south-02", "ap-southeast-1", "storage-node"),
    ];

    let base_ts: i64 = 1_763_000_000_000; // Fixed deterministic epoch ms
    let mut batches = Vec::with_capacity(num_batches);

    for batch_idx in 0..num_batches {
        let mut event_ids = Vec::with_capacity(rows_per_batch);
        let mut timestamps = Vec::with_capacity(rows_per_batch);
        let mut device_ids = Vec::with_capacity(rows_per_batch);
        let mut regions = Vec::with_capacity(rows_per_batch);
        let mut services = Vec::with_capacity(rows_per_batch);
        let mut cpu_pcts = Vec::with_capacity(rows_per_batch);
        let mut memory_mbs = Vec::with_capacity(rows_per_batch);
        let mut latency_ms = Vec::with_capacity(rows_per_batch);
        let mut status_codes = Vec::with_capacity(rows_per_batch);
        let mut is_errors = Vec::with_capacity(rows_per_batch);

        for row_idx in 0..rows_per_batch {
            let global_idx = (batch_idx * rows_per_batch + row_idx) as i64;
            let (dev_id, region, service) = devices[(global_idx as usize) % devices.len()];

            // Deterministic but realistic telemetry signal with periodic spikes
            let wave = ((global_idx % 37) as f64) / 37.0;
            let is_spike = global_idx % 29 == 0;
            let cpu = 22.5 + wave * 55.0 + if is_spike { 19.5 } else { 0.0 };
            let mem = 2048.0 + ((global_idx % 16) as f64) * 512.0 + (batch_idx as f64) * 128.0;
            let lat = if is_spike {
                185.0 + ((global_idx % 13) as f64) * 18.5
            } else {
                4.2 + ((global_idx % 19) as f64) * 3.1
            };
            let status = if is_spike && (global_idx % 58 == 0) {
                503
            } else if is_spike {
                429
            } else {
                200
            };
            let err = status >= 400;

            event_ids.push(10_000 + global_idx);
            timestamps.push(base_ts + global_idx * 1_000);
            device_ids.push(dev_id);
            regions.push(region);
            services.push(service);
            cpu_pcts.push((cpu * 100.0).round() / 100.0);
            memory_mbs.push(mem);
            latency_ms.push((lat * 100.0).round() / 100.0);
            status_codes.push(status);
            is_errors.push(err);
        }

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(event_ids)),
            Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(device_ids)),
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(services)),
            Arc::new(Float64Array::from(cpu_pcts)),
            Arc::new(Float64Array::from(memory_mbs)),
            Arc::new(Float64Array::from(latency_ms)),
            Arc::new(Int32Array::from(status_codes)),
            Arc::new(BooleanArray::from(is_errors)),
        ];

        batches.push(RecordBatch::try_new(schema.clone(), columns)?);
    }

    Ok(batches)
}

/// Generates the `devices` dimension table `RecordBatch`.
pub fn generate_devices_batch() -> Result<RecordBatch> {
    let schema = devices_schema();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            "dev-us-east-01",
            "dev-us-east-02",
            "dev-us-west-01",
            "dev-us-west-02",
            "dev-eu-central-01",
            "dev-eu-central-02",
            "dev-ap-south-01",
            "dev-ap-south-02",
        ])),
        Arc::new(StringArray::from(vec![
            "us-east-1",
            "us-east-1",
            "us-west-2",
            "us-west-2",
            "eu-central-1",
            "eu-central-1",
            "ap-southeast-1",
            "ap-southeast-1",
        ])),
        Arc::new(StringArray::from(vec![
            "c7g.4xlarge",
            "i4i.8xlarge",
            "c7g.2xlarge",
            "i4i.8xlarge",
            "c7g.4xlarge",
            "r7g.4xlarge",
            "c7g.2xlarge",
            "i4i.4xlarge",
        ])),
        Arc::new(Int32Array::from(vec![16, 32, 8, 32, 16, 16, 8, 16])),
        Arc::new(Int32Array::from(vec![32, 256, 16, 256, 32, 128, 16, 128])),
        Arc::new(StringArray::from(vec![
            "mission-critical",
            "storage-tier-1",
            "edge-ingest",
            "storage-tier-1",
            "mission-critical",
            "background-compaction",
            "edge-ingest",
            "storage-tier-2",
        ])),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Generates the `incidents` operational table `RecordBatch`.
pub fn generate_incidents_batch() -> Result<RecordBatch> {
    let schema = incidents_schema();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            "INC-1001",
            "INC-1002",
            "INC-1003",
            "INC-1004",
            "INC-1005",
        ])),
        Arc::new(StringArray::from(vec![
            "dev-us-east-02",
            "dev-us-west-01",
            "dev-eu-central-02",
            "dev-ap-south-02",
            "dev-us-east-01",
        ])),
        Arc::new(StringArray::from(vec![
            "SEV-2",
            "SEV-3",
            "SEV-2",
            "SEV-3",
            "SEV-1",
        ])),
        Arc::new(StringArray::from(vec![
            "NVMe queue depth saturation during parquet compaction",
            "Ingress rate limit spike on TLS termination",
            "Memory pressure during multi-way hash join",
            "Cross-AZ replication tail latency above SLO",
            "Flight gRPC stream backpressure on slow consumer",
        ])),
        Arc::new(Int32Array::from(vec![18, 7, 32, 12, 45])),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

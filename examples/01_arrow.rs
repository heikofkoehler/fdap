//! # Layer 1: Apache Arrow — In-Memory Columnar Format & Vectorized Compute
//!
//! Run with:
//! ```bash
//! cargo run --example 01_arrow
//! ```
//!
//! This example explores the foundation of the FDAP stack:
//! 1. Constructing schemas & columnar `RecordBatch`es (primitives, dictionaries, nested Structs)
//! 2. Inspecting Arrow's physical memory buffers (validity bitmaps, offset buffers, value buffers)
//!    and performing zero-copy slicing (`RecordBatch::slice`)
//! 3. Executing SIMD-friendly vectorized compute kernels (`filter`, arithmetic, `sum`/`min`/`max`, `sort` + `take`)
//! 4. Serializing and deserializing `RecordBatch`es with the Arrow IPC wire format

use std::io::Cursor;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, StringArray,
    StringDictionaryBuilder, StructArray,
};
use arrow::compute::kernels::cmp::gt_eq;
use arrow::compute::kernels::numeric::sub;
use arrow::compute::{
    SortColumn, SortOptions, filter_record_batch, lexsort_to_indices, max, min, sum,
    take_record_batch,
};
use arrow::datatypes::{DataType, Field, Fields, Float64Type, Int32Type, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;

fn main() -> Result<()> {
    println!("================================================================================");
    println!("  FDAP Layer 1: Apache Arrow (Columnar Memory Layout, Kernels & IPC)");
    println!("================================================================================\n");

    // -------------------------------------------------------------------------
    // 1. Construct a Rich Arrow Schema & RecordBatch (including Dictionary & Struct)
    // -------------------------------------------------------------------------
    println!("[1] Building Columnar Arrays (Primitives, Dictionary<Int32, Utf8>, Nested Struct)...");

    let host_fields = Fields::from(vec![
        Field::new("rack", DataType::Utf8, false),
        Field::new("az", DataType::Utf8, false),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("device_id", DataType::Utf8, false),
        Field::new(
            "region",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("cpu_pct", DataType::Float64, true), // nullable to demonstrate validity bitmap
        Field::new("latency_ms", DataType::Float64, false),
        Field::new("sla_budget_ms", DataType::Float64, false),
        Field::new("placement", DataType::Struct(host_fields.clone()), false),
    ]));

    let event_ids = Int64Array::from(vec![1001, 1002, 1003, 1004, 1005, 1006]);
    let device_ids = StringArray::from(vec![
        "dev-us-east-01",
        "dev-us-east-02",
        "dev-us-west-01",
        "dev-eu-central-01",
        "dev-eu-central-02",
        "dev-ap-south-01",
    ]);

    let mut region_builder = StringDictionaryBuilder::<Int32Type>::new();
    for r in [
        "us-east-1",
        "us-east-1",
        "us-west-2",
        "eu-central-1",
        "eu-central-1",
        "ap-southeast-1",
    ] {
        region_builder.append(r)?;
    }
    let regions = region_builder.finish();

    // Nullable Float64Array (notice None at index 2)
    let cpu_pcts = Float64Array::from(vec![
        Some(42.5),
        Some(88.1),
        None,
        Some(94.7),
        Some(31.0),
        Some(76.4),
    ]);
    let latencies = Float64Array::from(vec![12.4, 145.0, 18.2, 230.5, 8.9, 112.0]);
    let budgets = Float64Array::from(vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0]);

    let rack_array: ArrayRef =
        Arc::new(StringArray::from(vec!["r-01", "r-02", "r-09", "r-04", "r-04", "r-12"]));
    let az_array: ArrayRef =
        Arc::new(StringArray::from(vec!["use1-az1", "use1-az2", "usw2-az1", "euc1-az1", "euc1-az3", "aps1-az2"]));
    let placement = StructArray::new(host_fields, vec![rack_array, az_array], None);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(event_ids),
            Arc::new(device_ids),
            Arc::new(regions),
            Arc::new(cpu_pcts),
            Arc::new(latencies),
            Arc::new(budgets),
            Arc::new(placement),
        ],
    )?;

    println!(
        "Created RecordBatch: {} rows x {} columns ({} bytes in memory)\n{}",
        batch.num_rows(),
        batch.num_columns(),
        batch.get_array_memory_size(),
        pretty_format_batches(std::slice::from_ref(&batch))?
    );

    // -------------------------------------------------------------------------
    // 2. Inspect Physical Buffers & Zero-Copy Slicing
    // -------------------------------------------------------------------------
    println!("\n[2] Inspecting Physical Arrow Buffers & Zero-Copy Slicing...");
    let cpu_col = batch.column_by_name("cpu_pct").unwrap();
    let cpu_data = cpu_col.to_data();
    println!(
        "  - `cpu_pct` column: len={}, null_count={}, buffers={}",
        cpu_col.len(),
        cpu_col.null_count(),
        cpu_data.buffers().len()
    );
    if let Some(nulls) = cpu_col.nulls() {
        let valid_indices: Vec<usize> = nulls.valid_indices().collect();
        println!("  - Validity bitmap valid row indices: {:?}", valid_indices);
    }

    // Zero-copy slice of rows [1..4]
    let sliced = batch.slice(1, 3);
    let orig_ptr = batch.column(0).to_data().buffers()[0].as_ptr();
    let slice_ptr = sliced.column(0).to_data().buffers()[0].as_ptr();
    println!(
        "  - Zero-copy slice(offset=1, len=3): shares backing buffer = {} (orig={:p}, slice={:p})",
        unsafe { orig_ptr.add(std::mem::size_of::<i64>()) == slice_ptr },
        orig_ptr,
        slice_ptr
    );

    // -------------------------------------------------------------------------
    // 3. Vectorized Compute Kernels (Filter, Arithmetic, Aggregations, Sort)
    // -------------------------------------------------------------------------
    println!("\n[3] Running Vectorized Compute Kernels (`arrow::compute`)...");
    let lat_col = batch
        .column_by_name("latency_ms")
        .unwrap()
        .as_primitive::<Float64Type>();
    let budget_col = batch
        .column_by_name("sla_budget_ms")
        .unwrap()
        .as_primitive::<Float64Type>();

    // Vectorized subtraction: headroom_ms = sla_budget_ms - latency_ms
    let headroom = sub(budget_col, lat_col)?;
    let headroom_f64 = headroom.as_primitive::<Float64Type>();
    println!(
        "  - Vectorized `sla_budget_ms - latency_ms` (headroom_ms): {:?}",
        headroom_f64.values()
    );

    // Vectorized aggregations on latency_ms
    println!(
        "  - Aggregations on `latency_ms`: min={:?} ms, max={:?} ms, sum={:?} ms",
        min(lat_col),
        max(lat_col),
        sum(lat_col)
    );

    // Vectorized boolean predicate filter: latency_ms >= sla_budget_ms (SLA breach)
    let breach_mask: BooleanArray = gt_eq(lat_col, budget_col)?;
    let breached_batch = filter_record_batch(&batch, &breach_mask)?;
    println!(
        "\n  - Filtered SLA Breaches (`latency_ms >= sla_budget_ms`, {} matching rows):\n{}",
        breached_batch.num_rows(),
        pretty_format_batches(&[breached_batch])?
    );

    // Multi-column sort by latency_ms DESC using lexsort_to_indices + take_record_batch
    let sort_indices = lexsort_to_indices(
        &[SortColumn {
            values: batch.column_by_name("latency_ms").unwrap().clone(),
            options: Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
        }],
        Some(3), // Top-3 highest latency events
    )?;
    let top3_batch = take_record_batch(&batch, &sort_indices)?;
    println!(
        "\n  - Top-3 Highest Latency Events (`lexsort_to_indices` + `take_record_batch`):\n{}",
        pretty_format_batches(&[top3_batch])?
    );

    // -------------------------------------------------------------------------
    // 4. Arrow IPC Stream Serialization & Deserialization
    // -------------------------------------------------------------------------
    println!("\n[4] Arrow IPC Stream Serialization & Zero-Copy Wire Round-Trip...");
    let mut ipc_buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buffer, &batch.schema())?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    println!(
        "  - Serialized RecordBatch into {} bytes of Arrow IPC stream format",
        ipc_buffer.len()
    );

    let reader = StreamReader::try_new(Cursor::new(ipc_buffer), None)?;
    let decoded_batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(decoded_batches.len(), 1);
    assert_eq!(decoded_batches[0].num_rows(), batch.num_rows());
    println!(
        "  - Successfully decoded {} batch ({} rows) from IPC stream!",
        decoded_batches.len(),
        decoded_batches[0].num_rows()
    );

    Ok(())
}

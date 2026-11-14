---
marp: true
theme: default
paginate: true
title: "The FDAP Stack in Rust: Flight, DataFusion, Arrow & Parquet on MinIO"
description: "Accompanying presentation for the heikofkoehler/fdap repository"
---

# The FDAP Stack in Rust
### Flight • DataFusion • Arrow • Parquet on Local MinIO Object Storage

**Accompanying Codebase:** [`heikofkoehler/fdap`](https://github.com/heikofkoehler/fdap)

- **A — Apache Arrow**: In-memory columnar format, zero-copy buffers & vectorized SIMD kernels
- **P — Apache Parquet**: Compressed columnar persistence, statistics, bloom filters & S3 range reads
- **D — Apache DataFusion**: Extensible SQL & DataFrame query engine with pushdown optimization
- **F — Apache Arrow Flight**: High-speed gRPC wire protocol streaming Arrow `RecordBatch`es

---

## 1. Why the FDAP Stack?

Traditional analytical engines suffer from **repeated serialization & format translation**:
- Storage format $\rightarrow$ Row structs $\rightarrow$ Execution engine $\rightarrow$ Protobuf/JSON $\rightarrow$ Client rows
- Up to **70–80% of CPU cycles** can be spent in serialization, parsing, and memory copying.

### The FDAP Composable Architecture
Every layer speaks **Apache Arrow** natively:
1. **Parquet on Object Storage (MinIO)** stores compressed columns and statistics on disk/S3.
2. **Arrow** is the universal in-memory columnar format across the entire pipeline.
3. **DataFusion** executes vectorized operators directly on Arrow arrays and pushes predicates into Parquet.
4. **Arrow Flight** streams Arrow IPC `RecordBatch`es over HTTP/2 gRPC with **zero row-conversion overhead**.

---

## 2. End-to-End Architecture (`heikofkoehler/fdap`)

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                        Client / CLI (`fdap-cli`)                           │
│   Streams RecordBatches (`DoPut`) & Executes SQL Queries (`DoGet`)         │
└───────────────────┬────────────────────────────────────▲───────────────────┘
                    │  Arrow Flight gRPC (HTTP/2 + IPC)  │
                    ▼                                    │
┌────────────────────────────────────────────────────────────────────────────┐
│               Layer 4: Apache Arrow Flight (`src/flight.rs`)               │
│   `GetFlightInfo` | `DoGet` | `DoPut` | `ListFlights` | `DoAction`         │
└───────────────────────────────────┬────────────────────────────────────────┘
                                    │  Zero-copy Arrow RecordBatch streams
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│              Layer 3: Apache DataFusion (`src/engine.rs`)                  │
│   • SQL Planner & DataFrame API                                            │
│   • Custom Vectorized Scalar UDFs (`sla_tier`, `saturation_score`)         │
│   • Optimizer: Projection, Filter, Statistics & Limit Pushdown             │
│   • `ListingTable` Catalog over `s3://fdap-warehouse/tables/<name>/`       │
└───────────────────────────────────┬────────────────────────────────────────┘
                                    │  Layer 1: Apache Arrow RecordBatches
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│         Layer 2: Apache Parquet + MinIO S3 (`src/parquet_io.rs`)           │
│   • `AsyncArrowWriter` + `ParquetObjectWriter` (ZSTD, Bloom Filters)       │
│   • `ParquetObjectReader` (Async HTTP Range Reads for Footers & Columns)   │
│   • Local MinIO Object Storage (`http://127.0.0.1:9000`)                   │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Layer 1: Apache Arrow (`examples/01_arrow.rs`)

**What it solves:** Cache-friendly, SIMD-aligned columnar memory layout with zero-copy sharing.

### Key Concepts Demonstrated in `examples/01_arrow.rs`
- **Rich Columnar Types**:
  - Primitives (`Int64Array`, `Float64Array`), nullable validity bitmaps (`NullBuffer`)
  - Dictionary-encoded strings (`StringDictionaryBuilder<Int32Type>`) for low-cardinality columns (`region`)
  - Nested types (`StructArray` for host `placement: { rack, az }`)
- **Zero-Copy Slicing (`RecordBatch::slice`)**:
  - `batch.slice(1, 3)` increments `Arc` reference counts and offsets pointers without copying array bytes.
- **Vectorized Compute Kernels (`arrow::compute`)**:
  - Arithmetic (`sub(budget_col, lat_col)`), boolean predicates (`gt_eq`), `filter_record_batch`
  - Vectorized aggregations (`min`, `max`, `sum`) and `lexsort_to_indices` + `take_record_batch`
- **Arrow IPC Wire Format**:
  - `StreamWriter` / `StreamReader` serializes `RecordBatch`es into framing ready for network or shared memory.

```bash
cargo run --example 01_arrow
```

---

## 4. Layer 1: Arrow Code Highlights (`examples/01_arrow.rs`)

```rust
// 1. Zero-copy slice of rows [1..4] — shares the exact same physical Buffer
let sliced = batch.slice(1, 3);

// 2. Vectorized subtraction: headroom_ms = sla_budget_ms - latency_ms
let headroom = sub(budget_col, lat_col)?;

// 3. Vectorized boolean mask filter: latency_ms >= sla_budget_ms
let breach_mask: BooleanArray = gt_eq(lat_col, budget_col)?;
let breached_batch = filter_record_batch(&batch, &breach_mask)?;

// 4. Top-K sort using lexsort_to_indices + take_record_batch
let sort_indices = lexsort_to_indices(
    &[SortColumn {
        values: batch.column_by_name("latency_ms").unwrap().clone(),
        options: Some(SortOptions { descending: true, nulls_first: false }),
    }],
    Some(3),
)?;
let top3_batch = take_record_batch(&batch, &sort_indices)?;
```

---

## 5. Layer 2: Apache Parquet on MinIO (`examples/02_parquet.rs`)

**What it solves:** High-ratio columnar compression and selective I/O over object storage (`s3://fdap-warehouse`).

### How Parquet Optimizes Object Storage Reads
1. **Footer-First HTTP Range Read**:
   - `ParquetObjectReader` issues a tiny byte-range `GET` at the tail of the object on MinIO to read the `ParquetMetaData` footer.
2. **Row Group Pruning via Column Statistics**:
   - Each Row Group stores `min`, `max`, and `null_count` per column chunk. Row groups where `max(latency_ms) < threshold` are skipped completely.
3. **Bloom Filters (`device_id`, `service`)**:
   - Configured via `WriterProperties::builder().set_column_bloom_filter_enabled(...)` for fast point-lookup pruning.
4. **Column Projection Pushdown (`ProjectionMask`)**:
   - Queries reading 5 of 10 columns only fetch those 5 byte ranges from MinIO.
5. **Late Materialization (`RowFilter` / `ArrowPredicateFn`)**:
   - Decodes only the filter column (`latency_ms`) first, builds a boolean selection mask, and decodes remaining projected columns only for matching rows.

```bash
cargo run --example 02_parquet
```

---

## 6. Layer 2: Async Parquet I/O on MinIO (`src/parquet_io.rs`)

```rust
// Writing Arrow RecordBatches directly to MinIO with ZSTD & Bloom Filters
let props = WriterProperties::builder()
    .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
    .set_statistics_enabled(EnabledStatistics::Page)
    .set_max_row_group_row_count(Some(max_row_group_size))
    .set_column_bloom_filter_enabled(ColumnPath::from("device_id"), true)
    .build();

let object_writer = ParquetObjectWriter::new(storage.as_object_store(), path);
let mut writer = AsyncArrowWriter::try_new(object_writer, schema, Some(props))?;
for batch in batches {
    writer.write(batch).await?;
}
let file_meta = writer.close().await?;
```

```rust
// Reading from MinIO with ProjectionMask + RowGroup Pruning + RowFilter
let reader = ParquetObjectReader::new(storage.as_object_store(), path).with_file_size(head.size);
let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
builder = builder
    .with_row_groups(selected_row_groups)
    .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
    .with_projection(ProjectionMask::roots(&parquet_schema, root_indices));
```

---

## 7. Layer 3: Apache DataFusion (`examples/03_datafusion.rs`)

**What it solves:** Turns Arrow + Parquet on MinIO into a full relational SQL & DataFrame query engine.

### Key Capabilities in `src/engine.rs` & `examples/03_datafusion.rs`
- **MinIO ObjectStore Registration**:
  ```rust
  ctx.runtime_env().register_object_store(&Url::parse("s3://fdap-warehouse")?, storage.as_object_store());
  ```
- **Multi-File `ListingTable` Catalog**:
  - Automatically infers Arrow schemas from Parquet footers in `s3://fdap-warehouse/tables/<table_name>/` and scans partitions in parallel.
- **Custom Vectorized Scalar UDFs**:
  - `sla_tier(latency_ms, is_error)` $\rightarrow$ classifies events into `'CRITICAL'`, `'DEGRADED'`, `'NORMAL'`, `'FAST'`
  - `saturation_score(cpu_pct, memory_mb)` $\rightarrow$ weighted `[0..100]` host saturation index
- **Cost-Based Optimizer & Pushdown**:
  - Pushes `WHERE latency_ms > 200.0 AND is_error = true` and column projections directly into `DataSourceExec` (`file_type=parquet`), synthesizing a `pruning_predicate` from column min/max statistics.

```bash
cargo run --example 03_datafusion
```

---

## 8. Layer 3: DataFusion Physical Plan Pushdown

When executing:
```sql
SELECT event_id, device_id, service, latency_ms
FROM telemetry
WHERE latency_ms > 200.0 AND is_error = true
ORDER BY latency_ms DESC
LIMIT 5
```

DataFusion optimizes the physical plan into:
```text
SortPreservingMergeExec: [latency_ms@3 DESC], fetch=5
  SortExec: TopK(fetch=5), expr=[latency_ms@3 DESC]
    FilterExec: latency_ms@3 > 200 AND is_error@4, projection=[event_id@0, device_id@1, service@2, latency_ms@3]
      RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
        DataSourceExec:
          file_groups={2 groups: [[tables/telemetry/part-1.parquet], [tables/telemetry/part-2.parquet]]},
          projection=[event_id, device_id, service, latency_ms, is_error],
          file_type=parquet,
          predicate=latency_ms@7 > 200 AND is_error@9,
          pruning_predicate=latency_ms_null_count@1 != row_count@2 AND latency_ms_max@0 > 200 AND (is_error_min@3 OR is_error_max@4)
```
- Only **5 of 10 columns** are read from MinIO.
- Both `predicate` and `pruning_predicate` are pushed into `DataSourceExec` (`parquet`).
- `TopK(fetch=5)` bounds memory usage during sorting.

---

## 9. Layer 4: Apache Arrow Flight (`examples/04_flight.rs`)

**What it solves:** High-speed network transport of Arrow `RecordBatch`es between processes and nodes.

### Flight RPC Lifecycle (`src/flight.rs`)
| Flight RPC Method | Role in the FDAP Engine |
| :--- | :--- |
| **`GetFlightInfo`** | Accepts a SQL query (`DescriptorType::Cmd`) or table path (`DescriptorType::Path`), plans the query in DataFusion, returns the output Arrow `Schema` + execution `Ticket`. |
| **`DoGet`** | Accepts the `Ticket`, executes the physical plan via `df.execute_stream()`, and streams Arrow IPC `FlightData` frames over HTTP/2 gRPC using `FlightDataEncoderBuilder`. |
| **`DoPut`** | Accepts a client stream of `FlightData`, decodes `RecordBatch`es via `FlightRecordBatchStream`, writes a new ZSTD Parquet partition to MinIO, and updates the DataFusion catalog. |
| **`ListFlights`** | Enumerates all MinIO-backed tables with their schemas, row counts, and compressed byte sizes. |
| **`DoAction`** | Administrative RPCs: `ping`, `list_tables`, `explain`, `seed_warehouse`, and `refresh_catalog`. |

```bash
cargo run --example 04_flight
```

---

## 10. Putting It All Together: The FDAP Query Engine (`examples/05_fdap_query_engine.rs`)

Combines **Flight + DataFusion + Arrow + Parquet on MinIO** into a live analytical warehouse:

1. **Storage Bootstrap (`src/storage.rs`)**:
   - Connects to local MinIO (`http://127.0.0.1:9000`), automatically starting a local `minio` server process and creating `s3://fdap-warehouse` via AWS SigV4 if needed.
2. **Warehouse Seeding (`DoAction("seed_warehouse")`)**:
   - Writes multi-partition Parquet tables to MinIO: `telemetry` (400 rows across 2 files / 4 row groups), `devices` (8 rows), and `incidents` (5 rows).
3. **Live Streaming Ingestion (`DoPut`)**:
   - Streams 50 new `telemetry` rows from the Flight client into `s3://fdap-warehouse/tables/telemetry/part-<uuid>.parquet` (total: 450 rows across 3 Parquet files).
4. **Analytical SQL over Flight (`GetFlightInfo` $\rightarrow$ `DoGet`)**:
   - **Query 1**: Regional fleet health & saturation using custom vectorized UDFs (`saturation_score`, `sla_tier`)
   - **Query 2**: 3-way JOIN (`incidents` $\bowtie$ `devices` $\bowtie$ `telemetry`) correlating infrastructure incidents with error spikes
   - **Query 3**: Window function (`ROW_NUMBER() OVER (PARTITION BY region ORDER BY latency_ms DESC)`) isolating the worst tail-latency outlier per region

---

## 11. Running the Engine, CLI & Tests

### Run Layer Examples & Full Demo
```bash
cargo run --example 01_arrow
cargo run --example 02_parquet
cargo run --example 03_datafusion
cargo run --example 04_flight
cargo run --example 05_fdap_query_engine
```

### Run Standalone Server & CLI Client
```bash
# Terminal 1: Start the FDAP Flight + DataFusion + MinIO server
cargo run --bin fdap-server

# Terminal 2: Inspect catalog, run SQL, explain plans, and ingest batches via Flight
cargo run --bin fdap-cli -- tables
cargo run --bin fdap-cli -- query "SELECT region, COUNT(*) AS events, MAX(latency_ms) AS max_lat FROM telemetry GROUP BY region"
cargo run --bin fdap-cli -- explain "SELECT device_id, latency_ms FROM telemetry WHERE latency_ms > 250.0"
cargo run --bin fdap-cli -- ingest --table telemetry --rows 100
```

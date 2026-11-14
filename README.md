# FDAP Stack Explorer & Query Engine in Rust

An end-to-end exploration and implementation of the **FDAP** modern analytical data stack in Rust:

- **F — [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)**: High-throughput, zero-serialization-overhead gRPC wire protocol for streaming Arrow `RecordBatch`es between clients and servers.
- **D — [Apache DataFusion](https://datafusion.apache.org/)**: Extensible, vectorized SQL and DataFrame query engine with cost-based optimization, custom UDFs, and physical pushdown.
- **A — [Apache Arrow](https://arrow.apache.org/)**: Standardized in-memory columnar format with validity bitmaps, dictionary encoding, nested types, zero-copy slicing, and SIMD compute kernels.
- **P — [Apache Parquet](https://parquet.apache.org/)**: Compressed columnar storage format with row groups, column chunk min/max statistics, bloom filters, and async S3 HTTP Range reads on **MinIO** object storage.

---

## Architecture

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

## Repository Structure

| Path | Description |
| :--- | :--- |
| [`examples/01_arrow.rs`](examples/01_arrow.rs) | **Layer 1 (Arrow)**: Columnar arrays (primitives, dictionaries, structs), physical buffer inspection, zero-copy slicing, vectorized compute kernels (`filter`, arithmetic, `sum`/`min`/`max`, `lexsort`), and Arrow IPC streaming. |
| [`examples/02_parquet.rs`](examples/02_parquet.rs) | **Layer 2 (Parquet + MinIO)**: Async Parquet writing to MinIO (`ParquetObjectWriter`) with ZSTD & bloom filters, remote footer metadata inspection via HTTP Range requests, row-group statistics pruning, projection pushdown, and `RowFilter` predicate pushdown. |
| [`examples/03_datafusion.rs`](examples/03_datafusion.rs) | **Layer 3 (DataFusion)**: Registering MinIO `s3://fdap-warehouse` object storage, querying Parquet `ListingTable`s via the DataFrame API and SQL (JOINs, aggregations, custom vectorized UDFs), and inspecting Logical/Optimized/Physical plans. |
| [`examples/04_flight.rs`](examples/04_flight.rs) | **Layer 4 (Flight)**: Implementing a `FlightService` gRPC server and client supporting `ListActions`, `DoAction`, `DoPut` (streaming ingestion), `ListFlights`, `GetFlightInfo`, and `DoGet` (streaming query results). |
| [`examples/05_fdap_query_engine.rs`](examples/05_fdap_query_engine.rs) | **Combined FDAP Query Engine**: End-to-end showcase uniting Flight + DataFusion + Arrow + Parquet on MinIO object storage. |
| [`src/storage.rs`](src/storage.rs) | MinIO S3 `ObjectStore` client, automatic local MinIO process supervisor, and AWS SigV4 bucket bootstrapper. |
| [`src/parquet_io.rs`](src/parquet_io.rs) | Async Parquet reader/writer on MinIO, metadata inspector, and row-group/predicate pushdown utilities. |
| [`src/engine.rs`](src/engine.rs) | `FdapEngine` managing DataFusion `SessionContext`, MinIO `ListingTable` catalog discovery, and vectorized UDFs. |
| [`src/flight.rs`](src/flight.rs) | `FdapFlightService` gRPC server and `FdapFlightClient` implementation. |
| [`src/bin/fdap-server.rs`](src/bin/fdap-server.rs) | Standalone FDAP Query Engine server listening on `127.0.0.1:50051`. |
| [`src/bin/fdap-cli.rs`](src/bin/fdap-cli.rs) | CLI client to list MinIO tables, run SQL queries, inspect plans, and stream Arrow batches into MinIO over Flight. |

---

## Quick Start

### 1. Start Local MinIO Object Storage

You can start MinIO using either the helper script (which uses a local `minio` binary if installed, or falls back to Docker Compose) or `docker compose` directly. Furthermore, `FdapEngine` automatically starts a local `minio` server process and creates the `fdap-warehouse` bucket if MinIO is not already running!

```bash
# Option A: Using the helper script
./scripts/minio.sh start

# Option B: Using Docker Compose
docker compose up -d
```

Default MinIO settings (configurable via environment variables):
- **S3 Endpoint (`FDAP_MINIO_ENDPOINT`)**: `http://127.0.0.1:9000`
- **Console UI**: `http://127.0.0.1:9001`
- **Credentials (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`)**: `minioadmin` / `minioadmin`
- **Warehouse Bucket (`FDAP_BUCKET`)**: `fdap-warehouse`

### 2. Run Each Layer Example

```bash
# Layer 1: Apache Arrow (In-memory arrays, buffers, zero-copy slicing, compute kernels, IPC)
cargo run --example 01_arrow

# Layer 2: Apache Parquet (ZSTD compression, bloom filters, footer stats & pushdown on MinIO)
cargo run --example 02_parquet

# Layer 3: Apache DataFusion (DataFrame API, SQL joins, custom UDFs & physical plan pushdown)
cargo run --example 03_datafusion

# Layer 4: Apache Arrow Flight (gRPC FlightService: DoPut, ListFlights, GetFlightInfo, DoGet)
cargo run --example 04_flight

# Full Stack: Combined FDAP Query Engine backed by Parquet on MinIO
cargo run --example 05_fdap_query_engine
```

### 3. Run the Standalone Server & CLI Client

```bash
# Terminal 1: Start the FDAP Query Engine server (seeds MinIO warehouse on startup)
cargo run --bin fdap-server

# Terminal 2: Query and manage the engine via Arrow Flight
cargo run --bin fdap-cli -- tables
cargo run --bin fdap-cli -- query "SELECT region, service, COUNT(*) AS events, ROUND(AVG(latency_ms), 2) AS avg_lat FROM telemetry GROUP BY region, service ORDER BY avg_lat DESC"
cargo run --bin fdap-cli -- explain "SELECT device_id, latency_ms FROM telemetry WHERE latency_ms > 250.0"
cargo run --bin fdap-cli -- ingest --table telemetry --rows 100
```

### 4. Run Automated Integration Tests

```bash
cargo test
```

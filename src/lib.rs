//! # FDAP Stack Explorer & Query Engine (`fdap`)
//!
//! This crate explores each layer of the **FDAP** modern analytical data stack in Rust:
//! - **A — Apache Arrow**: In-memory columnar format, zero-copy buffers, and SIMD/vectorized compute kernels.
//! - **P — Apache Parquet**: Compressed columnar persistence with row groups, statistics, bloom filters, and async S3 range reads.
//! - **D — Apache DataFusion**: Extensible vectorized SQL & DataFrame query engine with cost-based optimization and pushdown.
//! - **F — Apache Arrow Flight**: High-throughput, zero-serialization-overhead gRPC wire protocol for streaming Arrow `RecordBatch`es.
//!
//! Finally, it combines all four layers into a functional **FDAP Query Engine** backed by Parquet on **MinIO** object storage.

pub mod config;
pub mod dataset;
pub mod engine;
pub mod flight;
pub mod parquet_io;
pub mod storage;

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, AsArray, BooleanArray, Float64Array, StringBuilder};
use arrow::datatypes::{DataType, Float64Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::config::FdapConfig;
use crate::dataset::{
    generate_devices_batch, generate_incidents_batch, generate_telemetry_batches,
};
use crate::parquet_io::{ParquetWriteSummary, write_parquet_to_minio};
use crate::storage::MinioStorage;

/// Summary of a registered Parquet-backed table in the MinIO warehouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSummary {
    pub table_name: String,
    pub s3_uri: String,
    pub parquet_files: usize,
    pub total_bytes: u64,
    pub row_count: i64,
    pub columns: Vec<String>,
}

/// Logical, optimized, and physical plans produced by DataFusion for a SQL query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExplainReport {
    pub sql: String,
    pub unoptimized_logical_plan: String,
    pub optimized_logical_plan: String,
    pub physical_plan: String,
}

/// The unified FDAP Query Engine: DataFusion backed by Parquet datasets stored in MinIO.
#[derive(Clone)]
pub struct FdapEngine {
    storage: MinioStorage,
    ctx: SessionContext,
    tables: Arc<RwLock<BTreeSet<String>>>,
}

impl FdapEngine {
    /// Initializes the FDAP query engine, connects to MinIO, registers the S3 object store
    /// with DataFusion, registers custom vectorized UDFs, and discovers any existing tables.
    pub async fn new(config: FdapConfig) -> Result<Self> {
        let storage = MinioStorage::connect(config).await?;

        let session_cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_target_partitions(4)
            .with_parquet_pruning(true)
            .with_parquet_bloom_filter_pruning(true)
            .with_parquet_page_index_pruning(true);

        // Disable stale list_files_cache so new Parquet partitions written via Flight DoPut
        // are immediately visible on subsequent queries.
        let cache_cfg = CacheManagerConfig::default().with_list_files_cache_limit(0);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_cache_manager(cache_cfg)
            .build_arc()?;

        let ctx = SessionContext::new_with_config_rt(session_cfg, runtime_env);

        // Register the MinIO bucket S3 URL with DataFusion's RuntimeEnv
        let bucket_url = storage.bucket_url()?;
        ctx.runtime_env()
            .register_object_store(&bucket_url, storage.as_object_store());

        // Register custom vectorized UDFs
        register_fdap_udfs(&ctx);

        let engine = Self {
            storage,
            ctx,
            tables: Arc::new(RwLock::new(BTreeSet::new())),
        };

        engine.discover_tables().await?;
        Ok(engine)
    }

    /// Returns a reference to the underlying [`MinioStorage`].
    pub fn storage(&self) -> &MinioStorage {
        &self.storage
    }

    /// Returns a reference to the DataFusion [`SessionContext`].
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Writes Arrow [`RecordBatch`]es as a new Parquet file in MinIO under
    /// `s3://<bucket>/tables/<table_name>/part-<uuid>.parquet` and registers (or refreshes)
    /// the table in DataFusion.
    pub async fn ingest_batches(
        &self,
        table_name: &str,
        batches: &[RecordBatch],
        max_row_group_size: usize,
    ) -> Result<ParquetWriteSummary> {
        let object_key = format!("tables/{}/part-{}.parquet", table_name, Uuid::new_v4());
        let summary =
            write_parquet_to_minio(&self.storage, &object_key, batches, max_row_group_size).await?;
        self.register_parquet_table(table_name).await?;
        Ok(summary)
    }

    /// Replaces a table's Parquet objects on MinIO with the provided [`RecordBatch`]es.
    pub async fn overwrite_table(
        &self,
        table_name: &str,
        batches: &[RecordBatch],
        max_row_group_size: usize,
    ) -> Result<ParquetWriteSummary> {
        let prefix = format!("tables/{}/", table_name);
        self.storage.delete_prefix(&prefix).await?;
        self.ingest_batches(table_name, batches, max_row_group_size)
            .await
    }

    /// Registers (or re-registers) a directory of Parquet objects in MinIO
    /// (`s3://<bucket>/tables/<table_name>/`) as a DataFusion [`ListingTable`].
    pub async fn register_parquet_table(&self, table_name: &str) -> Result<SchemaRef> {
        let table_url = self.storage.config().table_url(table_name);
        let table_path = ListingTableUrl::parse(&table_url)
            .with_context(|| format!("Invalid ListingTableUrl: {}", table_url))?;

        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options =
            ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

        let resolved_schema = listing_options
            .infer_schema(&self.ctx.state(), &table_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to infer Parquet schema for table '{}' at {}",
                    table_name, table_url
                )
            })?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema.clone());

        let table = ListingTable::try_new(config)?;
        let _ = self.ctx.deregister_table(table_name);
        self.ctx.register_table(table_name, Arc::new(table))?;

        self.tables.write().await.insert(table_name.to_string());
        Ok(resolved_schema)
    }

    /// Scans `tables/` in the MinIO bucket and automatically registers every discovered
    /// Parquet table directory in DataFusion.
    pub async fn discover_tables(&self) -> Result<Vec<String>> {
        let objects = self.storage.list_objects(Some("tables/")).await?;
        let mut discovered = BTreeSet::new();

        for obj in objects {
            let path_str = obj.location.as_ref();
            if path_str.ends_with(".parquet") {
                let parts: Vec<&str> = path_str.split('/').collect();
                if parts.len() >= 3 && parts[0] == "tables" {
                    discovered.insert(parts[1].to_string());
                }
            }
        }

        let mut registered = Vec::new();
        for table_name in discovered {
            self.register_parquet_table(&table_name).await?;
            registered.push(table_name);
        }
        Ok(registered)
    }

    /// Seeds the MinIO warehouse with the three default FDAP tables (`telemetry`, `devices`,
    /// and `incidents`), writing multiple Parquet files and row groups for `telemetry`.
    pub async fn seed_default_tables(&self) -> Result<Vec<ParquetWriteSummary>> {
        self.storage.delete_prefix("tables/telemetry/").await?;
        self.storage.delete_prefix("tables/devices/").await?;
        self.storage.delete_prefix("tables/incidents/").await?;

        let mut summaries = Vec::new();

        // Write `telemetry` as 2 separate Parquet partition files on MinIO (200 rows each, 100 rows per row group)
        let telemetry_batches = generate_telemetry_batches(4, 100)?;
        let part1 = self
            .ingest_batches("telemetry", &telemetry_batches[0..2], 100)
            .await?;
        let part2 = self
            .ingest_batches("telemetry", &telemetry_batches[2..4], 100)
            .await?;
        summaries.push(part1);
        summaries.push(part2);

        // Write `devices` dimension table
        let devices_batch = generate_devices_batch()?;
        summaries.push(
            self.ingest_batches("devices", &[devices_batch], 100)
                .await?,
        );

        // Write `incidents` operational table
        let incidents_batch = generate_incidents_batch()?;
        summaries.push(
            self.ingest_batches("incidents", &[incidents_batch], 100)
                .await?,
        );

        Ok(summaries)
    }

    /// Executes a SQL query against the DataFusion context and collects the resulting Arrow [`RecordBatch`]es.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .with_context(|| format!("Failed to plan SQL query: {}", sql))?;
        df.collect()
            .await
            .with_context(|| format!("Failed to execute SQL query: {}", sql))
    }

    /// Produces the unoptimized logical plan, optimized logical plan, and physical execution plan
    /// for a SQL query (highlighting Parquet filter & projection pushdown).
    pub async fn explain(&self, sql: &str) -> Result<QueryExplainReport> {
        let df = self.ctx.sql(sql).await?;
        let unoptimized = df.logical_plan().display_indent().to_string();
        let (state, plan) = df.into_parts();
        let optimized = state.optimize(&plan)?;
        let optimized_str = optimized.display_indent().to_string();
        let physical = state.create_physical_plan(&optimized).await?;
        let physical_str =
            datafusion::physical_plan::displayable(physical.as_ref())
                .indent(true)
                .to_string();

        Ok(QueryExplainReport {
            sql: sql.to_string(),
            unoptimized_logical_plan: unoptimized,
            optimized_logical_plan: optimized_str,
            physical_plan: physical_str,
        })
    }

    /// Lists all registered tables with their MinIO Parquet file count, compressed byte size,
    /// row count, and column definitions.
    pub async fn table_summaries(&self) -> Result<Vec<TableSummary>> {
        let names: Vec<String> = self.tables.read().await.iter().cloned().collect();
        let mut summaries = Vec::with_capacity(names.len());

        for name in names {
            let prefix = format!("tables/{}/", name);
            let objects = self.storage.list_objects(Some(&prefix)).await?;
            let parquet_objs: Vec<_> = objects
                .into_iter()
                .filter(|o| o.location.as_ref().ends_with(".parquet"))
                .collect();
            let total_bytes: u64 = parquet_objs.iter().map(|o| o.size).sum();

            let count_batches = self
                .query(&format!("SELECT COUNT(*) AS cnt FROM {}", name))
                .await?;
            let row_count = count_batches
                .first()
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                })
                .map(|a| a.value(0))
                .unwrap_or(0);

            let table_provider = self.ctx.table_provider(&name).await?;
            let schema = table_provider.schema();
            let columns = schema
                .fields()
                .iter()
                .map(|f| format!("{}: {}", f.name(), f.data_type()))
                .collect();

            summaries.push(TableSummary {
                table_name: name.clone(),
                s3_uri: self.storage.config().table_url(&name),
                parquet_files: parquet_objs.len(),
                total_bytes,
                row_count,
                columns,
            });
        }

        Ok(summaries)
    }
}

/// Registers custom vectorized DataFusion Scalar UDFs:
/// - `sla_tier(latency_ms, is_error) -> Utf8`
/// - `saturation_score(cpu_pct, memory_mb) -> Float64`
pub fn register_fdap_udfs(ctx: &SessionContext) {
    // 1. `sla_tier(latency_ms: Float64, is_error: Boolean) -> Utf8`
    let sla_tier_fn = Arc::new(|args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
        let num_rows = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(a) => Some(a.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);

        let lat_array = args[0].clone().into_array(num_rows)?;
        let err_array = args[1].clone().into_array(num_rows)?;

        let latencies: &Float64Array = lat_array.as_primitive::<Float64Type>();
        let errors: &BooleanArray = err_array.as_boolean();

        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
        for i in 0..num_rows {
            if latencies.is_null(i) || errors.is_null(i) {
                builder.append_null();
            } else {
                let lat = latencies.value(i);
                let is_err = errors.value(i);
                let tier = if is_err || lat >= 250.0 {
                    "CRITICAL"
                } else if lat >= 100.0 {
                    "DEGRADED"
                } else if lat <= 15.0 {
                    "FAST"
                } else {
                    "NORMAL"
                };
                builder.append_value(tier);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    });

    let sla_tier_udf = create_udf(
        "sla_tier",
        vec![DataType::Float64, DataType::Boolean],
        DataType::Utf8,
        Volatility::Immutable,
        sla_tier_fn,
    );
    ctx.register_udf(sla_tier_udf);

    // 2. `saturation_score(cpu_pct: Float64, memory_mb: Float64) -> Float64`
    let saturation_fn =
        Arc::new(|args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
            let num_rows = args
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(a) => Some(a.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);

            let cpu_arr = args[0].clone().into_array(num_rows)?;
            let mem_arr = args[1].clone().into_array(num_rows)?;

            let cpus: &Float64Array = cpu_arr.as_primitive::<Float64Type>();
            let mems: &Float64Array = mem_arr.as_primitive::<Float64Type>();

            let out: Float64Array = (0..num_rows)
                .map(|i| {
                    if cpus.is_null(i) || mems.is_null(i) {
                        None
                    } else {
                        let cpu_norm = cpus.value(i).clamp(0.0, 100.0);
                        let mem_norm = ((mems.value(i) / 16384.0) * 100.0).clamp(0.0, 100.0);
                        let score = ((cpu_norm * 0.65 + mem_norm * 0.35) * 100.0).round() / 100.0;
                        Some(score)
                    }
                })
                .collect();

            Ok(ColumnarValue::Array(Arc::new(out)))
        });

    let saturation_udf = create_udf(
        "saturation_score",
        vec![DataType::Float64, DataType::Float64],
        DataType::Float64,
        Volatility::Immutable,
        saturation_fn,
    );
    ctx.register_udf(saturation_udf);
}

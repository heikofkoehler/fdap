use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use clap::{Parser, Subcommand};
use fdap::dataset::generate_telemetry_batches;
use fdap::flight::FdapFlightClient;

/// CLI client for the FDAP Query Engine over Apache Arrow Flight gRPC.
#[derive(Debug, Parser)]
#[command(name = "fdap-cli", about = "Query and manage the FDAP Query Engine via Arrow Flight")]
struct Cli {
    /// Arrow Flight server gRPC endpoint
    #[arg(long, env = "FDAP_FLIGHT_ADDR", default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// List all registered MinIO Parquet tables, sizes, row counts, and schemas
    Tables,
    /// Execute a SQL query over Arrow Flight and pretty-print the resulting RecordBatches
    Query {
        /// SQL query to execute
        sql: String,
    },
    /// Show the DataFusion Logical, Optimized, and Physical execution plans for a SQL query
    Explain {
        /// SQL query to explain
        sql: String,
    },
    /// Stream new synthetic telemetry RecordBatches into MinIO via Flight DoPut
    Ingest {
        /// Target table name
        #[arg(long, default_value = "telemetry")]
        table: String,
        /// Number of rows to generate and stream
        #[arg(long, default_value_t = 100)]
        rows: usize,
    },
    /// Seed or reset the default MinIO Parquet warehouse tables
    Seed,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut client = FdapFlightClient::connect(&cli.endpoint).await?;

    match cli.command {
        Commands::Tables => {
            let tables = client.list_tables().await?;
            for t in tables {
                println!(
                    "Table `{}` ({})\n  Parquet files: {} | Total bytes: {} B | Rows: {}\n  Columns: {}",
                    t.table_name,
                    t.s3_uri,
                    t.parquet_files,
                    t.total_bytes,
                    t.row_count,
                    t.columns.join(", ")
                );
            }
        }
        Commands::Query { sql } => {
            let batches = client.execute_sql(&sql).await?;
            println!("{}", pretty_format_batches(&batches)?);
        }
        Commands::Explain { sql } => {
            let report = client.explain(&sql).await?;
            println!("=== Unoptimized Logical Plan ===\n{}", report.unoptimized_logical_plan);
            println!("=== Optimized Logical Plan ===\n{}", report.optimized_logical_plan);
            println!("=== Physical Execution Plan ===\n{}", report.physical_plan);
        }
        Commands::Ingest { table, rows } => {
            let batches = generate_telemetry_batches(1, rows)?;
            let summary = client.put_batches(&table, batches).await?;
            println!(
                "Ingested {} rows into s3://{}/{} ({} bytes across {} row group(s))",
                summary.num_rows,
                summary.bucket,
                summary.object_key,
                summary.object_size_bytes,
                summary.num_row_groups
            );
        }
        Commands::Seed => {
            let seeded = client.seed_warehouse().await?;
            for s in seeded {
                println!(
                    "Seeded s3://{}/{} ({} rows, {} bytes)",
                    s.bucket, s.object_key, s.num_rows, s.object_size_bytes
                );
            }
        }
    }

    Ok(())
}

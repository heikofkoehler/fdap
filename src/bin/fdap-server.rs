use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use fdap::config::FdapConfig;
use fdap::engine::FdapEngine;
use fdap::flight::FdapFlightService;
use tonic::transport::Server;

/// Standalone FDAP Query Engine Server (Arrow Flight + DataFusion + Parquet on MinIO).
#[derive(Debug, Parser)]
#[command(name = "fdap-server", about = "FDAP Query Engine Arrow Flight gRPC Server")]
struct Args {
    /// Bind address for the Arrow Flight gRPC server
    #[arg(long, env = "FDAP_FLIGHT_ADDR", default_value = "127.0.0.1:50051")]
    bind: String,

    /// MinIO S3 endpoint URL
    #[arg(long, env = "FDAP_MINIO_ENDPOINT", default_value = "http://127.0.0.1:9000")]
    minio_endpoint: String,

    /// MinIO S3 bucket name
    #[arg(long, env = "FDAP_BUCKET", default_value = "fdap-warehouse")]
    bucket: String,

    /// Seed default Parquet tables (`telemetry`, `devices`, `incidents`) on startup
    #[arg(long, default_value_t = true)]
    seed: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let mut config = FdapConfig::from_env();
    config.flight_addr = args.bind.clone();
    config.minio_endpoint = args.minio_endpoint;
    config.bucket = args.bucket;

    println!(
        "Starting FDAP Engine (MinIO: {}, Bucket: s3://{})...",
        config.minio_endpoint, config.bucket
    );
    let engine = Arc::new(FdapEngine::new(config.clone()).await?);

    if args.seed {
        let summaries = engine.seed_default_tables().await?;
        println!("Seeded {} Parquet objects in MinIO:", summaries.len());
        for s in summaries {
            println!(
                "  - s3://{}/{} ({} rows, {} bytes)",
                s.bucket, s.object_key, s.num_rows, s.object_size_bytes
            );
        }
    }

    let addr = args.bind.parse()?;
    let service = FdapFlightService::new(engine);
    println!("FDAP Arrow Flight gRPC Server listening on http://{}", addr);

    Server::builder()
        .add_service(service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}

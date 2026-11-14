use std::env;
use std::path::PathBuf;

/// Configuration for the local MinIO object store and FDAP query engine.
#[derive(Debug, Clone)]
pub struct FdapConfig {
    /// MinIO / S3 HTTP endpoint (e.g. `http://127.0.0.1:9000`)
    pub minio_endpoint: String,
    /// Bucket name used as the Parquet data warehouse (`fdap-warehouse`)
    pub bucket: String,
    /// MinIO / S3 access key ID
    pub access_key: String,
    /// MinIO / S3 secret access key
    pub secret_key: String,
    /// AWS region name for S3 signing
    pub region: String,
    /// Local data directory when spawning a local `minio` server process
    pub minio_data_dir: PathBuf,
    /// Whether to automatically spawn a local `minio` binary if the endpoint is unreachable
    pub auto_start_minio: bool,
    /// Address for the Arrow Flight gRPC server (`127.0.0.1:50051`)
    pub flight_addr: String,
}

impl Default for FdapConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl FdapConfig {
    /// Load configuration from environment variables with sensible local defaults for MinIO.
    pub fn from_env() -> Self {
        let minio_endpoint = env::var("FDAP_MINIO_ENDPOINT")
            .or_else(|_| env::var("AWS_ENDPOINT_URL"))
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());

        let bucket = env::var("FDAP_BUCKET").unwrap_or_else(|_| "fdap-warehouse".to_string());

        let access_key = env::var("MINIO_ROOT_USER")
            .or_else(|_| env::var("AWS_ACCESS_KEY_ID"))
            .unwrap_or_else(|_| "minioadmin".to_string());

        let secret_key = env::var("MINIO_ROOT_PASSWORD")
            .or_else(|_| env::var("AWS_SECRET_ACCESS_KEY"))
            .unwrap_or_else(|_| "minioadmin".to_string());

        let region = env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let minio_data_dir = env::var("FDAP_MINIO_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("."))
                    .join(".minio-data")
            });

        let auto_start_minio = env::var("FDAP_AUTO_START_MINIO")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);

        let flight_addr =
            env::var("FDAP_FLIGHT_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());

        Self {
            minio_endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            minio_data_dir,
            auto_start_minio,
            flight_addr,
        }
    }

    /// Returns the base S3 URL for the configured bucket (e.g. `s3://fdap-warehouse`).
    pub fn bucket_url(&self) -> String {
        format!("s3://{}", self.bucket)
    }

    /// Returns the S3 URL for a named table inside the warehouse (`s3://fdap-warehouse/tables/<name>/`).
    pub fn table_url(&self, table_name: &str) -> String {
        format!("s3://{}/tables/{}/", self.bucket, table_name)
    }
}

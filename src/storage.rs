use std::fs;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt;
use hmac::{Hmac, Mac};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use object_store::{ClientOptions, ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};
use sha2::{Digest, Sha256};
use url::Url;

use crate::config::FdapConfig;

type HmacSha256 = Hmac<Sha256>;

/// Wrapper around an `AmazonS3` [`ObjectStore`] pointing at a local MinIO instance.
#[derive(Clone)]
pub struct MinioStorage {
    config: FdapConfig,
    store: Arc<AmazonS3>,
}

impl MinioStorage {
    /// Connects to MinIO, optionally starting a local `minio` server process if not already
    /// listening and ensuring the configured bucket exists.
    pub async fn connect(config: FdapConfig) -> Result<Self> {
        ensure_minio_available(&config).await?;

        let client_options = ClientOptions::new()
            .with_allow_http(true)
            .with_timeout(Duration::from_secs(15));

        let s3 = AmazonS3Builder::new()
            .with_endpoint(&config.minio_endpoint)
            .with_bucket_name(&config.bucket)
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.secret_key)
            .with_region(&config.region)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_client_options(client_options)
            .build()
            .context("Failed to build AmazonS3 object_store client for MinIO")?;

        Ok(Self {
            config,
            store: Arc::new(s3),
        })
    }

    /// Returns the underlying `Arc<AmazonS3>` object store.
    pub fn store(&self) -> Arc<AmazonS3> {
        self.store.clone()
    }

    /// Returns the underlying store as a generic `Arc<dyn ObjectStore>`.
    pub fn as_object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Returns the active [`FdapConfig`].
    pub fn config(&self) -> &FdapConfig {
        &self.config
    }

    /// Returns the root `s3://<bucket>` URL for registering with DataFusion.
    pub fn bucket_url(&self) -> Result<Url> {
        Url::parse(&self.config.bucket_url()).context("Invalid bucket S3 URL")
    }

    /// Lists all objects in the MinIO bucket under an optional prefix.
    pub async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<ObjectMeta>> {
        let path = prefix.map(ObjectPath::from);
        let mut metas: Vec<ObjectMeta> = self
            .store
            .list(path.as_ref())
            .try_collect()
            .await
            .context("Failed to list objects in MinIO bucket")?;
        metas.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(metas)
    }

    /// Deletes all objects under a given prefix in the MinIO bucket.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<usize> {
        let metas = self.list_objects(Some(prefix)).await?;
        let count = metas.len();
        for meta in metas {
            self.store
                .delete(&meta.location)
                .await
                .with_context(|| format!("Failed to delete object {}", meta.location))?;
        }
        Ok(count)
    }

    /// Writes raw bytes to an object path in MinIO.
    pub async fn put_bytes(&self, location: &str, data: Bytes) -> Result<()> {
        let path = ObjectPath::from(location);
        self.store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .with_context(|| format!("Failed to PUT s3://{}/{}", self.config.bucket, location))?;
        Ok(())
    }

    /// Reads raw bytes from an object path in MinIO.
    pub async fn get_bytes(&self, location: &str) -> Result<Bytes> {
        let path = ObjectPath::from(location);
        let result = self
            .store
            .get(&path)
            .await
            .with_context(|| format!("Failed to GET s3://{}/{}", self.config.bucket, location))?;
        Ok(result.bytes().await?)
    }
}

/// Ensures MinIO is reachable at `config.minio_endpoint` and the target bucket exists.
pub async fn ensure_minio_available(config: &FdapConfig) -> Result<()> {
    // Ensure reqwest / object_store does not route localhost MinIO traffic through an external proxy
    let url = Url::parse(&config.minio_endpoint).context("Invalid MinIO endpoint URL")?;
    let host = url.host_str().unwrap_or("127.0.0.1").to_string();
    let port = url.port_or_known_default().unwrap_or(9000);
    let addr = format!("{}:{}", host, port);

    // Pre-create the bucket directory inside the local MinIO data directory if running locally
    if (host == "127.0.0.1" || host == "localhost") && config.auto_start_minio {
        let bucket_dir = config.minio_data_dir.join(&config.bucket);
        let _ = fs::create_dir_all(&bucket_dir);
    }

    if !is_tcp_reachable(&addr) {
        if config.auto_start_minio && (host == "127.0.0.1" || host == "localhost") {
            spawn_local_minio(config, &addr)?;
        } else {
            bail!(
                "MinIO is not reachable at {}. Start MinIO with `./scripts/minio.sh start` or `docker compose up -d`.",
                config.minio_endpoint
            );
        }
    }

    ensure_bucket_via_s3_api(config).await?;
    Ok(())
}

fn is_tcp_reachable(addr: &str) -> bool {
    TcpStream::connect_timeout(
        &addr
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:9000".parse().unwrap()),
        Duration::from_millis(300),
    )
    .is_ok()
}

fn find_minio_binary() -> Option<PathBuf> {
    if let Ok(path_var) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let candidate = dir.join("minio");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let local_bin = PathBuf::from(home).join(".local/bin/minio");
        if local_bin.is_file() {
            return Some(local_bin);
        }
    }
    None
}

fn spawn_local_minio(config: &FdapConfig, addr: &str) -> Result<()> {
    let minio_bin = find_minio_binary().context(
        "MinIO server is not running and no `minio` binary was found in PATH or ~/.local/bin/minio",
    )?;

    fs::create_dir_all(config.minio_data_dir.join(&config.bucket))
        .context("Failed to create local MinIO data directory")?;

    let log_file = fs::File::create(config.minio_data_dir.join("minio.log"))
        .context("Failed to create MinIO log file")?;
    let err_file = log_file.try_clone()?;

    Command::new(minio_bin)
        .arg("server")
        .arg(&config.minio_data_dir)
        .arg("--address")
        .arg(addr)
        .arg("--console-address")
        .arg("127.0.0.1:9001")
        .env("MINIO_ROOT_USER", &config.access_key)
        .env("MINIO_ROOT_PASSWORD", &config.secret_key)
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(err_file))
        .spawn()
        .context("Failed to spawn local MinIO server")?;

    for _ in 0..50 {
        if is_tcp_reachable(addr) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    bail!("Timed out waiting for local MinIO server to listen on {}", addr)
}

/// Sends a signed AWS SigV4 `PUT /{bucket}` request to MinIO to ensure the bucket exists
/// (idempotent if the bucket already exists).
async fn ensure_bucket_via_s3_api(config: &FdapConfig) -> Result<()> {
    let endpoint = config.minio_endpoint.trim_end_matches('/');
    let url_str = format!("{}/{}", endpoint, config.bucket);
    let parsed = Url::parse(&url_str)?;
    let host_header = match parsed.port() {
        Some(port) => format!("{}:{}", parsed.host_str().unwrap_or("127.0.0.1"), port),
        None => parsed.host_str().unwrap_or("127.0.0.1").to_string(),
    };

    let now = Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let payload_hash = hex::encode(Sha256::digest(b""));

    let canonical_uri = format!("/{}", config.bucket);
    let canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        host_header, payload_hash, amz_date
    );
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "PUT\n{}\n\n{}\n{}\n{}",
        canonical_uri, canonical_headers, signed_headers, payload_hash
    );

    let credential_scope = format!("{}/{}/s3/aws4_request", date_stamp, config.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        credential_scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", config.secret_key).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, config.region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        config.access_key, credential_scope, signed_headers, signature
    );

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()?;

    let resp = client
        .put(&url_str)
        .header("Host", &host_header)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash)
        .header("Authorization", &auth_header)
        .send()
        .await
        .context("Failed to send bucket creation request to MinIO")?;

    let status = resp.status().as_u16();
    // 200 OK = created, 409 BucketAlreadyOwnedByYou / BucketAlreadyExists = already exists
    if status == 200 || status == 204 || status == 409 {
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        bail!(
            "Unexpected S3 response creating bucket '{}' (HTTP {}): {}",
            config.bucket,
            status,
            body
        )
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightActionResult,
    SchemaAsIpc, SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status, Streaming};

use crate::engine::{FdapEngine, QueryExplainReport, TableSummary};
use crate::parquet_io::ParquetWriteSummary;

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Arrow Flight gRPC service backed by [`FdapEngine`] (DataFusion + Parquet on MinIO).
#[derive(Clone)]
pub struct FdapFlightService {
    engine: Arc<FdapEngine>,
}

impl FdapFlightService {
    pub fn new(engine: Arc<FdapEngine>) -> Self {
        Self { engine }
    }

    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    fn resolve_sql_from_descriptor(descriptor: &FlightDescriptor) -> Result<String, Status> {
        match DescriptorType::try_from(descriptor.r#type).unwrap_or(DescriptorType::Unknown) {
            DescriptorType::Cmd => {
                let sql = String::from_utf8(descriptor.cmd.to_vec())
                    .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 SQL: {}", e)))?;
                Ok(sql)
            }
            DescriptorType::Path => {
                let table = descriptor
                    .path
                    .first()
                    .ok_or_else(|| Status::invalid_argument("Empty FlightDescriptor path"))?;
                Ok(format!("SELECT * FROM {}", table))
            }
            _ => Err(Status::invalid_argument(
                "FlightDescriptor must be CMD (SQL query) or PATH (table name)",
            )),
        }
    }
}

#[tonic::async_trait]
impl FlightService for FdapFlightService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoActionStream = BoxedFlightStream<FlightActionResult>;
    type ListActionsStream = BoxedFlightStream<ActionType>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut inbound = request.into_inner();
        let payload = if let Some(req) = inbound.message().await? {
            req.payload
        } else {
            Bytes::from_static(b"fdap-ok")
        };
        let resp = HandshakeResponse {
            protocol_version: 1,
            payload,
        };
        Ok(Response::new(Box::pin(stream::once(async { Ok(resp) }))))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let summaries = self
            .engine
            .table_summaries()
            .await
            .map_err(|e| Status::internal(format!("Failed to list tables: {}", e)))?;

        let mut infos = Vec::with_capacity(summaries.len());
        for summary in summaries {
            let provider = self
                .engine
                .session_context()
                .table_provider(&summary.table_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let schema = provider.schema();
            let descriptor = FlightDescriptor::new_path(vec![summary.table_name.clone()]);
            let ticket = Ticket::new(format!("SELECT * FROM {}", summary.table_name));
            let endpoint = FlightEndpoint::new().with_ticket(ticket);

            let info = FlightInfo::new()
                .try_with_schema(schema.as_ref())
                .map_err(|e| Status::internal(format!("Schema encoding error: {}", e)))?
                .with_descriptor(descriptor)
                .with_endpoint(endpoint)
                .with_total_records(summary.row_count)
                .with_total_bytes(summary.total_bytes as i64);

            infos.push(Ok(info));
        }

        Ok(Response::new(Box::pin(stream::iter(infos))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let sql = Self::resolve_sql_from_descriptor(&descriptor)?;

        let df = self
            .engine
            .session_context()
            .sql(&sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL planning failed: {}", e)))?;

        let arrow_schema = df.schema().inner().clone();
        let ticket = Ticket::new(sql.into_bytes());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = FlightInfo::new()
            .try_with_schema(arrow_schema.as_ref())
            .map_err(|e| Status::internal(format!("Failed to encode Arrow schema: {}", e)))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint);

        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not used"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let sql = Self::resolve_sql_from_descriptor(&descriptor)?;

        let df = self
            .engine
            .session_context()
            .sql(&sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL planning failed: {}", e)))?;

        let options = IpcWriteOptions::default();
        let schema_result: SchemaResult = SchemaAsIpc::new(df.schema().inner().as_ref(), &options)
            .try_into()
            .map_err(|e| Status::internal(format!("Schema IPC error: {}", e)))?;

        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| Status::invalid_argument(format!("Invalid UTF-8 SQL in ticket: {}", e)))?;

        let df = self
            .engine
            .session_context()
            .sql(&sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL planning error: {}", e)))?;

        let schema = df.schema().inner().clone();
        let batch_stream = df
            .execute_stream()
            .await
            .map_err(|e| Status::internal(format!("Execution error: {}", e)))?;

        let mapped_batches = batch_stream.map_err(|e| FlightError::ExternalError(Box::new(e)));

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(mapped_batches)
            .map_err(|e| Status::internal(format!("Flight encoding error: {}", e)));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut inbound = request.into_inner().peekable();

        // Peek the first FlightData frame to extract the target table name from FlightDescriptor
        let first_frame = Pin::new(&mut inbound)
            .peek()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty do_put FlightData stream"))?
            .as_ref()
            .map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

        let descriptor = first_frame
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing FlightDescriptor on first do_put message"))?;

        let table_name = if !descriptor.path.is_empty() {
            descriptor.path[0].clone()
        } else if !descriptor.cmd.is_empty() {
            String::from_utf8_lossy(&descriptor.cmd).to_string()
        } else {
            return Err(Status::invalid_argument(
                "FlightDescriptor must specify target table name in path or cmd",
            ));
        };

        let flight_stream = inbound.map_err(|e| FlightError::Tonic(Box::new(e)));
        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(flight_stream);
        let batches: Vec<RecordBatch> = record_batch_stream
            .try_collect()
            .await
            .map_err(|e| Status::invalid_argument(format!("Failed decoding Flight batches: {}", e)))?;

        if batches.is_empty() {
            return Err(Status::invalid_argument("No RecordBatches received in do_put"));
        }

        let summary = self
            .engine
            .ingest_batches(&table_name, &batches, 100)
            .await
            .map_err(|e| Status::internal(format!("Failed writing Parquet to MinIO: {}", e)))?;

        let metadata_json = serde_json::to_vec(&summary)
            .map_err(|e| Status::internal(format!("JSON serialization error: {}", e)))?;

        let put_result = PutResult {
            app_metadata: Bytes::from(metadata_json),
        };

        Ok(Response::new(Box::pin(stream::once(async {
            Ok(put_result)
        }))))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not used"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let payload_bytes: Vec<u8> = match action.r#type.as_str() {
            "ping" => serde_json::to_vec(&serde_json::json!({
                "status": "ok",
                "engine": "fdap",
                "minio_bucket": self.engine.storage().config().bucket,
                "minio_endpoint": self.engine.storage().config().minio_endpoint,
            }))
            .unwrap_or_default(),
            "list_tables" => {
                let summaries = self
                    .engine
                    .table_summaries()
                    .await
                    .map_err(|e| Status::internal(format!("list_tables failed: {}", e)))?;
                serde_json::to_vec(&summaries).unwrap_or_default()
            }
            "explain" => {
                let sql = String::from_utf8(action.body.to_vec())
                    .map_err(|e| Status::invalid_argument(format!("Invalid SQL UTF-8: {}", e)))?;
                let report = self
                    .engine
                    .explain(&sql)
                    .await
                    .map_err(|e| Status::invalid_argument(format!("Explain failed: {}", e)))?;
                serde_json::to_vec(&report).unwrap_or_default()
            }
            "seed_warehouse" => {
                let written = self
                    .engine
                    .seed_default_tables()
                    .await
                    .map_err(|e| Status::internal(format!("seed_warehouse failed: {}", e)))?;
                serde_json::to_vec(&written).unwrap_or_default()
            }
            "refresh_catalog" => {
                let tables = self
                    .engine
                    .discover_tables()
                    .await
                    .map_err(|e| Status::internal(format!("refresh_catalog failed: {}", e)))?;
                serde_json::to_vec(&tables).unwrap_or_default()
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unknown action type '{}'. Supported actions: ping, list_tables, explain, seed_warehouse, refresh_catalog",
                    other
                )));
            }
        };

        let result = FlightActionResult {
            body: Bytes::from(payload_bytes),
        };
        Ok(Response::new(Box::pin(stream::once(async { Ok(result) }))))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            Ok(ActionType {
                r#type: "ping".to_string(),
                description: "Health check for Flight server and MinIO warehouse".to_string(),
            }),
            Ok(ActionType {
                r#type: "list_tables".to_string(),
                description: "List all MinIO Parquet tables, file counts, byte sizes, and schemas"
                    .to_string(),
            }),
            Ok(ActionType {
                r#type: "explain".to_string(),
                description: "Return Logical, Optimized, and Physical DataFusion execution plans"
                    .to_string(),
            }),
            Ok(ActionType {
                r#type: "seed_warehouse".to_string(),
                description: "Seed default telemetry, devices, and incidents Parquet tables in MinIO"
                    .to_string(),
            }),
            Ok(ActionType {
                r#type: "refresh_catalog".to_string(),
                description: "Scan MinIO bucket and auto-register all Parquet table directories"
                    .to_string(),
            }),
        ];
        Ok(Response::new(Box::pin(stream::iter(actions))))
    }
}

/// Spawns the FDAP Arrow Flight gRPC server on a background Tokio task and returns the bound [`SocketAddr`].
pub async fn spawn_flight_server(
    engine: Arc<FdapEngine>,
    bind_addr: &str,
) -> Result<(SocketAddr, tokio::task::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("Failed to bind Flight server on {}", bind_addr))?;
    let local_addr = listener.local_addr()?;
    let service = FdapFlightService::new(engine);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming(incoming)
            .await
            .context("Flight gRPC server error")
    });

    Ok((local_addr, handle))
}

/// High-level async Arrow Flight client for interacting with [`FdapFlightService`].
pub struct FdapFlightClient {
    inner: FlightServiceClient<Channel>,
}

impl FdapFlightClient {
    /// Connects to an FDAP Arrow Flight gRPC endpoint (e.g. `http://127.0.0.1:50051`).
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.to_string()
        } else {
            format!("http://{}", endpoint)
        };
        let channel = Channel::from_shared(url.clone())
            .with_context(|| format!("Invalid Flight endpoint URL: {}", url))?
            .connect()
            .await
            .with_context(|| format!("Failed to connect to Flight server at {}", url))?;
        Ok(Self {
            inner: FlightServiceClient::new(channel),
        })
    }

    /// Executes a SQL query via Arrow Flight (`GetFlightInfo` -> `DoGet`) and returns
    /// the decoded Arrow [`RecordBatch`]es.
    pub async fn execute_sql(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let descriptor = FlightDescriptor::new_cmd(sql.as_bytes().to_vec());
        let info = self
            .inner
            .get_flight_info(descriptor)
            .await
            .context("Flight get_flight_info RPC failed")?
            .into_inner();

        let ticket = info
            .endpoint
            .into_iter()
            .next()
            .and_then(|ep| ep.ticket)
            .context("FlightInfo did not contain a Ticket")?;

        let inbound = self
            .inner
            .do_get(ticket)
            .await
            .context("Flight do_get RPC failed")?
            .into_inner();

        let batch_stream = FlightRecordBatchStream::new_from_flight_data(
            inbound.map_err(|e| FlightError::Tonic(Box::new(e))),
        );
        let batches: Vec<RecordBatch> = batch_stream
            .try_collect()
            .await
            .context("Failed decoding RecordBatches from Flight stream")?;
        Ok(batches)
    }

    /// Streams Arrow [`RecordBatch`]es to the server via `DoPut`, which persists them
    /// as a Parquet file in MinIO and registers the table in DataFusion.
    pub async fn put_batches(
        &mut self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<ParquetWriteSummary> {
        if batches.is_empty() {
            bail!("Cannot send empty batch list in put_batches");
        }
        let schema = batches[0].schema();
        let descriptor = FlightDescriptor::new_path(vec![table_name.to_string()]);
        let input_stream = stream::iter(batches.into_iter().map(Ok));

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_flight_descriptor(Some(descriptor))
            .build(input_stream)
            .filter_map(|res| async move { res.ok() });

        let mut response_stream = self
            .inner
            .do_put(Request::new(flight_data_stream))
            .await
            .context("Flight do_put RPC failed")?
            .into_inner();

        let put_result = response_stream
            .message()
            .await?
            .context("Server did not return a PutResult")?;

        let summary: ParquetWriteSummary = serde_json::from_slice(&put_result.app_metadata)
            .context("Failed to deserialize ParquetWriteSummary from PutResult")?;
        Ok(summary)
    }

    /// Invokes the `list_tables` Flight action to retrieve metadata for all MinIO Parquet tables.
    pub async fn list_tables(&mut self) -> Result<Vec<TableSummary>> {
        let action = Action::new("list_tables", Bytes::new());
        let mut stream = self.inner.do_action(action).await?.into_inner();
        let msg = stream.message().await?.context("Empty list_tables response")?;
        Ok(serde_json::from_slice(&msg.body)?)
    }

    /// Invokes the `explain` Flight action to retrieve logical and physical execution plans.
    pub async fn explain(&mut self, sql: &str) -> Result<QueryExplainReport> {
        let action = Action::new("explain", Bytes::from(sql.to_string()));
        let mut stream = self.inner.do_action(action).await?.into_inner();
        let msg = stream.message().await?.context("Empty explain response")?;
        Ok(serde_json::from_slice(&msg.body)?)
    }

    /// Invokes the `seed_warehouse` Flight action to populate MinIO with sample datasets.
    pub async fn seed_warehouse(&mut self) -> Result<Vec<ParquetWriteSummary>> {
        let action = Action::new("seed_warehouse", Bytes::new());
        let mut stream = self.inner.do_action(action).await?.into_inner();
        let msg = stream
            .message()
            .await?
            .context("Empty seed_warehouse response")?;
        Ok(serde_json::from_slice(&msg.body)?)
    }

    /// Returns the raw [`FlightServiceClient`] for low-level Flight protocol inspection.
    pub fn inner_mut(&mut self) -> &mut FlightServiceClient<Channel> {
        &mut self.inner
    }
}

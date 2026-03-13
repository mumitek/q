use std::{error::Error, fs::create_dir_all, sync::Arc};

use crate::{cli::QConfig, lb::PartitionBalancer};
use qp::{queue_service_server::*, *};
use tonic::{Request, Response, Status, transport::Server};
use tower_http::trace::TraceLayer;
use tracing::{debug, info, instrument};

struct PartitionedQueueService {
    balancer: Arc<PartitionBalancer>,
}

#[tonic::async_trait]
impl QueueService for PartitionedQueueService {
    #[instrument(skip(self))]
    async fn push(&self, request: Request<PushRequest>) -> Result<Response<PushResponse>, Status> {
        let request = request.into_inner();
        debug!(queue_id = request.queue_id, "Ignoring queue_id");
        self.balancer
            .add(&request.message) // message is Vec<u8>
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PushResponse {}))
    }

    async fn pop(&self, request: Request<PopRequest>) -> Result<Response<PopResponse>, Status> {
        let request = request.into_inner();
        debug!(queue_id = request.queue_id, "Ignoring queue_id");
        let message = self
            .balancer
            .next()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(PopResponse { message }))
    }

    async fn create_queue(
        &self,
        _: Request<CreateQueueRequest>,
    ) -> Result<Response<CreateQueueResponse>, Status> {
        info!("Not implemented yet");
        Ok(Response::new(CreateQueueResponse {
            queue_id: "".to_string(),
        }))
    }

    async fn delete_queue(
        &self,
        _: Request<DeleteQueueRequest>,
    ) -> Result<Response<DeleteQueueResponse>, Status> {
        info!("Not implemented yet");
        Ok(Response::new(DeleteQueueResponse {}))
    }
}

pub(crate) async fn start_server(cfg: QConfig) -> Result<(), Box<dyn Error>> {
    info!(cfg = ?cfg, "Got config");
    create_dir_all(&cfg.data_dir)?;

    let addr = format!("{}:{}", cfg.host, cfg.port).parse()?;
    let balancer = PartitionBalancer::try_new(&cfg.data_dir, cfg.num_partition)?;

    let service = QueueServiceServer::new(PartitionedQueueService {
        balancer: Arc::new(balancer),
    });
    debug!("Server listening on {}", addr);

    Server::builder()
        .layer(TraceLayer::new_for_grpc())
        .add_service(service)
        // This is the key change: it waits for the signal to finish
        .serve(addr)
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use pretty_assertions::assert_eq;
    use qp::queue_service_client::QueueServiceClient;
    use tempfile::tempdir;
    use tokio::time::{Duration, sleep};
    use tonic::Code;

    // Helper to setup the service with a clean temp directory
    fn setup_service() -> (PartitionedQueueService, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let balancer =
            PartitionBalancer::try_new(dir.path(), 1).expect("Failed to create balancer");

        (
            PartitionedQueueService {
                balancer: Arc::new(balancer),
            },
            dir,
        )
    }

    #[tokio::test]
    async fn test_push_message_success() {
        let (service, _dir) = setup_service();
        let message = b"hello world".to_vec();

        let request = Request::new(PushRequest {
            queue_id: "test-queue".to_string(),
            message: message.clone(),
        });

        // Use unwrap() to fail with clear error if Result is Err
        let response = service.push(request).await.unwrap();

        assert_eq!(response.into_inner(), PushResponse {});
    }

    #[tokio::test]
    async fn test_pop_message_returns_pushed_data() {
        let (service, _dir) = setup_service();
        let message_content = b"unit-test-data".to_vec();

        // Push first
        service
            .push(Request::new(PushRequest {
                queue_id: "q1".to_string(),
                message: message_content.clone(),
            }))
            .await
            .unwrap();

        // Pop and verify
        let pop_request = Request::new(PopRequest {
            queue_id: "q1".to_string(),
        });

        let response = service.pop(pop_request).await.unwrap();
        assert_eq!(response.into_inner().message, message_content);
    }

    #[tokio::test]
    async fn test_create_queue_returns_placeholder() {
        let (service, _dir) = setup_service();
        let request = Request::new(CreateQueueRequest {
            queue_name: "".to_string(),
        });

        let response = service.create_queue(request).await.unwrap();

        // Verifying the specific "Not implemented" behavior described in the code
        assert_eq!(response.into_inner().queue_id, "".to_string());
    }

    #[tokio::test]
    async fn test_delete_queue_returns_empty_response() {
        let (service, _dir) = setup_service();
        let request = Request::new(DeleteQueueRequest {
            queue_id: "to-delete".to_string(),
        });

        let response = service.delete_queue(request).await.unwrap();

        assert_eq!(response.into_inner(), DeleteQueueResponse {});
    }

    #[tokio::test]
    async fn test_push_ignoring_queue_id_logic() {
        // Since the code explicitly logs "Ignoring queue_id", we verify
        // that pushing to 'queue-a' and popping from 'queue-b' works
        // (confirming the current implementation's global balancer behavior).
        let (service, _dir) = setup_service();
        let data = b"global-data".to_vec();

        service
            .push(Request::new(PushRequest {
                queue_id: "queue-a".to_string(),
                message: data.clone(),
            }))
            .await
            .unwrap();

        let response = service
            .pop(Request::new(PopRequest {
                queue_id: "queue-b".to_string(),
            }))
            .await
            .unwrap();

        assert_eq!(response.into_inner().message, data);
    }

    async fn spawn_test_server(data_dir: std::path::PathBuf) -> u16 {
        let port = 50051; // In a production test, you'd ideally find a free port
        let cfg = QConfig {
            host: "127.0.0.1".to_string(),
            port,
            data_dir: data_dir.into_os_string().into_string().unwrap(),
            num_partition: 1,
        };

        // Spawn server in the background
        tokio::spawn(async move {
            start_server(cfg).await.expect("Server failed to start");
        });

        // Give the server a moment to bind to the socket
        sleep(Duration::from_millis(100)).await;
        port
    }

    #[tokio::test]
    async fn test_start_server_and_push_pop_client() {
        let dir = tempdir().expect("Failed to create temp dir");
        let port = spawn_test_server(dir.path().to_path_buf()).await;

        // Setup Client
        let channel = tonic::transport::Channel::from_shared(format!("http://127.0.0.1:{}", port))
            .expect("Invalid URI")
            .connect()
            .await
            .expect("Failed to connect to server");

        let mut client = QueueServiceClient::new(channel);

        // 1. Test Push via Client
        let push_req = PushRequest {
            queue_id: "integration-q".to_string(),
            message: b"net-payload".to_vec(),
        };
        let push_res = client.push(push_req).await.unwrap();
        assert_eq!(push_res.into_inner(), PushResponse {});

        // 2. Test Pop via Client
        let pop_req = PopRequest {
            queue_id: "integration-q".to_string(),
        };
        let pop_res = client.pop(pop_req).await.unwrap();
        assert_eq!(pop_res.into_inner().message, b"net-payload".to_vec());
    }

    #[tokio::test]
    async fn test_start_server_failure() {
        let dir = tempdir().expect("Failed to create temp dir");
        let port = 50051; // In a production test, you'd ideally find a free port
        let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();

        // 1. Bind a listener to the port first to "occupy" it
        let _occupier = std::net::TcpListener::bind(addr).unwrap();
        let cfg = QConfig {
            host: "127.0.0.1".to_string(),
            port,
            data_dir: dir.path().to_str().unwrap().to_string(),
            num_partition: 1,
        };

        let result = start_server(cfg).await;

        assert!(result.is_err(), "Expect failure when host is invalid")
    }

    #[tokio::test]
    async fn test_start_server_creates_data_directory() {
        let base_dir = tempdir().expect("Failed to create temp dir");
        let nested_data_dir = base_dir.path().join("subdir/data");

        // Ensure the directory doesn't exist yet
        assert_eq!(nested_data_dir.exists(), false);

        let cfg = QConfig {
            host: "127.0.0.1".to_string(),
            port: 50052,
            data_dir: nested_data_dir
                .clone()
                .into_os_string()
                .into_string()
                .unwrap(),
            num_partition: 1,
        };

        // We only need to trigger the start_server setup logic
        // start_server calls create_dir_all before it starts listening
        let _keep = tokio::spawn(async move {
            let _ = start_server(cfg).await;
        });

        // Poll for directory creation
        let mut created = false;
        for _ in 0..10 {
            if nested_data_dir.exists() {
                created = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(
            created, true,
            "start_server should have created the data directory"
        );
    }

    #[tokio::test]
    async fn test_pop_fails_when_queue_is_empty() {
        let (service, _dir) = setup_service(); // Using the helper from previous response

        // Attempting to pop from a fresh balancer that has no messages.
        // If your PartitionBalancer returns an error on empty (instead of None),
        // the service will map it to Internal.
        let request = Request::new(PopRequest {
            queue_id: "empty-q".to_string(),
        });

        let result = service.pop(request).await;

        // If your balancer logic treats "Empty" as an error (common in strict FIFO balancers):
        let err = result.unwrap_err();
        assert_eq!(err.code(), Code::Internal);
    }
}

use std::error::Error;

use crate::{cli::QConfig, partition::Partition};
use clap::Parser;
use qp::{queue_service_server::*, *};
use tonic::{Request, Response, Status, transport::Server};
use tower_http::trace::TraceLayer;
use tracing::{info, instrument};
use tracing_subscriber::EnvFilter;

struct Q {
    partition: Partition,
}

#[tonic::async_trait]
impl QueueService for Q {
    #[instrument(skip(self))]
    async fn push(&self, request: Request<PushRequest>) -> Result<Response<PushResponse>, Status> {
        Ok(Response::new(PushResponse {}))
    }

    async fn pop(&self, request: Request<PopRequest>) -> Result<Response<PopResponse>, Status> {
        dbg!(request);
        todo!()
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

pub(crate) async fn start_server() -> Result<(), Box<dyn Error>> {
    let cli = QConfig::try_parse()?;
    info!(cli = ?cli, "Parsed CLI parameters");
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    info!("Setting up logger");
    let addr = format!("[::1]:{}", cli.port).parse()?;
    info!("Statring server");
    Server::builder()
        .layer(TraceLayer::new_for_grpc())
        .add_service(QueueServiceServer::new(Q {
            partition: Partition::try_new("", "").unwrap(),
        }))
        .serve(addr)
        .await?;
    info!("Server started");
    Ok(())
}

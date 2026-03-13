pub mod cli;
pub mod lb;
pub mod partition;
pub mod server;

use clap::Parser;
use server::start_server;
use std::error::Error;
use tracing_subscriber::EnvFilter;

use crate::cli::QConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = QConfig::try_parse()?;
    start_server(cfg).await
}

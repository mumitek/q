pub mod cli;
pub mod partition;
pub mod server;

use server::start_server;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    start_server().await
}

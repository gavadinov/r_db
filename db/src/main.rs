#![warn(clippy::all)]

use crate::api::storage_api::storage_server::StorageServer;
use crate::server::StorageService;
use crate::storage::shard_map::ShardMap;
use std::sync::Arc;
use tonic::transport::Server;

mod api;
mod server;
mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:10000".parse().unwrap();

    println!("StorageService listening on: {}", addr);
    let shard_map = Arc::new(ShardMap::new());
    let storage_service = StorageService::new(shard_map);
    Server::builder()
        .add_service(StorageServer::new(storage_service))
        .serve(addr)
        .await?;

    Ok(())
}

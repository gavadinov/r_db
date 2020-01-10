use crate::api::storage_api::storage_server::Storage;
use crate::api::storage_api::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse,
};
use crate::storage::shard_map::ShardMap;
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

#[derive(Clone)]
pub struct StorageService {
    shard_map: Arc<ShardMap>,
}

impl StorageService {
    pub fn new(shard_map: Arc<ShardMap>) -> Self {
        Self { shard_map }
    }
}

#[tonic::async_trait]
impl Storage for StorageService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        let shard_id = request.shard_id as usize;
        let key = request.key;

        let reader = self
            .shard_map
            .reader(&shard_id)
            .expect(&*format!("Missing shard with id: {}", shard_id));

        let result = reader.get(&key);
        match result {
            Some(val) => Ok(Response::new(GetResponse { val })),
            None => Err(Status::new(Code::NotFound, "Not found")),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let request = request.into_inner();
        let shard_id = request.shard_id as usize;
        let key = request.key;
        let val = request.val;

        let writer = self
            .shard_map
            .writer(&shard_id)
            .expect(&*format!("Missing shard with id: {}", shard_id));

        writer
            .lock()
            .expect(&*format!("Can't lock writer for shard: {}", shard_id))
            .put(key, val);

        Ok(Response::new(PutResponse {}))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let shard_id = request.shard_id as usize;
        let key = request.key;

        let writer = self
            .shard_map
            .writer(&shard_id)
            .expect(&*format!("Missing shard with id: {}", shard_id));

        writer
            .lock()
            .expect(&*format!("Can't lock writer for shard: {}", shard_id))
            .delete(&key);

        Ok(Response::new(DeleteResponse {}))
    }
}

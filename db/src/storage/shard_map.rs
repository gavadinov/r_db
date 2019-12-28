use super::shard::{Reader, Shard, Writer};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct ShardMap {
    shards: RwLock<HashMap<usize, Shard>>,
}

impl ShardMap {
    pub fn new() -> Self {
        ShardMap {
            shards: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, shard: Shard) {
        self.shards.write().unwrap().insert(shard.id(), shard);
    }

    pub fn remove(&self, shard_id: &usize) {
        self.shards.write().unwrap().remove(shard_id);
    }

    pub fn reader(&self, shard_id: &usize) -> Option<Reader> {
        Some(self.shards.read().unwrap().get(shard_id)?.reader())
    }

    pub fn writer(&self, shard_id: &usize) -> Option<Arc<Mutex<Writer>>> {
        Some(self.shards.read().unwrap().get(shard_id)?.writer())
    }
}

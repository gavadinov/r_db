use super::types::{Key, Val};
use crate::storage::shard_map::ShardMap;
use std::sync::mpsc::{channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

enum RequestType {
    Put(usize, Key, Val),
    Remove(usize, Key),
    Get(usize, Key),
}

pub enum QueryResult {
    Empty,
    Data(Option<Val>),
}

pub struct Request {
    request_type: RequestType,
    result: SyncSender<QueryResult>,
}

pub struct ThreadPool {
    sender: Sender<Request>,
    receiver: Arc<Mutex<Receiver<Request>>>,
    num_threads: usize,
    shard_map: Arc<ShardMap>,
}

struct PanicGuard {
    active: bool,
}

impl PanicGuard {
    fn new() -> Self {
        PanicGuard { active: true }
    }

    fn cancel(mut self) {
        self.active = false;
    }
}

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if self.active {}
    }
}

impl ThreadPool {
    pub fn new(num_threads: usize, shard_map: ShardMap) -> Self {
        let (sender, receiver) = channel();

        ThreadPool {
            num_threads,
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            shard_map: Arc::new(shard_map),
        }
    }

    pub fn execute(&self, req: Request) {
        let sender = self.sender.clone();
    }

    fn run(&self) {
        for i in 0..self.num_threads {
            let mut builder = thread::Builder::new();
            let receiver = self.receiver.clone();
            let shard_map = self.shard_map.clone();

            builder
                .name(format!("DB-Thread-{}", i))
                .spawn(move || {
                    let panic_guard = PanicGuard::new();

                    loop {
                        let message = {
                            let lock = receiver.lock().expect("Unable to lock receiver");
                            lock.recv()
                        };

                        let request = match message {
                            Ok(request) => request,
                            // The pool has been deallocated
                            Err(_) => break,
                        };

                        let result = match request.request_type {
                            RequestType::Get(shard_id, key) => {
                                let reader = shard_map
                                    .reader(&shard_id)
                                    .expect(&*format!("Missing shard with id: {}", shard_id));

                                let result = reader.get(&key);
                                QueryResult::Data(result)
                            }
                            RequestType::Put(shard_id, key, val) => {
                                let writer = shard_map
                                    .writer(&shard_id)
                                    .expect(&*format!("Missing shard with id: {}", shard_id));
                                writer
                                    .lock()
                                    .expect(&*format!("Can't lock writer for shard: {}", shard_id))
                                    .put(key, val);

                                QueryResult::Empty
                            }
                            RequestType::Remove(shard_id, key) => {
                                let writer = shard_map
                                    .writer(&shard_id)
                                    .expect(&*format!("Missing shard with id: {}", shard_id));
                                writer
                                    .lock()
                                    .expect(&*format!("Can't lock writer for shard: {}", shard_id))
                                    .delete(&key);

                                QueryResult::Empty
                            }
                        };

                        request.result.send(result).unwrap();
                    }

                    panic_guard.cancel();
                })
                .unwrap();
        }
    }
}

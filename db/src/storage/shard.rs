use std::collections::HashMap;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

type Key = Vec<u8>;
type Val = Vec<u8>;

pub struct Shard {
    id: u64,
    r: Arc<AtomicPtr<HashMap<Key, Val>>>,
    w: Option<Box<HashMap<Key, Val>>>,
}

// TODO: Split into write and reader. The writer should always be one. It will need to be behind a mutex?
// TODO: Figure out how to wait for all readers to finish with the old pointer after swap. Release the mutex after that.

impl Shard {
    pub fn new(id: u64) -> Self {
        Shard {
            id,
            r: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(HashMap::new())))),
            w: Some(Box::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &Key) -> Option<&Val> {
        self.reader().get(key)
    }

    pub fn put(&mut self, key: Key, value: Val) -> Option<Val> {
        self.writer().insert(key, value)
    }

    pub fn delete(&mut self, key: &Key) -> Option<Val> {
        self.writer().remove(key)
    }

    pub fn size(&self) -> usize {
        self.reader().len()
    }

    fn swap(&mut self) {
        let w = self.w.take().unwrap();
        let new_w = self.r.swap(Box::into_raw(w), Release);
        unsafe {
            self.w = Some(Box::from_raw(new_w));
        }
    }

    fn writer(&mut self) -> &mut HashMap<Key, Val> {
        self.w.as_mut().unwrap()
    }

    fn reader(&self) -> &HashMap<Key, Val> {
        // Unwrap should never panic because self.r is always initialized
        unsafe { self.r.load(Acquire).as_ref().unwrap() }
    }
}

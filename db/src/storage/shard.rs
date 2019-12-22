use std::borrow::Borrow;
use std::collections::HashMap;
use std::mem::swap;
use std::panic::set_hook;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

type Key = Vec<u8>;
type Val = Vec<u8>;
type Map = HashMap<Key, Val>;

// TODO: Figure out how to wait for all readers to finish with the old pointer after swap. Release the mutex after that.
// TODO: Check the smallvec crate
// TODO: It is super optimized for reads. What about creaing a new shard and copying data from the old one. It's a huge burst of writes.
// TODO: Instead of storing the shard and having mut ref for the writer and immut for the readers I can have just a (reader, writer) in the shards map

pub struct Reader {
    data: Arc<AtomicPtr<Map>>,
}

pub struct Writer {
    data: Option<Box<Map>>,
    reader: Reader,
}

pub struct Shard {
    id: u64,
    reader: Reader,
    writer: Arc<Mutex<Writer>>,
}

impl Reader {
    pub fn new() -> Self {
        Self {
            data: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(HashMap::new())))),
        }
    }

    pub fn get(&self, key: &Key) -> Option<&Val> {
        self.reader().get(key)
    }

    #[inline]
    fn reader(&self) -> &Map {
        // Unwrap should never panic because self.r is always initialized
        unsafe { self.data.load(Acquire).as_ref().unwrap() }
    }
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

impl Writer {
    pub fn new(r: Reader) -> Self {
        Self {
            data: Some(Box::new(HashMap::new())),
            reader: r,
        }
    }

    // TODO: Fix the code duplication with delete
    pub fn put(&mut self, key: Key, value: Val) -> Option<Val> {
        let mut data = self.data();
        data.insert(key.clone(), value.clone());

        self.swap(data);

        // Writer has changed
        let mut data = self.data();
        let result = data.insert(key, value);
        self.data = Some(data);

        result
    }

    // TODO: Fix the code duplication with put
    pub fn delete(&mut self, key: &Key) -> Option<Val> {
        let mut data = self.data();
        data.remove(key);

        self.swap(data);

        // Writer has changed
        let mut data = self.data();
        let result = data.remove(key);
        self.data = Some(data);

        result
    }

    fn swap(&mut self, data: Box<Map>) {
        // Because Box::into_raw consumes the Box we have to keep the self.data in an Option
        // so it can be swapped with None and then put back in
        let new_data = self.reader.data.swap(Box::into_raw(data), Release);
        unsafe {
            self.data = Some(Box::from_raw(new_data));
        }
    }

    // Will leave a None in place of self.data
    #[inline]
    fn data(&mut self) -> Box<Map> {
        self.data.take().unwrap()
    }
}

impl Shard {
    pub fn new(id: u64) -> Self {
        let reader = Reader::new();
        Self {
            id,
            writer: Arc::new(Mutex::new(Writer::new(reader.clone()))),
            reader,
        }
    }

    pub fn writer(&mut self) -> MutexGuard<Writer> {
        self.writer.lock().unwrap()
    }

    pub fn reader(&self) -> Reader {
        self.reader.clone()
    }
}

#[test]
fn t() {
    let mut s = Shard::new(42);
    let r = s.reader();
    let mut w = s.writer();
    w.put(vec![1], vec![2]);
    assert_eq!(r.get(&vec![1 as u8]), Some(&vec![2 as u8]));
}

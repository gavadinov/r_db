use std::collections::HashMap;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

type Key = Vec<u8>;
type Val = Vec<u8>;
type Map = HashMap<Key, Val>;
type Counter = Arc<AtomicUsize>;
const MIN_STALE_POINTERS: usize = 0;

// TODO: Figure out how to wait for all readers to finish with the old pointer after swap. Release the mutex after that.
// TODO: Check the smallvec crate
// TODO: It is super optimized for reads. What about creating a new shard and copying data from the old one. It's a huge burst of writes.
// TODO: Instead of storing the shard and having mut ref for the writer and immut for the readers I can have just a (reader, writer) in the shards map

pub struct Reader {
    data: Arc<AtomicPtr<Map>>,
    // false: first, true: second
    mode: Arc<AtomicBool>,
    // Use the Arc as a counter. After swapping the pointers wait for the ref count to go to 0
    first: Counter,
    second: Counter,
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
            mode: Arc::new(AtomicBool::new(false)),
            first: Arc::new(AtomicUsize::new(0)),
            second: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get(&self, key: &Key) -> Option<&Val> {
        let result;
        let data = self.data();

        self.increment_curr_counter();
        result = data.get(key);
        self.decrement_curr_counter();

        result
    }

    #[inline]
    fn data(&self) -> &Map {
        // Unwrap should never panic because self.r is always initialized
        unsafe { self.data.load(Acquire).as_ref().unwrap() }
    }

    fn counter_count(&self, mode: bool) -> usize {
        if mode {
            self.second.load(Acquire)
        } else {
            self.first.load(Acquire)
        }
    }

    #[inline]
    fn decrement_curr_counter(&self) {
        if self.mode.load(Acquire) {
            self.second.fetch_sub(1, AcqRel);
        } else {
            self.first.fetch_sub(1, AcqRel);
        }
    }

    #[inline]
    fn increment_curr_counter(&self) {
        if self.mode.load(Acquire) {
            self.second.fetch_add(1, AcqRel);
        } else {
            self.first.fetch_add(1, AcqRel);
        }
    }

    fn toggle_mode(&self) -> bool {
        self.mode.fetch_xor(true, AcqRel)
    }
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            mode: self.mode.clone(),
            first: self.first.clone(),
            second: self.second.clone(),
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

        let prev_mode = self.reader.toggle_mode();
        self.wait(prev_mode);

        unsafe {
            self.data = Some(Box::from_raw(new_data));
        }
    }

    // TODO: What if a readers is stuck holding the pointer?
    #[inline]
    fn wait(&mut self, prev_mode: bool) {
        loop {
            let stale_pointers = self.reader.counter_count(prev_mode);
            if stale_pointers > MIN_STALE_POINTERS {
                thread::yield_now();
            } else {
                break;
            }
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

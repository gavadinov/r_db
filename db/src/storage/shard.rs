#![allow(dead_code)]

use super::types::{Key, Val};
use std::collections::HashMap;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::thread;

type Map = HashMap<Key, Val>;

/// A lock-free* concurrent hash map that will store the data for a single database shard.
/// It is backed by the std::collections::HashMap which, after Rust 1.36, is a port of
/// Google's SwissTable so we get that sweet SIMD lookup performance.
///
/// Heavily optimized for reads - reads will never block, writes are behind a Mutex.
/// Instead of using a reader-writer lock which will block the reads while writing, the Shard keeps
/// 2 maps behind atomic pointers. The readers read from one and the writers write to the other one.
/// After a write the two pointers are swapped and the write is replayed to she stale map.
///
/// The main difficulty is keeping track of all readers that have already dereferenced a pointer to the other map.
/// To solve this every Reader increments an Atomic counter when it dereferences the pointer -> reads -> decrements the counter.
/// The writer swaps the two pointers and then waits for the counter for the swapped map to get to 0.
/// Then it knows that there is no one else using the map and the writes can be applied.
///
/// Important disadvantage is that all data is stored twice. 'Tis the cost of performance.
pub struct Shard {
    id: usize,
    reader: Reader,
    writer: Arc<Mutex<Writer>>,
}

pub struct Reader {
    data: Arc<AtomicPtr<Map>>,
    // false: first, true: second
    mode: Arc<AtomicBool>,
    first: Arc<AtomicUsize>,
    second: Arc<AtomicUsize>,
}

pub struct Writer {
    data: Option<Box<Map>>,
    reader: Reader,
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
    fn with_data(data: Map) -> Self {
        Self {
            data: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(data)))),
            mode: Arc::new(AtomicBool::new(false)),
            first: Arc::new(AtomicUsize::new(0)),
            second: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get(&self, key: &Key) -> Option<Val> {
        let result;

        let mode = self.mode.load(Acquire);
        self.increment_counter(mode);
        result = self.data().get(key);
        self.decrement_counter(mode);

        match result {
            // TODO: This clone means an extra allocation for every read.
            Some(r) => Some(r.clone()),
            None => None,
        }
    }

    #[inline]
    fn data(&self) -> &Map {
        // Unwrap should never panic because self.r is always valid
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
    fn decrement_counter(&self, mode: bool) {
        if mode {
            self.second.fetch_sub(1, AcqRel);
        } else {
            self.first.fetch_sub(1, AcqRel);
        }
    }

    #[inline]
    fn increment_counter(&self, mode: bool) {
        if mode {
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
    pub fn new(reader: Reader) -> Self {
        Self {
            data: Some(Box::new(HashMap::new())),
            reader,
        }
    }

    fn with_data(reader: Reader, data: Map) -> Self {
        Self {
            data: Some(Box::new(data)),
            reader,
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

    // TODO: What if a reader is stuck holding the pointer?
    #[inline]
    fn wait(&mut self, prev_mode: bool) {
        loop {
            let stale_pointers = self.reader.counter_count(prev_mode);
            if stale_pointers > 0 {
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
    pub fn new(id: usize) -> Self {
        let reader = Reader::new();
        let writer = Writer::new(reader.clone());

        Self {
            id,
            writer: Arc::new(Mutex::new(writer)),
            reader,
        }
    }

    /// When autoscaling a shard will have to be moved to a different server. This constructor
    /// will import the existing data in a more performant way.
    pub fn with_data(id: usize, data: Map) -> Self {
        let mut reader_data = HashMap::with_capacity(data.len());
        data.iter().for_each(|(key, value)| {
            reader_data.insert(key.clone(), value.clone());
        });

        let reader = Reader::with_data(reader_data);
        let writer = Writer::with_data(reader.clone(), data);

        Self {
            id,
            writer: Arc::new(Mutex::new(writer)),
            reader,
        }
    }

    pub fn writer(&self) -> Arc<Mutex<Writer>> {
        self.writer.clone()
    }

    pub fn reader(&self) -> Reader {
        self.reader.clone()
    }

    pub fn id(&self) -> usize {
        self.id
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        use std::ptr;

        // Free the raw pointers in the Reader.
        // This is safe because by the time we drop the Shard all references to the Reader are already dropped.
        unsafe { Box::from_raw(self.reader.data.swap(ptr::null_mut(), Release)) };
    }
}

#[cfg(test)]
mod tests {
    use super::Shard;
    use std::collections::HashMap;
    use std::thread;

    #[test]
    fn test_with_data() {
        let mut data = HashMap::new();
        for i in 0..10 {
            data.insert(i.to_string(), i.to_string());
        }

        let mut s = Shard::with_data(42, data);
        let r = s.reader();

        for i in 0..10 {
            assert_eq!(r.get(&i.to_string()), Some(i.to_string()));
        }

        let w = s.writer();
        let mut w = w.lock().unwrap();
        w.put("1".to_string(), "2".to_string());
        assert_eq!(r.get(&"1".to_string()), Some("2".to_string()));
        // Check that after the swap all the data is still there
        assert_eq!(r.get(&"0".to_string()), Some("0".to_string()));
    }

    #[test]
    fn test_basic() {
        let mut s = Shard::new(42);
        let r = s.reader();
        assert_eq!(r.get(&"1".to_string()), None);
        let w = s.writer();
        let mut w = w.lock().unwrap();
        w.put("1".to_string(), "2".to_string());
        assert_eq!(r.get(&"1".to_string()), Some("2".to_string()));
        w.put("1".to_string(), "3".to_string());
        assert_eq!(r.get(&"1".to_string()), Some("3".to_string()));
        w.delete(&"1".to_string());
        assert_eq!(r.get(&"1".to_string()), None);
    }

    #[test]
    fn test_very_busy() {
        let mut s = Shard::new(42);
        let n = 255 as u8;
        let readers: Vec<_> = (0..6)
            .map(|_| {
                let r = s.reader();
                thread::spawn(move || {
                    let mut i = 0;
                    while i < n {
                        match r.get(&i.to_string()) {
                            Some(val) => {
                                assert_eq!(val, i.to_string());
                                i += 1;
                            }
                            None => thread::yield_now(),
                        }
                    }
                })
            })
            .collect();

        let writers: Vec<_> = (0..4)
            .map(|_| {
                let lock = s.writer();
                thread::spawn(move || {
                    for i in 0..n {
                        let mut w = lock.lock().unwrap();
                        w.put(i.to_string(), i.to_string());
                    }
                })
            })
            .collect();

        for handle in readers {
            handle.join().unwrap();
        }

        for handle in writers {
            handle.join().unwrap();
        }
    }
}

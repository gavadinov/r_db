use std::collections::HashMap;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::sync::{Arc, Mutex};

type Key = Vec<u8>;
type Val = Vec<u8>;
type Map = HashMap<Key, Val>;

/// A lock-free* concurrent hash map that will store the data for a single database shard.
///
/// Heavily optimized for reads. Reads will never block. Writes are behind a Mutex.
/// Instead of using a reader-writer lock which will block the reads while writing the Shard keeps
/// 2 maps behind atomic pointers. The readers read from one and the writers write to the other one.
/// After a write the two pointers are swapped and the write is replayed to she stale map.
///
/// The main difficulty is keeping track of all readers that have already dereferenced a pointer to the map.
/// To solve this every Reader increments an Atomic counter when it dereferences the pointer -> reads -> decrements the counter.
/// The writer swaps the two pointer and then waits for the counter for the swapped map to get to 0.
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
    counter: Arc<AtomicPtr<AtomicUsize>>,
}

pub struct Writer {
    data: Option<Box<Map>>,
    reader: Reader,
}

struct CounterHandle<'a> {
    counter: &'a mut AtomicUsize,
}

impl<'a> CounterHandle<'a> {
    fn new(counter: &'a mut AtomicUsize) -> Self {
        // Possible data race between loading the pointer and incrementing the counter :?
        counter.fetch_add(1, AcqRel);
        Self { counter }
    }
}

impl<'a> Drop for CounterHandle<'a> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, AcqRel);
    }
}

impl Reader {
    fn new() -> Self {
        Self {
            data: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(HashMap::new())))),
            counter: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(AtomicUsize::new(0))))),
        }
    }

    fn with_data(data: Map) -> Self {
        Self {
            data: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(data)))),
            counter: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(AtomicUsize::new(0))))),
        }
    }

    pub fn get(&self, key: &Key) -> Option<&Val> {
        let _handle = CounterHandle::new(unsafe { self.counter.load(Acquire).as_mut().unwrap() });
        self.data().get(key)
    }

    #[inline]
    fn data(&self) -> &Map {
        // Unwrap should never panic because self.r is always valid
        unsafe { self.data.load(Acquire).as_ref().unwrap() }
    }
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            counter: self.counter.clone(),
        }
    }
}

impl Writer {
    fn new(reader: Reader) -> Self {
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

        // The map has changed
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
        // so it can be easily swapped with None and later put back in
        let new_data = self.reader.data.swap(Box::into_raw(data), AcqRel);

        let new_counter = Box::into_raw(Box::new(AtomicUsize::new(0)));
        let counter: *mut AtomicUsize = self.reader.counter.swap(new_counter, AcqRel);

        // Stick it in a Box so we don't leak memory
        self.wait(unsafe { Box::from_raw(counter) });

        unsafe {
            self.data = Some(Box::from_raw(new_data));
        }
    }

    #[inline]
    #[allow(clippy::boxed_local)]
    fn wait(&mut self, counter: Box<AtomicUsize>) {
        use std::thread;
        loop {
            let stale_pointers = counter.load(Acquire);
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

    pub fn writer(&mut self) -> Arc<Mutex<Writer>> {
        self.writer.clone()
    }

    pub fn reader(&self) -> Reader {
        self.reader.clone()
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        use std::ptr;
        // Free the raw pointers in the Reader.
        // This is safe because by the time we drop the Shard all references to the Reader are already dropped.
        unsafe { Box::from_raw(self.reader.data.swap(ptr::null_mut(), Release)) };
        unsafe { Box::from_raw(self.reader.counter.swap(ptr::null_mut(), Release)) };
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::Shard;
    use std::collections::HashMap;
    use std::thread;

    #[test]
    fn test_with_data() {
        let mut data = HashMap::new();
        for i in 0..10 {
            data.insert(vec![i as u8], vec![i as u8]);
        }

        let mut s = Shard::with_data(42, data);
        let r = s.reader();

        for i in 0..10 {
            assert_eq!(r.get(&vec![i as u8]), Some(&vec![i as u8]));
        }

        let w = s.writer();
        let mut w = w.lock().unwrap();
        w.put(vec![1], vec![2]);
        assert_eq!(r.get(&vec![1 as u8]), Some(&vec![2 as u8]));
        // Check that after the swap all the data is still there
        assert_eq!(r.get(&vec![0 as u8]), Some(&vec![0 as u8]));
    }

    #[test]
    fn test_basic() {
        let mut s = Shard::new(42);
        let r = s.reader();
        assert_eq!(r.get(&vec![1 as u8]), None);
        let w = s.writer();
        let mut w = w.lock().unwrap();
        w.put(vec![1], vec![2]);
        assert_eq!(r.get(&vec![1 as u8]), Some(&vec![2 as u8]));
        w.put(vec![1], vec![3]);
        assert_eq!(r.get(&vec![1 as u8]), Some(&vec![3 as u8]));
        w.delete(&vec![1 as u8]);
        assert_eq!(r.get(&vec![1 as u8]), None);
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
                        match r.get(&vec![i]) {
                            Some(val) => {
                                assert_eq!(val, &vec![i]);
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
                        w.put(vec![i], vec![i]);
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

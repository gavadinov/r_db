use std::borrow::Borrow;
use std::collections::HashMap;
use std::mem::swap;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

type Key = Vec<u8>;
type Val = Vec<u8>;
type Map = HashMap<Key, Val>;

// TODO: Split into write and reader. The writer should always be one. It will need to be behind a mutex?
// TODO: Figure out how to wait for all readers to finish with the old pointer after swap. Release the mutex after that.
// TODO: Check the smallvec crate
// TODO: It is super optimized for reads. What about creaing a new shard and copying data from the old one. It's a huge burst of writes.

pub struct Shard {
    id: u64,
    r: AtomicPtr<Map>,
    w: Option<Box<Map>>,
}

impl Shard {
    pub fn new(id: u64) -> Self {
        Shard {
            id,
            r: AtomicPtr::new(Box::into_raw(Box::new(HashMap::new()))),
            w: Some(Box::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &Key) -> Option<&Val> {
        self.reader().get(key)
    }

    // TODO: Fix the code duplication with delete
    pub fn put(&mut self, key: Key, value: Val) -> Option<Val> {
        let mut writer = self.writer();
        writer.insert(key.clone(), value.clone());

        self.swap(writer);

        // Writer has changed
        let mut writer = self.writer();
        let result = writer.insert(key, value);
        self.w = Some(writer);

        result
    }

    // TODO: Fix the code duplication with put
    pub fn delete(&mut self, key: &Key) -> Option<Val> {
        let mut writer = self.writer();
        writer.remove(key);

        self.swap(writer);

        // Writer has changed
        let mut writer = self.writer();
        let result = writer.remove(key);
        self.w = Some(writer);

        result
    }

    fn swap(&mut self, writer: Box<Map>) {
        // Because Box::into_raw consumes the Box we have to keep the self.w in an Option
        // so it can be swapped with None and then put back in
        let new_w = self.r.swap(Box::into_raw(writer), Release);
        unsafe {
            self.w = Some(Box::from_raw(new_w));
        }
    }

    // Will leave a None in place of self.w
    #[inline]
    fn writer(&mut self) -> Box<Map> {
        self.w.take().unwrap()
    }

    #[inline]
    fn reader(&self) -> &Map {
        // Unwrap should never panic because self.r is always initialized
        unsafe { self.r.load(Acquire).as_ref().unwrap() }
    }
}

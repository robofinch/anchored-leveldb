use std::fmt::Debug;

use clone_behavior::{MirroredClone, Speed};

use super::KVCache;


#[derive(Default, Debug, Clone, Copy)]
pub struct NoCache;

impl<S: Speed> MirroredClone<S> for NoCache {
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

impl<Key, Value> KVCache<Key, Value> for NoCache {
    type CacheAsDebug = Self where Key: Debug, Value: Debug;

    fn insert(&self, _cache_key: Key, _value: &Value) {}

    fn get(&self, _cache_key: &Key) -> Option<Value> {
        None
    }

    fn debug(&self) -> &Self::CacheAsDebug
    where
        Key:   Debug,
        Value: Debug,
    {
        self
    }
}

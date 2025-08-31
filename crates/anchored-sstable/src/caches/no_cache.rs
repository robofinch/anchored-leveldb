use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{MirroredClone, Speed};

use super::KVCache;


#[derive(Debug, Clone, Copy)]
pub enum NoCache {}

#[expect(clippy::uninhabited_references, reason = "this code is unreachable")]
impl<S: Speed> MirroredClone<S> for NoCache {
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

#[expect(clippy::uninhabited_references, reason = "this code is unreachable")]
impl<Key, Value> KVCache<Key, Value> for NoCache {
    fn insert(&self, _cache_key: Key, _value: &Value) {}

    fn get(&self, _cache_key: &Key) -> Option<Value> {
        match *self {}
    }

    fn debug(&self, _f: &mut Formatter<'_>) -> FmtResult {
        match *self {}
    }
}

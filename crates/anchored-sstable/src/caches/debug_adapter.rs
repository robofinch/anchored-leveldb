#![expect(unsafe_code, reason = "Re-add Send and Sync impls removed by PhantomData")]

use std::marker::PhantomData;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{MirroredClone, Speed};

use super::KVCache;


#[derive(Clone)]
pub struct CacheDebugAdapter<Cache, Key, Value> {
    cache:   Cache,
    _marker: PhantomData<(Key, Value)>
}

impl<Cache, Key, Value> CacheDebugAdapter<Cache, Key, Value> {
    #[inline]
    #[must_use]
    pub const fn new(cache: Cache) -> Self {
        Self {
            cache,
            _marker: PhantomData,
        }
    }
}

impl<Cache, Key, Value> CacheDebugAdapter<Cache, Key, Value>
where
    Cache: KVCache<Key, Value>,
{
    #[inline]
    pub fn insert(&self, cache_key: Key, value: &Value) {
        self.cache.insert(cache_key, value);
    }

    #[inline]
    #[must_use]
    pub fn get(&self, cache_key: &Key) -> Option<Value> {
        self.cache.get(cache_key)
    }
}

impl<Cache, Key, Value> Debug for CacheDebugAdapter<Cache, Key, Value>
where
    Cache: KVCache<Key, Value>,
    Key:   Debug,
    Value: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.cache.debug(f)
    }
}

impl<S, Cache, Key, Value> MirroredClone<S> for CacheDebugAdapter<Cache, Key, Value>
where
    S:     Speed,
    Cache: MirroredClone<S>
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            cache:   self.cache.mirrored_clone(),
            _marker: PhantomData,
        }
    }
}

// Safety: we only store `Cache`; `Key` and `Value` are only inside `PhantomData`
unsafe impl<Cache, Key, Value> Send for CacheDebugAdapter<Cache, Key, Value>
where
    Cache: Send,
{}

// Safety: we only store `Cache`; `Key` and `Value` are only inside `PhantomData`
unsafe impl<Cache, Key, Value> Sync for CacheDebugAdapter<Cache, Key, Value>
where
    Cache: Sync,
{}

use std::marker::PhantomData;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{MirroredClone, Speed};

use super::{CacheKey, TableBlockCache};


#[derive(Clone)]
pub struct CacheDebugAdapter<Cache, BlockContents> {
    cache:   Cache,
    _marker: PhantomData<BlockContents>
}

impl<Cache, BlockContents> CacheDebugAdapter<Cache, BlockContents> {
    #[inline]
    #[must_use]
    pub const fn new(cache: Cache) -> Self {
        Self {
            cache,
            _marker: PhantomData,
        }
    }
}

impl<Cache, BlockContents> CacheDebugAdapter<Cache, BlockContents>
where
    Cache: TableBlockCache<BlockContents>,
{
    #[inline]
    pub fn insert(&self, cache_key: CacheKey, block: &BlockContents) {
        self.cache.insert(cache_key, block);
    }

    #[inline]
    #[must_use]
    pub fn get(&self, cache_key: &CacheKey) -> Option<BlockContents> {
        self.cache.get(cache_key)
    }
}

impl<Cache, BlockContents> Debug for CacheDebugAdapter<Cache, BlockContents>
where
    Cache:         TableBlockCache<BlockContents>,
    BlockContents: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.cache.debug(f)
    }
}

impl<S, Cache, BlockContents> MirroredClone<S>
for CacheDebugAdapter<Cache, BlockContents>
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

// Safety: we only store `Cache`; `BlockContents` is only inside `PhantomData`
unsafe impl<Cache, BlockContents> Send for CacheDebugAdapter<Cache, BlockContents>
where
    Cache: Send,
{}

// Safety: we only store `Cache`; `BlockContents` is only inside `PhantomData`
unsafe impl<Cache, BlockContents> Sync for CacheDebugAdapter<Cache, BlockContents>
where
    Cache: Sync,
{}

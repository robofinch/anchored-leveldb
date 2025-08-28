use std::marker::PhantomData;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{MirroredClone, Speed};

use crate::block::TableBlock;
use super::{CacheKey, TableBlockCache};


#[derive(Clone)]
pub struct CacheDebugAdapter<Cache, BlockContents, TableCmp> {
    cache:   Cache,
    _marker: PhantomData<(BlockContents, TableCmp)>
}

impl<Cache, BlockContents, TableCmp> CacheDebugAdapter<Cache, BlockContents, TableCmp> {
    #[inline]
    #[must_use]
    pub const fn new(cache: Cache) -> Self {
        Self {
            cache,
            _marker: PhantomData,
        }
    }
}

impl<Cache, BlockContents, TableCmp> CacheDebugAdapter<Cache, BlockContents, TableCmp>
where
    Cache: TableBlockCache<BlockContents, TableCmp>,
{
    #[inline]
    pub fn insert(&self, cache_key: CacheKey, block: &TableBlock<BlockContents, TableCmp>) {
        self.cache.insert(cache_key, block);
    }

    #[inline]
    #[must_use]
    pub fn get(&self, cache_key: &CacheKey) -> Option<TableBlock<BlockContents, TableCmp>> {
        self.cache.get(cache_key)
    }
}

impl<Cache, BlockContents, TableCmp> Debug for CacheDebugAdapter<Cache, BlockContents, TableCmp>
where
    Cache:         TableBlockCache<BlockContents, TableCmp>,
    BlockContents: Debug,
    TableCmp:      Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.cache.debug(f)
    }
}

impl<S, Cache, BlockContents, TableCmp> MirroredClone<S>
for CacheDebugAdapter<Cache, BlockContents, TableCmp>
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

// Safety: we only store `Cache`; `BlockContents` and `TableCmp` are only inside `PhantomData`
unsafe impl<Cache, BlockContents, TableCmp> Send
for CacheDebugAdapter<Cache, BlockContents, TableCmp>
where
    Cache: Send,
{}

// Safety: we only store `Cache`; `BlockContents` and `TableCmp` are only inside `PhantomData`
unsafe impl<Cache, BlockContents, TableCmp> Sync
for CacheDebugAdapter<Cache, BlockContents, TableCmp>
where
    Cache: Sync,
{}

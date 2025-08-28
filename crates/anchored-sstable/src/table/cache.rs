#![expect(unsafe_code, reason = "Re-add Send and Sync impls removed by PhantomData")]

use std::marker::PhantomData;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone, Speed};

use crate::block::TableBlock;


#[expect(
    clippy::field_scoped_visibility_modifiers,
    reason = "P.O.D. struct, but don't want to expose implementation details",
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CacheKey {
    pub(super) table_id:      u64,
    pub(super) handle_offset: u64,
}

pub trait TableBlockCache<BlockContents, TableCmp>: MirroredClone<ConstantTime> {
    fn insert(&self, cache_key: CacheKey, block: &TableBlock<BlockContents, TableCmp>);

    #[must_use]
    fn get(&self, cache_key: &CacheKey) -> Option<TableBlock<BlockContents, TableCmp>>;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    ///
    /// Usage of this trait should be paired with [`CacheDebugAdapter`].
    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        BlockContents: Debug,
        TableCmp:      Debug;
}

#[derive(Debug, Clone, Copy)]
pub enum NoCache {}

#[expect(clippy::uninhabited_references, reason = "this code is unreachable")]
impl<S: Speed> MirroredClone<S> for NoCache {
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

#[expect(clippy::uninhabited_references, reason = "this code is unreachable")]
impl<BlockContents, TableCmp> TableBlockCache<BlockContents, TableCmp> for NoCache {
    fn insert(&self, _cache_key: CacheKey, _block: &TableBlock<BlockContents, TableCmp>) {}

    fn get(&self, _cache_key: &CacheKey) -> Option<TableBlock<BlockContents, TableCmp>> {
        match *self {}
    }

    fn debug(&self, _f: &mut Formatter<'_>) -> FmtResult {
        match *self {}
    }
}

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

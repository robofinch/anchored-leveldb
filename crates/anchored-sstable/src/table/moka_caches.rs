use std::{cell::RefCell, rc::Rc};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone, Speed};
use mini_moka::unsync::Cache as UnsyncMokaCache;
use moka::sync::Cache as SyncMokaCache;

use crate::block::TableBlock;
use super::cache::{CacheKey, TableBlockCache};


#[derive(Debug)]
pub struct UnsyncCache<BlockContents, TableCmp>(
    pub Rc<RefCell<UnsyncMokaCache<
        CacheKey,
        TableBlock<BlockContents, TableCmp>,
    >>>,
);

impl<BlockContents, TableCmp> TableBlockCache<BlockContents, TableCmp>
for UnsyncCache<BlockContents, TableCmp>
where
    BlockContents: MirroredClone<ConstantTime>,
    TableCmp:      MirroredClone<ConstantTime>,
{
    #[inline]
    fn insert(&self, cache_key: CacheKey, block: &TableBlock<BlockContents, TableCmp>) {
        self.0.borrow_mut()
            .insert(cache_key, block.mirrored_clone());
    }

    #[inline]
    fn get(&self, cache_key: &CacheKey) -> Option<TableBlock<BlockContents, TableCmp>> {
        self.0.borrow_mut()
            .get(cache_key)
            .map(TableBlock::mirrored_clone)
    }

    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        BlockContents: Debug,
        TableCmp:      Debug,
    {
        Debug::fmt(&self, f)
    }
}

impl<BlockContents, TableCmp> Clone for UnsyncCache<BlockContents, TableCmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<BlockContents, TableCmp, S: Speed> MirroredClone<S> for UnsyncCache<BlockContents, TableCmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

/// The `BlockContents` generic should use an [`Arc`] or similar; the `Clone` implementations
/// of both `BlockContents` and `TableCmp` should be constant-time operations.
///
/// [`Arc`]: std::sync::Arc
pub struct SyncCache<BlockContents, TableCmp>(
    pub SyncMokaCache<
        CacheKey,
        TableBlock<BlockContents, TableCmp>,
    >,
);

impl<BlockContents, TableCmp> TableBlockCache<BlockContents, TableCmp>
for SyncCache<BlockContents, TableCmp>
where
    BlockContents: MirroredClone<ConstantTime> + Clone + Send + Sync + 'static,
    TableCmp:      MirroredClone<ConstantTime> + Clone + Send + Sync + 'static,
{
    #[inline]
    fn insert(&self, cache_key: CacheKey, block: &TableBlock<BlockContents, TableCmp>) {
        self.0.insert(cache_key, block.mirrored_clone());
    }

    #[inline]
    fn get(&self, cache_key: &CacheKey) -> Option<TableBlock<BlockContents, TableCmp>> {
        self.0.get(cache_key)
    }

    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        BlockContents: Debug,
        TableCmp:      Debug,
    {
        self.fmt(f)
    }
}

impl<BlockContents, TableCmp> Clone for SyncCache<BlockContents, TableCmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<BlockContents, TableCmp, S: Speed> MirroredClone<S> for SyncCache<BlockContents, TableCmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        // `self.clone()` performs 5 atomic operations. Also, the struct is less than 64 bytes.
        self.clone()
    }
}

impl<BlockContents, TableCmp> Debug for SyncCache<BlockContents, TableCmp>
where
    BlockContents: Debug + Clone + Send + Sync + 'static,
    TableCmp:      Debug + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("SyncCache").field(&self.0).finish()
    }
}

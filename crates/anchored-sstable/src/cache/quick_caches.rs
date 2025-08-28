use std::{cell::RefCell, rc::Rc, sync::Arc};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone, Speed};
use quick_cache::{sync::Cache as SyncCache, unsync::Cache as UnsyncCache};

use crate::block::TableBlock;
use super::{CacheKey, TableBlockCache};


#[derive(Debug)]
pub struct UnsyncQuickCache<BlockContents, TableCmp>(
    pub Rc<RefCell<UnsyncCache<
        CacheKey,
        TableBlock<BlockContents, TableCmp>,
    >>>,
);

impl<BlockContents, TableCmp> TableBlockCache<BlockContents, TableCmp>
for UnsyncQuickCache<BlockContents, TableCmp>
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

impl<BlockContents, TableCmp> Clone for UnsyncQuickCache<BlockContents, TableCmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<BlockContents, TableCmp, S: Speed> MirroredClone<S>
for UnsyncQuickCache<BlockContents, TableCmp>
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

/// The `BlockContents` generic should use an [`Arc`] or similar; the `Clone` implementations
/// of both `BlockContents` and `TableCmp` should be constant-time operations.
///
/// [`Arc`]: std::sync::Arc
pub struct SyncQuickCache<BlockContents, TableCmp>(
    pub Arc<SyncCache<
        CacheKey,
        TableBlock<BlockContents, TableCmp>,
    >>,
);

impl<BlockContents, TableCmp> TableBlockCache<BlockContents, TableCmp>
for SyncQuickCache<BlockContents, TableCmp>
where
    BlockContents: MirroredClone<ConstantTime> + Clone,
    TableCmp:      MirroredClone<ConstantTime> + Clone,
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
        Debug::fmt(&self, f)
    }
}

impl<BlockContents, TableCmp> Clone for SyncQuickCache<BlockContents, TableCmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<BlockContents, TableCmp, S: Speed> MirroredClone<S>
for SyncQuickCache<BlockContents, TableCmp>
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        // cloning an `Arc` is cheap
        self.clone()
    }
}

impl<BlockContents, TableCmp> Debug for SyncQuickCache<BlockContents, TableCmp> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("SyncMokaCache").field(&self.0).finish()
    }
}

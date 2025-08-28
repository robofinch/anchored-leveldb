use std::{cell::RefCell, rc::Rc, sync::Arc};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone, Speed};
use quick_cache::{sync::Cache as SyncCache, unsync::Cache as UnsyncCache};

use super::{CacheKey, TableBlockCache};


#[derive(Debug)]
pub struct UnsyncQuickCache<BlockContents>(
    pub Rc<RefCell<UnsyncCache<CacheKey, BlockContents>>>,
);

impl<BlockContents> TableBlockCache<BlockContents> for UnsyncQuickCache<BlockContents>
where
    BlockContents: MirroredClone<ConstantTime>,
{
    #[inline]
    fn insert(&self, cache_key: CacheKey, block: &BlockContents) {
        self.0.borrow_mut()
            .insert(cache_key, block.mirrored_clone());
    }

    #[inline]
    fn get(&self, cache_key: &CacheKey) -> Option<BlockContents> {
        self.0.borrow_mut()
            .get(cache_key)
            .map(BlockContents::mirrored_clone)
    }

    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        BlockContents: Debug,
    {
        Debug::fmt(&self, f)
    }
}

impl<BlockContents> Clone for UnsyncQuickCache<BlockContents> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<BlockContents, S: Speed> MirroredClone<S> for UnsyncQuickCache<BlockContents> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

/// The `BlockContents` generic should use an [`Arc`] or similar; the `Clone` implementations
/// of both `BlockContents` and `TableCmp` should be constant-time operations.
///
/// [`Arc`]: std::sync::Arc
pub struct SyncQuickCache<BlockContents>(pub Arc<SyncCache<CacheKey, BlockContents>>);

impl<BlockContents> TableBlockCache<BlockContents> for SyncQuickCache<BlockContents>
where
    BlockContents: MirroredClone<ConstantTime> + Clone,
{
    #[inline]
    fn insert(&self, cache_key: CacheKey, block: &BlockContents) {
        self.0.insert(cache_key, block.mirrored_clone());
    }

    #[inline]
    fn get(&self, cache_key: &CacheKey) -> Option<BlockContents> {
        self.0.get(cache_key)
    }

    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        BlockContents: Debug,
    {
        Debug::fmt(&self, f)
    }
}

impl<BlockContents> Clone for SyncQuickCache<BlockContents> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<BlockContents, S: Speed> MirroredClone<S> for SyncQuickCache<BlockContents> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        // cloning an `Arc` is cheap
        self.clone()
    }
}

impl<BlockContents> Debug for SyncQuickCache<BlockContents> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("SyncMokaCache").field(&self.0).finish()
    }
}

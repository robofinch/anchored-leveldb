use std::{cell::RefCell, rc::Rc};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone, Speed};
use mini_moka::unsync::Cache as UnsyncCache;
use moka::sync::Cache as SyncCache;

use super::{CacheKey, TableBlockCache};


#[derive(Debug)]
pub struct UnsyncMokaCache<BlockContents>(
    pub Rc<RefCell<UnsyncCache<CacheKey, BlockContents>>>,
);

impl<BlockContents> TableBlockCache<BlockContents> for UnsyncMokaCache<BlockContents>
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

impl<BlockContents> Clone for UnsyncMokaCache<BlockContents> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<BlockContents, S: Speed> MirroredClone<S> for UnsyncMokaCache<BlockContents> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

/// The `BlockContents` generic should use an [`Arc`] or similar; the `Clone` implementation
/// of `BlockContents` should be a constant-time operation.
///
/// [`Arc`]: std::sync::Arc
pub struct SyncMokaCache<BlockContents>(pub SyncCache<CacheKey, BlockContents>);

impl<BlockContents> TableBlockCache<BlockContents> for SyncMokaCache<BlockContents>
where
    BlockContents: MirroredClone<ConstantTime> + Clone + Send + Sync + 'static,
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

impl<BlockContents> Clone for SyncMokaCache<BlockContents> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<BlockContents, S: Speed> MirroredClone<S> for SyncMokaCache<BlockContents> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        // `self.clone()` performs 5 atomic operations. Also, the struct is less than 64 bytes.
        self.clone()
    }
}

impl<BlockContents> Debug for SyncMokaCache<BlockContents>
where
    BlockContents: Debug + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("SyncMokaCache").field(&self.0).finish()
    }
}

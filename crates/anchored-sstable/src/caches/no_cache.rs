use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{MirroredClone, Speed};

use super::{CacheKey, TableBlockCache};


#[derive(Debug, Clone, Copy)]
pub enum NoCache {}

#[expect(clippy::uninhabited_references, reason = "this code is unreachable")]
impl<S: Speed> MirroredClone<S> for NoCache {
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

#[expect(clippy::uninhabited_references, reason = "this code is unreachable")]
impl<BlockContents> TableBlockCache<BlockContents> for NoCache {
    fn insert(&self, _cache_key: CacheKey, _block: &BlockContents) {}

    fn get(&self, _cache_key: &CacheKey) -> Option<BlockContents> {
        match *self {}
    }

    fn debug(&self, _f: &mut Formatter<'_>) -> FmtResult {
        match *self {}
    }
}

#![expect(unsafe_code, reason = "Re-add Send and Sync impls removed by PhantomData")]

mod no_cache;
mod debug_adapter;
#[cfg(feature = "moka-caches")]
mod moka_caches;
#[cfg(feature = "quick-caches")]
mod quick_caches;


use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone};


pub use self::{debug_adapter::CacheDebugAdapter, no_cache::NoCache};
#[cfg(feature = "moka-caches")]
pub use self::moka_caches::{SyncMokaCache, UnsyncMokaCache};
#[cfg(feature = "quick-caches")]
pub use self::quick_caches::{SyncQuickCache, UnsyncQuickCache};


#[expect(
    clippy::field_scoped_visibility_modifiers,
    reason = "P.O.D. struct, but don't want to expose implementation details",
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CacheKey {
    pub(crate) table_id:      u64,
    pub(crate) handle_offset: u64,
}

pub trait TableBlockCache<BlockContents>: MirroredClone<ConstantTime> {
    fn insert(&self, cache_key: CacheKey, block: &BlockContents);

    #[must_use]
    fn get(&self, cache_key: &CacheKey) -> Option<BlockContents>;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    ///
    /// Usage of this trait should be paired with [`CacheDebugAdapter`].
    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult where BlockContents: Debug;
}

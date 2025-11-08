mod no_cache;
#[cfg(feature = "moka-caches")]
mod moka_caches;
#[cfg(feature = "quick-caches")]
mod quick_caches;

use std::fmt::Debug;

use clone_behavior::{Fast, MirroredClone};


pub use self::no_cache::NoCache;
#[cfg(feature = "moka-caches")]
pub use self::moka_caches::{SyncMokaCache, UnsyncMokaCache};
#[cfg(feature = "quick-caches")]
pub use self::quick_caches::{SyncQuickCache, UnsyncQuickCache};


#[expect(
    clippy::field_scoped_visibility_modifiers,
    reason = "P.O.D. struct, but don't want to expose implementation details",
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockCacheKey {
    pub(crate) table_file_number: u64,
    pub(crate) handle_offset:     u64,
}

pub trait KVCache<Key, Value>: MirroredClone<Fast> {
    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    type CacheAsDebug: Debug where Key: Debug, Value: Debug;

    fn insert(&self, cache_key: Key, value: &Value);

    #[must_use]
    fn get(&self, cache_key: &Key) -> Option<Value>;

    /// Workaround for the fact that a conditional trait bound, like "must implement `Debug`
    /// if `OtherType` implements `Debug`", is not currently possible in Rust.
    fn debug(&self) -> &Self::CacheAsDebug
    where
        Key:   Debug,
        Value: Debug;
}

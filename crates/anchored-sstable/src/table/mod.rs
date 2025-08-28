mod build;
mod read;
mod format;
mod iter;
mod cache;
mod table_struct;

#[cfg(feature = "moka-caches")]
mod moka_caches;
#[cfg(feature = "quick-caches")]
mod quick_caches;


#[expect(clippy::module_name_repetitions, reason = "clarity when used or reexported elsewhere")]
pub use self::{
    build::{TableBuilder, WriteTableOptions},
    cache::{CacheDebugAdapter, CacheKey, NoCache, TableBlockCache},
    format::{
        BLOCK_TRAILER_LEN, BlockHandle, FILTER_META_PREFIX,
        mask_checksum, TableFooter, unmask_checksum,
    },
    iter::{TableIter, TableIterPieces},
    read::TableBlockReader,
    table_struct::{ReadTableOptions, Table},
};

#[cfg(feature = "moka-caches")]
pub use self::moka_caches::{SyncMokaCache, UnsyncMokaCache};
#[cfg(feature = "quick-caches")]
pub use self::quick_caches::{SyncQuickCache, UnsyncQuickCache};

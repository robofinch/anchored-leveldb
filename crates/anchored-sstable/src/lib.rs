#![cfg_attr(docsrs, feature(doc_cfg))]

mod varint_utils;

mod comparator;
mod filters;
mod compressors;
mod internal_utils;

mod block;
mod filter_block;
mod caches;
mod pool;

mod table;

mod option_structs;
mod error;


// Below, these are all publicly exported from the crate root
mod core_features {
    pub use crate::{
        block::{Block, BlockBuilder, TableBlock},
        table::{Table, TableBuilder, TableEntry},
        option_structs::{ReadTableOptions, TableOptions, WriteTableOptions},
    };
}

pub mod options {
    pub use crate::{
        caches::{BlockCacheKey, KVCache, NoCache},
        comparator::{LexicographicComparator, TableComparator},
        compressors::{
            Compressor, CompressionError, CompressorID, CompressorList,
            DecompressionError, NoneCompressor,
            NO_COMPRESSION, SNAPPY_COMPRESSION, ZSTD_COMPRESSION,
        },
        filters::{
            BloomPolicy, BloomPolicyName, FILTER_KEY_LENGTH_LIMIT, FILTER_NUM_KEYS_LIMIT,
            NoFilterPolicy, TableFilterPolicy,
        },
        pool::BufferPool,
    };

    #[cfg(feature = "moka-caches")]
    #[cfg_attr(docsrs, doc(cfg(feature = "moka-caches")))]
    pub use crate::caches::{SyncMokaCache, UnsyncMokaCache};

    #[cfg(feature = "quick-caches")]
    #[cfg_attr(docsrs, doc(cfg(feature = "quick-caches")))]
    pub use crate::caches::{SyncQuickCache, UnsyncQuickCache};

    #[cfg(feature = "snappy-compressor")]
    #[cfg_attr(docsrs, doc(cfg(feature = "snappy-compressor")))]
    pub use crate::compressors::SnappyCompressor;

    #[cfg(feature = "zstd-compressor")]
    #[cfg_attr(docsrs, doc(cfg(feature = "zstd-compressor")))]
    pub use crate::compressors::ZstdCompressor;
}

pub mod iter {
    #[expect(clippy::module_name_repetitions, reason = "clarity")]
    pub use crate::{
        block::{
            BlockIterImpl, BlockIterImplPieces,
            BorrowedBlockIter, OwnedBlockIter, OwnedBlockIterPieces,
        },
        table::{TableIter, TableIterPieces},
    };
}

pub mod adapters {
    pub use crate::{caches::CacheDebugAdapter, comparator::ComparatorAdapter};
}

pub mod table_format {
    pub use crate::{
        comparator::MetaindexComparator,
        filter_block::{FilterBlockBuilder, FilterBlockReader},
        table::{
            BLOCK_TRAILER_LEN, BlockHandle, FILTER_META_PREFIX,
            mask_checksum, TableBlockReader, TableFooter, unmask_checksum,
        },
    };
}


pub use self::core_features::*;


// getrandom is unused directly within this crate, but used as a recursive dependency via:
// sorted_vector_map -> quickcheck -> rand -> rand_core -> getrandom 0.2.x
// moka -> uuid -> getrandom 0.3.x
// This silences an "unused dependency" lint.
#[cfg(target_family = "wasm")]
use getrandom2 as _;
#[cfg(target_family = "wasm")]
use getrandom3 as _;
#[cfg(target_family = "wasm")]
use uuid as _;

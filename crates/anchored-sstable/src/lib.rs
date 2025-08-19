#![cfg_attr(docsrs, feature(doc_cfg))]

mod comparator;
mod filter;
// Reason this is pub: there's a bunch of constants and traits that will not usually be needed.
// They need to be public, but need not be in the crate root.
pub mod compressors;
mod utils;

mod block;
mod filter_block;

// Temporarily public, to silence errors.
pub mod table;


pub use self::block::{
    Block, BlockBuilder, BlockContentsContainer,
    BlockIterImpl, BlockIterImplPieces, BorrowedBlockIter, OwnedBlockIter, OwnedBlockIterPieces,
    TableBlock,
};
pub use self::comparator::{
    ComparatorAdapter, DefaultComparator, DefaultComparatorID, MetaindexComparator, TableComparator,
};
pub use self::compressors::{Compressor, CompressorList};
pub use self::filter::{
    BloomPolicy, BloomPolicyName, FILTER_KEYS_LENGTH_LIMIT, FilterPolicy, NoFilterPolicy,
};
pub use self::filter_block::{FilterBlockBuilder, FilterBlockReader};


// TODO: provide functions that can rigorously validate the data of blocks, filter blocks,
// etc, so that I don't feel guilty about letting the normal implementations panic.
// Someone who's concerned about corruption can do the paranoid checks.


// getrandom is unused directly within this crate, but used as a recursive dependency via:
// sorted_vector_map -> quickcheck -> rand -> rand_core -> getrandom 0.2.x
// moka -> uuid -> getrandom 0.3.x
// This silences an "unused dependency" lint.
#[cfg(target_family = "wasm")]
use getrandom2 as _;
#[cfg(target_family = "wasm")]
use uuid as _;

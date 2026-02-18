#![no_std]
#![warn(clippy::std_instead_of_alloc, clippy::std_instead_of_core, clippy::alloc_instead_of_core)]

extern crate alloc;


mod interface;
mod maybe_loom;

mod raw_skiplist;
mod skiplist;


pub use self::{
    interface::{Comparator, EncodeWith, Entry, Key, SkiplistFormat},
    raw_skiplist::{
        AllocErr, RawSkiplist,
        RawSkiplistIterState, RawSkiplistIterView, RawSkiplistIterViewMut,
    },
    skiplist::{Skiplist, SkiplistIter, SkiplistLendingIter, SkiplistReader, TryResetError},
};

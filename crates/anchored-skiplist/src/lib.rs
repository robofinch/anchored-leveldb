#![cfg_attr(test, allow(unused_crate_dependencies, reason = "`generic_container` is unused"))]

// TODO: somewhere, document the time complexity of various operations

mod interface;
pub mod iter_defaults;

mod single_threaded;
mod threadsafe_impl;

mod node_heights;


mod maybe_loom;


pub mod simple {
    #[expect(clippy::module_name_repetitions, reason = "distinguish skiplist type")]
    pub use crate::single_threaded::simple::{Iter, LendingIter, SimpleSkiplist};
}

pub mod concurrent {
    #[expect(clippy::module_name_repetitions, reason = "distinguish skiplist type")]
    pub use crate::single_threaded::concurrent::{Iter, LendingIter, ConcurrentSkiplist};
}

pub mod threadsafe {
    #[expect(clippy::module_name_repetitions, reason = "distinguish skiplist type")]
    pub use crate::threadsafe_impl::pub_structs::{
        Iter, LendingIter, LockedIter, LockedLendingIter,
        LockedThreadsafeSkiplist, ThreadsafeSkiplist,
    };
}


pub use self::{
    concurrent::ConcurrentSkiplist,
    simple::SimpleSkiplist,
    threadsafe::ThreadsafeSkiplist,
};
pub use self::interface::Skiplist;

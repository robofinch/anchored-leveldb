#![expect(clippy::redundant_pub_crate, reason = "clarify what's internal and what's not")]
#![expect(
    clippy::multiple_crate_versions,
    reason = "latest `loom` -> latest `tracing-subscriber` \
              -> out-of-date `matchers` -> out-of-date regex-related deps",
)]

mod interface;
mod default_comparator;
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
    default_comparator::DefaultComparator,
    simple::SimpleSkiplist,
    threadsafe::ThreadsafeSkiplist,
};
pub use self::interface::{Comparator, Skiplist, SkiplistIterator, SkiplistLendingIterator};

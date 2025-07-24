#![expect(clippy::redundant_pub_crate, reason = "clarify what's internal and what's not")]

mod interface;
pub mod iter_defaults;

mod single_threaded;
mod threadsafe_impl;

mod node_heights;


pub mod simple {
    #[expect(clippy::module_name_repetitions, reason = "distinguish skiplist type")]
    pub use crate::single_threaded::simple::{Iter, LendingIter, SimpleSkiplist};
}

pub mod concurrent {
    #[expect(clippy::module_name_repetitions, reason = "distinguish skiplist type")]
    pub use crate::single_threaded::concurrent::{Iter, LendingIter, ConcurrentSkiplist};
}

pub mod threadsafe {

}


pub use self::{concurrent::ConcurrentSkiplist, simple::SimpleSkiplist};
pub use self::interface::{Comparator, Skiplist, SkiplistIterator, SkiplistLendingIterator};

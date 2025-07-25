#![allow(unexpected_cfgs, reason = "Extra cfg's are used by the Miri tests for this crate")]
#![allow(unused_crate_dependencies, reason = "these are tests, not the main crate")]

mod all;


use std::array;
use std::{cell::RefCell, cmp::Ordering, collections::BTreeSet, rc::Rc};

use generic_container::GenericContainer;
use oorandom::Rand32;

use anchored_skiplist::{
    Comparator, DefaultComparator, Skiplist, SkiplistIterator as _, SkiplistLendingIterator as _
};
use anchored_skiplist::concurrent::{Iter, LendingIter, ConcurrentSkiplist};


all::tests_for_all_skiplists!(ConcurrentSkiplist);

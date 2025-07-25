#![allow(unexpected_cfgs, reason = "Extra cfg's are used by the Miri tests for this crate.")]
#![allow(unused_crate_dependencies, reason = "These are tests, not the main crate.")]
#![allow(unused_imports, reason = "Depending on cfg, some are unused. Annoying to annotate.")]

mod all;


use std::array;
use std::{cell::RefCell, cmp::Ordering, collections::BTreeSet, rc::Rc};

use generic_container::GenericContainer;
use oorandom::Rand32;

use anchored_skiplist::{
    Comparator, DefaultComparator, Skiplist, SkiplistIterator as _, SkiplistLendingIterator as _
};
use anchored_skiplist::simple::{Iter, LendingIter, SimpleSkiplist};


all::tests_for_all_skiplists!(SimpleSkiplist);

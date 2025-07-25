#![allow(unexpected_cfgs, reason = "Extra cfg's are used by the Miri tests for this crate.")]
#![allow(unused_crate_dependencies, reason = "These are tests, not the main crate.")]
#![allow(unused_imports, reason = "Depending on cfg, some are unused. Annoying to annotate.")]

mod all;
mod reference_counted;


use std::array;
use std::{cell::RefCell, cmp::Ordering, collections::BTreeSet, rc::Rc};

use generic_container::GenericContainer;
use oorandom::Rand32;

use anchored_skiplist::{
    Comparator, DefaultComparator, Skiplist, SkiplistIterator as _, SkiplistLendingIterator as _
};
use anchored_skiplist::concurrent::{Iter, LendingIter, ConcurrentSkiplist};


all::tests_for_all_skiplists!(ConcurrentSkiplist);
reference_counted::tests_for_refcounted_skiplists!(ConcurrentSkiplist);


// This test is unique to `ConcurrentSkiplist`.
#[cfg(not(tests_with_leaks))]
#[test]
fn concurrent_write_while_write_locked() {
    let mut list = ConcurrentSkiplist::new(DefaultComparator);

    list.insert_copy(&[1]);

    let mut list_handle = list.clone();
    // This is actualy a no-op that does nothing.
    let mut list: ConcurrentSkiplist<DefaultComparator> = list.write_locked();

    // No panic or anything.
    list_handle.insert_copy(&[2]);
    list.insert_copy(&[3]);

    let list = &ConcurrentSkiplist::write_unlocked(list);

    assert!(list.into_iter().eq([[1].as_slice(), [2].as_slice(), [3].as_slice()].into_iter()));
}

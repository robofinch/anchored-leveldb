#![allow(unexpected_cfgs, reason = "Extra cfg's are used by the Miri tests for this crate.")]
#![allow(unused_crate_dependencies, reason = "These are tests, not the main crate.")]
#![allow(unused_imports, reason = "Depending on cfg, some are unused. Annoying to annotate.")]

mod all;
mod reference_counted;


use std::array;
use std::{cell::RefCell, cmp::Ordering, collections::BTreeSet, rc::Rc};

use clone_behavior::{AnySpeed, IndependentClone, MirroredClone, MixedClone, NearInstant};
use generic_container::GenericContainer;
use oorandom::Rand32;
use seekable_iterator::{
    Comparator, CursorIterator as _, CursorLendingIterator as _,
    DefaultComparator, Seekable as _,
};

use anchored_skiplist::Skiplist;
use anchored_skiplist::concurrent::{Iter, LendingIter, ConcurrentSkiplist};


all::tests_for_all_skiplists!(ConcurrentSkiplist, Iter, LendingIter);
reference_counted::tests_for_refcounted_skiplists!(ConcurrentSkiplist, Iter, LendingIter);


// These tests are unique to `ConcurrentSkiplist`.

#[cfg(not(tests_with_leaks))]
#[test]
fn concurrent_write_while_write_locked() {
    let mut list = ConcurrentSkiplist::new(DefaultComparator);

    list.insert_copy(&[1]);

    let mut list_handle = list.refcounted_clone();
    // This is actually a no-op that does nothing.
    let mut list: ConcurrentSkiplist<DefaultComparator> = list.write_locked();

    // No panic or anything.
    list_handle.insert_copy(&[2]);
    list.insert_copy(&[3]);

    let list = &ConcurrentSkiplist::write_unlocked(list);

    assert!(list.into_iter().eq([[1].as_slice(), [2].as_slice(), [3].as_slice()].into_iter()));
}

#[cfg(not(tests_with_leaks))]
#[test]
fn suspicious_init_entry() {
    #[derive(Debug, Clone, Copy)]
    struct TrivialComparator;

    impl MirroredClone<AnySpeed> for TrivialComparator {
        fn mirrored_clone(&self) -> Self {
            Self
        }
    }

    impl Comparator<[u8]> for TrivialComparator {
        fn cmp(&self, _lhs: &[u8], _rhs: &[u8]) -> Ordering {
            Ordering::Equal
        }
    }

    let mut list = ConcurrentSkiplist::new(TrivialComparator);
    let mut other_handle = list.refcounted_clone();

    // The inner insert should succeed, and the outer insert should fail.
    assert!(!list.insert_with(1, |data| {
        data[0] = 1;
        assert!(other_handle.insert_copy(&[2]));
    }));

    assert!(list.iter().eq([[2].as_slice()].into_iter()));

    // In the next case, both should succeed.
    let mut list = ConcurrentSkiplist::new(DefaultComparator);
    let mut other_handle = list.refcounted_clone();

    assert!(list.insert_with(1, |data| {
        data[0] = 1;
        assert!(other_handle.insert_copy(&[2]));
    }));

    assert!(list.iter().eq([[1].as_slice(), [2].as_slice()].into_iter()))
}

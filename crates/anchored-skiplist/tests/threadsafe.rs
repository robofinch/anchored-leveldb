#![allow(unused_crate_dependencies, reason = "These are tests, not the main crate.")]
#![allow(unused_imports, reason = "Depending on cfg, some are unused. Annoying to annotate.")]

mod all;
mod reference_counted;


use std::array;
use std::{cell::RefCell, cmp::Ordering, collections::BTreeSet, rc::Rc};

use clone_behavior::DeepClone;
use generic_container::GenericContainer;
use oorandom::Rand32;
use seekable_iterator::{
    Comparator, CursorIterator as _, CursorLendingIterator as _,
    DefaultComparator, Seekable as _,
};

use anchored_skiplist::Skiplist;
use anchored_skiplist::threadsafe::{Iter, LendingIter, ThreadsafeSkiplist};


all::tests_for_all_skiplists!(ThreadsafeSkiplist, Iter, LendingIter);
reference_counted::tests_for_refcounted_skiplists!(ThreadsafeSkiplist, Iter, LendingIter);

mod locked_tests {
    use std::array;
    use std::{cell::RefCell, cmp::Ordering, collections::BTreeSet, rc::Rc};

    use clone_behavior::DeepClone;
    use generic_container::GenericContainer;
    use oorandom::Rand32;
    use seekable_iterator::{
        Comparator, CursorIterator as _, CursorLendingIterator as _,
        DefaultComparator, Seekable as _,
    };

    use anchored_skiplist::Skiplist;
    use anchored_skiplist::threadsafe::{LockedIter, LockedLendingIter, LockedThreadsafeSkiplist};

    super::all::tests_for_all_skiplists!(LockedThreadsafeSkiplist, LockedIter, LockedLendingIter);
}

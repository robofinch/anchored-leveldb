#![expect(unsafe_code, reason = "allow skiplists to be externally synchronized")]

use std::{cell::Cell, rc::Rc, sync::Arc};
use std::sync::atomic::{AtomicUsize, Ordering};

use clone_behavior::{Fast, MirroredClone, Speed};
use seekable_iterator::{LendItem, SeekableIterator, SeekableLendingIterator};

use anchored_skiplist::Skiplist;
use anchored_skiplist::{
    concurrent::{ConcurrentSkiplist, Iter as ConcurrentIter, LendingIter as ConcurrentLendingIter},
    threadsafe::{
        ThreadsafeSkiplist, LockedThreadsafeSkiplist,
        Iter as ThreadsafeIter, LendingIter as ThreadsafeLendingIter,
    },
};

use crate::table_traits::{LevelDBComparator, MemtableComparator};


// TODO: should likely be a safety requirement on this trait that methods do not create
// reference-counted clones with write access.
pub(crate) trait MemtableSkiplist<Cmp: LevelDBComparator> {
    type Iter<'a>:
        SeekableIterator<[u8], MemtableComparator<Cmp>, Item = &'a [u8]>
    where
        Self: 'a;
    type LendingIter: SeekableLendingIterator<[u8], MemtableComparator<Cmp>>
        + for<'a> LendItem<'a, Item = &'a [u8]>;
    type WriteAccess<'a>: WriteAccess where Self: 'a;

    #[must_use]
    fn new(cmp: Cmp) -> Self;

    /// # Safety
    /// The `WriteAccess` borrow must be unique across all reference-counted clones of a given
    /// `MemtableSkiplist` for its entire lifetime.
    ///
    /// It suffices for `externally_synchronized` to only ever be called on a certain instance of
    /// the `MemtableSkiplist`; the `&mut` borrow can ensure that access is unique for a single
    /// `Self` instance, and if the `externally_synchronized` method is never used on other
    /// reference-counted clones of `self`, then access is unique across all of them.
    unsafe fn externally_synchronized(&mut self) -> Self::WriteAccess<'_>;

    #[must_use]
    fn iter(&self) -> Self::Iter<'_>;

    #[must_use]
    fn lending_iter(self) -> Self::LendingIter;

    /// Returns a somewhat-close lower bound for the total number of bytes allocated by
    /// this skiplist.
    #[must_use]
    fn allocated_bytes(&self) -> usize;
}

/// If it is possible to [`Clone`], [`Copy`], or otherwise duplicate this type, then
/// passing a clone into the `init_entry` callback of `insert_with` must be safe by the
/// normal semantics of safe and `unsafe` Rust code. However, as far as non-safety-related
/// requirements go, `insert_with` is called once below and clearly is not pathological.
/// Implementations may assume that the `init_entry` callback is well-behaved, and need not
/// document their behavior in related edge cases.
pub(crate) trait WriteAccess {
    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// may be discarded. Attempting to add duplicate entries should be avoided, as the spent memory
    /// may or may not be reclaimed until the skiplist is dropped.
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F);
}

#[derive(Debug)]
pub(crate) struct UnsyncMemtableSkiplist<Cmp> {
    list:              ConcurrentSkiplist<MemtableComparator<Cmp>>,
    // TODO: expose `Bump` method on the skiplist to remove need for the `Rc<Cell<usize>>`
    total_entry_bytes: Rc<Cell<usize>>
}

impl<Cmp: LevelDBComparator> MemtableSkiplist<Cmp> for UnsyncMemtableSkiplist<Cmp> {
    type Iter<'a>        = ConcurrentIter<'a, MemtableComparator<Cmp>> where Self: 'a;
    type LendingIter     = ConcurrentLendingIter<MemtableComparator<Cmp>>;
    type WriteAccess<'a> = UnsyncWriteAccess<'a, Cmp> where Self: 'a;

    #[inline]
    fn new(cmp: Cmp) -> Self {
        Self {
            list:              ConcurrentSkiplist::new(MemtableComparator(cmp)),
            total_entry_bytes: Rc::new(Cell::new(0)),
        }
    }

    /// # Safety
    /// There are no safety requirements for this implementation.
    ///
    /// However, if [`WriteAccess::insert_with`] is called on the returned value with an
    /// `init_entry` callback which itself attempts to call `insert_with`, the behavior is
    /// deterministic but perhaps unexpected. Avoid doing that.
    #[inline]
    unsafe fn externally_synchronized(&mut self) -> Self::WriteAccess<'_> {
        UnsyncWriteAccess(self)
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        Skiplist::iter(&self.list)
    }

    #[inline]
    fn lending_iter(self) -> Self::LendingIter {
        Skiplist::lending_iter(self.list)
    }

    #[inline]
    fn allocated_bytes(&self) -> usize {
        self.total_entry_bytes.get()
    }
}

impl<Cmp: MirroredClone<S>, S: Speed> MirroredClone<S> for UnsyncMemtableSkiplist<Cmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            list:              self.list.mirrored_clone(),
            total_entry_bytes: Rc::clone(&self.total_entry_bytes),
        }
    }
}

impl<Cmp: Default + LevelDBComparator> Default for UnsyncMemtableSkiplist<Cmp> {
    #[inline]
    fn default() -> Self {
        Self::new(Cmp::default())
    }
}

#[derive(Debug)]
pub(crate) struct UnsyncWriteAccess<'a, Cmp>(&'a mut UnsyncMemtableSkiplist<Cmp>);

impl<Cmp: LevelDBComparator> WriteAccess for UnsyncWriteAccess<'_, Cmp> {
    #[inline]
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) {
        Skiplist::insert_with(&mut self.0.list, entry_len, init_entry);
        self.0.total_entry_bytes.set(self.0.total_entry_bytes.get() + entry_len);
    }
}

// TODO: create and use an externally-synchronized skiplist that doesn't need its own write lock

#[derive(Debug)]
pub(crate) struct SyncMemtableSkiplist<Cmp> {
    list:              ThreadsafeSkiplist<MemtableComparator<Cmp>>,
    total_entry_bytes: Arc<AtomicUsize>,
}

impl<Cmp: LevelDBComparator + MirroredClone<Fast>> MemtableSkiplist<Cmp>
for SyncMemtableSkiplist<Cmp>
{
    type Iter<'a>        = ThreadsafeIter<'a, MemtableComparator<Cmp>> where Self: 'a;
    type LendingIter     = ThreadsafeLendingIter<MemtableComparator<Cmp>>;
    type WriteAccess<'a> = SyncWriteAccess<'a, Cmp> where Self: 'a;

    #[inline]
    fn new(cmp: Cmp) -> Self {
        Self {
            list:              ThreadsafeSkiplist::new(MemtableComparator(cmp)),
            total_entry_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// # Safety
    /// The `WriteAccess` borrow must be unique across all reference-counted clones of a given
    /// `MemtableSkiplist` for its entire lifetime.
    ///
    /// It suffices for `externally_synchronized` to only ever be called on a certain instance of
    /// the `MemtableSkiplist`; the `&mut` borrow can ensure that access is unique for a single
    /// `Self` instance, and if the `externally_synchronized` method is never used on other
    /// reference-counted clones of `self`, then access is unique across all of them.
    #[inline]
    unsafe fn externally_synchronized(&mut self) -> Self::WriteAccess<'_> {
        SyncWriteAccess {
            list:              self.list.refcounted_clone().write_locked(),
            total_entry_bytes: &self.total_entry_bytes,
        }
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        Skiplist::iter(&self.list)
    }

    #[inline]
    fn lending_iter(self) -> Self::LendingIter {
        Skiplist::lending_iter(self.list)
    }

    #[inline]
    fn allocated_bytes(&self) -> usize {
        // The exact value doesn't matter very much
        self.total_entry_bytes.load(Ordering::Relaxed)
    }
}

impl<Cmp: MirroredClone<S>, S: Speed> MirroredClone<S> for SyncMemtableSkiplist<Cmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            list:              self.list.mirrored_clone(),
            total_entry_bytes: Arc::clone(&self.total_entry_bytes),
        }
    }
}

impl<Cmp> Default for SyncMemtableSkiplist<Cmp>
where
    Cmp: Default + LevelDBComparator + MirroredClone<Fast>,
{
    #[inline]
    fn default() -> Self {
        Self::new(Cmp::default())
    }
}

#[derive(Debug)]
pub(crate) struct SyncWriteAccess<'a, Cmp> {
    list:              LockedThreadsafeSkiplist<MemtableComparator<Cmp>>,
    total_entry_bytes: &'a AtomicUsize,
}

impl<Cmp: LevelDBComparator> WriteAccess for SyncWriteAccess<'_, Cmp> {
    #[inline]
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) {
        Skiplist::insert_with(&mut self.list, entry_len, init_entry);
        // The exact value doesn't matter very much
        self.total_entry_bytes.fetch_add(entry_len, Ordering::Relaxed);
    }
}

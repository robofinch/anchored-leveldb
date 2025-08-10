use clone_behavior::{AnySpeed, IndependentClone, MirroredClone, MixedClone, Speed};

use crate::{skiplistiter_wrapper, skiplistlendingiter_wrapper};
use crate::{
    interface::{Comparator, Skiplist, SkiplistIterator, SkiplistLendingIterator},
    iter_defaults::{SkiplistIter, SkiplistLendingIter},
};
use super::list_inner::MultithreadedSkiplist;
use super::head_state::{LockedThreadsafeState, UnlockedThreadsafeState};


// ================================================================
//  List
// ================================================================

/// A skiplist which supports multithreaded concurrency through reference-counted cloning.
///
/// Reading from the skiplist is lock-free, but the skiplist acquires a write lock for insertions.
///
/// The [`Skiplist`] trait must be imported to use the list effectively.
#[derive(Debug)]
pub struct ThreadsafeSkiplist<Cmp>(MultithreadedSkiplist<Cmp, UnlockedThreadsafeState>);

impl<Cmp: MirroredClone<AnySpeed>> ThreadsafeSkiplist<Cmp> {
    /// Get another reference-counted handle to the same skiplist.
    #[inline]
    #[must_use]
    pub fn refcounted_clone(&self) -> Self {
        self.mirrored_clone()
    }
}

impl<Cmp: Comparator + IndependentClone<AnySpeed>> ThreadsafeSkiplist<Cmp> {
    /// Copy the contents of this skiplist into a new, independent skiplist.
    #[inline]
    #[must_use]
    pub fn deep_clone(&self) -> Self {
        self.independent_clone()
    }
}

impl<S: Speed, Cmp: MirroredClone<S>> MirroredClone<S> for ThreadsafeSkiplist<Cmp> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

impl<Cmp: Comparator + IndependentClone<AnySpeed>> IndependentClone<AnySpeed>
for ThreadsafeSkiplist<Cmp>
{
    /// # Panics or Deadlocks
    /// Will either panic or deadlock if the current thread holds this skiplist's write lock for
    /// insertions.
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

impl<Cmp: Comparator + Default> Default for ThreadsafeSkiplist<Cmp> {
    #[inline]
    fn default() -> Self {
        Self::new(Cmp::default())
    }
}

#[expect(clippy::into_iter_without_iter, reason = ".iter() is provided by Skiplist trait")]
impl<'a, Cmp: Comparator> IntoIterator for &'a ThreadsafeSkiplist<Cmp> {
    type IntoIter = Iter<'a, Cmp>;
    type Item     = &'a [u8];

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<Cmp: Comparator> Skiplist<Cmp> for ThreadsafeSkiplist<Cmp> {
    type WriteLocked = LockedThreadsafeSkiplist<Cmp>;
    type Iter<'a>    = Iter<'a, Cmp> where Self: 'a;
    type LendingIter = LendingIter<Cmp>;

    #[inline]
    fn new_seeded(cmp: Cmp, seed: u64) -> Self {
        Self(MultithreadedSkiplist::new_seeded(cmp, seed))
    }

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as skiplist implementations might not reclaim the
    /// spent memory until the skiplist is dropped.
    ///
    /// # Panics or Deadlocks
    /// A panic or deadlock will occur if the `init_entry` callback attempts to call
    /// [`insert_with`], [`insert_copy`], [`write_locked`], or [`independent_clone`] on the
    /// skiplist (including via reference-counted clones).
    ///
    /// If a thread panics while inserting into the skiplist, or panics while holding a
    /// [`WriteLocked`], all other attempts to insert into the skiplist will panic as well,
    /// due to [poison errors].
    ///
    /// [`insert_with`]: Skiplist::insert_with
    /// [`insert_copy`]: Skiplist::insert_copy
    /// [`write_locked`]: Skiplist::write_locked
    /// [`independent_clone`]: ThreadsafeSkiplist::independent_clone
    /// [`WriteLocked`]: Skiplist::WriteLocked
    /// [poison errors]: std::sync::PoisonError
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) -> bool {
        self.0.insert_with(entry_len, init_entry)
    }

    /// Acquire and hold the write lock required for insertions, to improve the speed of following
    /// writes.
    ///
    /// Dropping the returned `WriteLocked` or using [`Self::write_unlocked`] releases the write
    /// lock.
    ///
    /// # Panics or Deadlocks
    /// After the current thread obtains a `WriteLocked`, a panic or deadlock will occur if that
    /// same thread attempts to call [`insert_with`], [`insert_copy`], [`write_locked`], or
    /// [`independent_clone`] on a reference-counted clone of the skiplist *other* than the
    /// returned `WriteLocked`. That is, the thread should attempt to mutate the skiplist only
    /// through the returned `WriteLocked`, while it exists.
    ///
    /// This function may block if a different thread holds write locks, perhaps for a long period
    /// of time if that thread has acquired a `WriteLocked`.
    ///
    /// Additionally, note that if the thread panics while holding the write locks, the related
    /// mutex will become poisoned and lead to later panics on other threads.
    ///
    /// [`insert_with`]: Skiplist::insert_with
    /// [`insert_copy`]: Skiplist::insert_copy
    /// [`write_locked`]: Skiplist::write_locked
    /// [`independent_clone`]: ThreadsafeSkiplist::independent_clone
    /// [`Self::write_unlocked`]: Skiplist::write_unlocked
    #[inline]
    fn write_locked(self) -> Self::WriteLocked {
        LockedThreadsafeSkiplist(self.0.write_locked())
    }

    /// Releases the lock required for insertions. `ThreadsafeSkiplist` acquires the lock
    /// only inside the insertion functions.
    #[inline]
    fn write_unlocked(list: Self::WriteLocked) -> Self {
       Self(MultithreadedSkiplist::write_unlocked(list.0))
    }

    fn contains(&self, entry: &[u8]) -> bool {
        self.0.contains(entry)
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        Iter::new(self)
    }

    #[inline]
    fn lending_iter(self) -> Self::LendingIter {
        LendingIter::new(self)
    }

    #[inline]
    fn from_lending_iter(lending_iter: Self::LendingIter) -> Self {
        lending_iter.into_list()
    }
}

// ================================================================
//  Iter and LendingIter
// ================================================================

skiplistiter_wrapper! {
    #[derive(Debug)]
    pub struct Iter<'_, Cmp: _>(
        #[List = MultithreadedSkiplist<Cmp, UnlockedThreadsafeState>] _,
    );
}

impl<'a, Cmp: Comparator> Iter<'a, Cmp> {
    #[inline]
    #[must_use]
    const fn new(list: &'a ThreadsafeSkiplist<Cmp>) -> Self {
        Self(SkiplistIter::new(&list.0))
    }
}

impl<Cmp: Comparator> Clone for Iter<'_, Cmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: Speed, Cmp: Comparator> MixedClone<S> for Iter<'_, Cmp> {
    #[inline]
    fn mixed_clone(&self) -> Self {
        self.clone()
    }
}

skiplistlendingiter_wrapper! {
    #[derive(Debug, Clone)]
    pub struct LendingIter<Cmp: _>(
        #[List = MultithreadedSkiplist<Cmp, UnlockedThreadsafeState>] _,
    );
}

impl<S: Speed, Cmp: Comparator + MirroredClone<S>> MixedClone<S> for LendingIter<Cmp> {
    #[inline]
    fn mixed_clone(&self) -> Self {
        Self(self.0.mixed_clone())
    }
}

impl<Cmp: Comparator + IndependentClone<AnySpeed>> IndependentClone<AnySpeed> for LendingIter<Cmp> {
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

impl<Cmp: Comparator> LendingIter<Cmp> {
    #[inline]
    #[must_use]
    fn new(list: ThreadsafeSkiplist<Cmp>) -> Self {
        Self(SkiplistLendingIter::new(list.0))
    }

    #[inline]
    #[must_use]
    fn into_list(self) -> ThreadsafeSkiplist<Cmp> {
        ThreadsafeSkiplist(self.0.into_list())
    }
}

// ================================================================
//  Locked List
// ================================================================

/// A skiplist which supports multithreaded concurrency through reference-counted cloning.
///
/// All operations on this skiplist are lock-free, as the write lock was already acquired in the
/// process of creating this skiplist.
///
/// The [`Skiplist`] trait must be imported to use the list effectively. This skiplist
/// may only be created through [`ThreadsafeSkiplist::write_locked`].
#[derive(Debug)]
pub struct LockedThreadsafeSkiplist<Cmp>(MultithreadedSkiplist<Cmp, LockedThreadsafeState>);

impl<Cmp: Comparator + IndependentClone<AnySpeed>> LockedThreadsafeSkiplist<Cmp> {
    /// Copy the contents of this skiplist into a new, independent skiplist.
    #[inline]
    #[must_use]
    pub fn deep_clone(&self) -> Self {
        self.independent_clone()
    }
}

impl<Cmp: Comparator + IndependentClone<AnySpeed>> IndependentClone<AnySpeed>
for LockedThreadsafeSkiplist<Cmp>
{
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

impl<Cmp: Comparator + Default> Default for LockedThreadsafeSkiplist<Cmp> {
    #[inline]
    fn default() -> Self {
        Self::new(Cmp::default())
    }
}

#[expect(clippy::into_iter_without_iter, reason = ".iter() is provided by Skiplist trait")]
impl<'a, Cmp: Comparator> IntoIterator for &'a LockedThreadsafeSkiplist<Cmp> {
    type IntoIter = LockedIter<'a, Cmp>;
    type Item     = &'a [u8];

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<Cmp: Comparator> Skiplist<Cmp> for LockedThreadsafeSkiplist<Cmp> {
    type WriteLocked = Self;
    type Iter<'a>    = LockedIter<'a, Cmp> where Self: 'a;
    type LendingIter = LockedLendingIter<Cmp>;

    #[inline]
    fn new_seeded(cmp: Cmp, seed: u64) -> Self {
        Self(MultithreadedSkiplist::new_seeded(cmp, seed))
    }

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as skiplist implementations might not reclaim the
    /// spent memory until the skiplist is dropped.
    ///
    /// # Panics or Deadlocks
    /// A panic or deadlock will occur if the `init_entry` callback attempts to call
    /// [`insert_with`], [`insert_copy`], or [`write_locked`] on the skiplist (including via
    /// reference-counted clones).
    ///
    /// If a thread panics while inserting into the skiplist, or panics while holding a
    /// [`WriteLocked`], all other attempts to insert into the skiplist will panic as well,
    /// due to [poison errors].
    ///
    /// [`insert_with`]: Skiplist::insert_with
    /// [`insert_copy`]: Skiplist::insert_copy
    /// [`write_locked`]: Skiplist::write_locked
    /// [`WriteLocked`]: Skiplist::WriteLocked
    /// [poison errors]: std::sync::PoisonError
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) -> bool {
        self.0.insert_with(entry_len, init_entry)
    }

    /// Since this skiplist already holds the write lock, `write_locked` is a no-op.
    ///
    /// There remains the risk of panics from using the skiplist's other handles to insert entries.
    #[inline]
    fn write_locked(self) -> Self::WriteLocked {
        self
    }

    /// Since this skiplist already holds the write lock, `write_unlocked` is a no-op.
    #[inline]
    fn write_unlocked(list: Self::WriteLocked) -> Self {
        list
    }

    fn contains(&self, entry: &[u8]) -> bool {
        self.0.contains(entry)
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        LockedIter::new(self)
    }

    #[inline]
    fn lending_iter(self) -> Self::LendingIter {
        LockedLendingIter::new(self)
    }

    #[inline]
    fn from_lending_iter(lending_iter: Self::LendingIter) -> Self {
        lending_iter.into_list()
    }
}

// ================================================================
//  LockedIter and LockedLendingIter
// ================================================================

skiplistiter_wrapper! {
    #[derive(Debug)]
    pub struct LockedIter<'_, Cmp: _>(
        #[List = MultithreadedSkiplist<Cmp, LockedThreadsafeState>] _,
    );
}

impl<'a, Cmp: Comparator> LockedIter<'a, Cmp> {
    #[inline]
    #[must_use]
    const fn new(list: &'a LockedThreadsafeSkiplist<Cmp>) -> Self {
        Self(SkiplistIter::new(&list.0))
    }
}

impl<Cmp: Comparator> Clone for LockedIter<'_, Cmp> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: Speed, Cmp: Comparator> MixedClone<S> for LockedIter<'_, Cmp> {
    #[inline]
    fn mixed_clone(&self) -> Self {
        self.clone()
    }
}

skiplistlendingiter_wrapper! {
    #[derive(Debug)]
    pub struct LockedLendingIter<Cmp: _>(
        #[List = MultithreadedSkiplist<Cmp, LockedThreadsafeState>] _,
    );
}

impl<Cmp: Comparator> LockedLendingIter<Cmp> {
    #[inline]
    #[must_use]
    fn new(list: LockedThreadsafeSkiplist<Cmp>) -> Self {
        Self(SkiplistLendingIter::new(list.0))
    }

    #[inline]
    #[must_use]
    fn into_list(self) -> LockedThreadsafeSkiplist<Cmp> {
        LockedThreadsafeSkiplist(self.0.into_list())
    }
}

impl<Cmp: Comparator + IndependentClone<AnySpeed>> IndependentClone<AnySpeed>
for LockedLendingIter<Cmp>
{
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

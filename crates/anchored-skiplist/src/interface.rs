use std::cmp::Ordering;

use generic_container::{FragileContainer, GenericContainer};


// TODO: add comments describing the time complexity of various operations.


/// A minimal [skiplist] interface which allows entries to be inserted but never removed.
///
/// Implementations may or may not be threadsafe. Even if an implementation is threadsafe,
/// newly-added entries may or may not be seen immediately by other threads.
///
/// If a thread panics while inserting into the skiplist, or panics while holding a [`WriteLocked`],
/// all other attempts to insert into the skiplist may or may not panic as well, depending on
/// whether an implementation can encounter [poison errors] and how they are handled.
///
/// [skiplist]: https://en.wikipedia.org/wiki/Skip_list
/// [`WriteLocked`]: Skiplist::WriteLocked
/// [poison errors]: std::sync::PoisonError
// TODO(feature): consider providing ways to gracefully error upon poisoned mutexes.
// As panics are not something most people have an interest in recovering from, this is
// not a priority.
pub trait Skiplist<Cmp: Comparator>: Sized {
    /// A version of the skiplist which holds any write locks needed for insertions until it is
    /// dropped or released with [`Self::write_unlocked`], instead of acquiring those locks only
    /// while performing insertions.
    ///
    /// If the skiplist implementation does not have any such locks to acquire (or is itself
    /// a `WriteLocked` type which already holds those locks), `WriteLocked` should be set to
    /// `Self`.
    ///
    /// If a thread panics while inserting into the skiplist, or panics while holding a
    /// `WriteLocked`, all other attempts to insert into the skiplist may or may not panic as well,
    /// depending on whether an implementation can encounter [poison errors] and how they are
    /// handled.
    ///
    /// [`Self::write_unlocked`]: Skiplist::write_unlocked
    /// [poison errors]: std::sync::PoisonError
    type WriteLocked: Skiplist<Cmp>;
    type Iter<'a>:    SkiplistIterator<'a> where Self: 'a;
    type LendingIter: SkiplistLendingIterator;

    #[inline]
    #[must_use]
    fn new(cmp: Cmp) -> Self {
        // Figured I'd use the fun default seed at
        // https://github.com/google/leveldb/blob/ac691084fdc5546421a55b25e7653d450e5a25fb/db/skiplist.h#L322-L328
        Self::new_seeded(cmp, 0x_deadbeef)
    }

    #[must_use]
    fn new_seeded(cmp: Cmp, seed: u64) -> Self;

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as skiplist implementations might not reclaim the
    /// spent memory until the skiplist is dropped.
    ///
    /// # Panics or Deadlocks
    /// Implementatations may panic or deadlock if the `init_entry` callback attempts to call
    /// [`insert_with`], [`insert_copy`], or [`write_locked`] on the skiplist (including via
    /// reference-counted clones). Specific implementations may indicate otherwise.
    ///
    /// If a thread panics while inserting into the skiplist, or panics while holding a
    /// [`WriteLocked`], all other attempts to insert into the skiplist may or may not panic as
    /// well, depending on whether an implementation can encounter [poison errors] and how they are
    /// handled.
    ///
    /// [`insert_with`]: Skiplist::insert_with
    /// [`insert_copy`]: Skiplist::insert_copy
    /// [`write_locked`]: Skiplist::write_locked
    /// [`WriteLocked`]: Skiplist::WriteLocked
    /// [poison errors]: std::sync::PoisonError
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F) -> bool;

    /// Insert the provided data into the skiplist, incurring a copy to create an owned version of
    /// the entry.
    ///
    /// If the resulting entry compares equal to an entry already in the skiplist, the entry
    /// is discarded, and `false` is returned. Otherwise, `true` is returned. Attempting to add
    /// duplicate entries should be avoided, as skiplist implementations might not reclaim the
    /// spent memory until the skiplist is dropped.
    #[inline]
    fn insert_copy(&mut self, entry: &[u8]) -> bool {
        self.insert_with(
            entry.len(),
            |created_entry| created_entry.copy_from_slice(entry),
        )
    }

    /// Signal to the skiplist implementation that it should acquire and hold any write locks
    /// it needs for insertions, to improve the speed of following writes.
    ///
    /// If the skiplist implementation does not have any locks to acquire, or this skiplist is
    /// already a `WriteLocked` type which has acquired those locks, this function should be a
    /// no-op which returns `Self`.
    ///
    /// Dropping the returned `WriteLocked` or using [`Self::write_unlocked`] should release
    /// any write locks newly acquired by this function.
    ///
    /// # Panics or Deadlocks
    /// After the current thread obtains a `WriteLocked`, implementations may panic or deadlock
    /// if that same thread attempts to call [`insert_with`], [`insert_copy`], or [`write_locked`]
    /// on a reference-counted clone of the skiplist *other* than the returned `WriteLocked`. That
    /// is, the thread should attempt to mutate the skiplist only through the returned
    /// `WriteLocked`, while it exists.
    ///
    /// This function may block if a different thread holds write locks, perhaps for a long period
    /// of time if that thread has acquired a `WriteLocked`.
    ///
    /// Additionally, note that if the thread panics while holding the write locks, the related
    /// mutexes may become poisoned and lead to later panics on other threads.
    ///
    /// Specific implementations may indicate otherwise.
    ///
    /// [`insert_with`]: Skiplist::insert_with
    /// [`insert_copy`]: Skiplist::insert_copy
    /// [`write_locked`]: Skiplist::write_locked
    /// [`Self::write_unlocked`]: Skiplist::write_unlocked
    #[must_use]
    fn write_locked(self) -> Self::WriteLocked;

    /// Unless [`Self::write_locked`] was a no-op, release any locks required for insertions,
    /// and return to acquiring them only inside the insertion functions.
    ///
    /// If [`Self::write_locked`] was a no-op, then this function should be a no-op which returns
    /// the provided `list`.
    ///
    /// [`Self::write_locked`]: Skiplist::write_locked
    #[must_use]
    fn write_unlocked(list: Self::WriteLocked) -> Self;

    /// Check whether the entry, or something which compares as equal to the entry, is in
    /// the skiplist.
    #[must_use]
    fn contains(&self, entry: &[u8]) -> bool;

    /// Get an iterator that can seek through the skiplist to read entries.
    ///
    /// See [`SkiplistIterator`] for more.
    #[must_use]
    fn iter(&self) -> Self::Iter<'_>;

    /// Move the skiplist into a lending iterator which can seek through the list to read entries.
    ///
    /// See [`SkiplistLendingIterator`] for more.
    #[must_use]
    fn lending_iter(self) -> Self::LendingIter;

    /// Reclaim the underlying skiplist from a lending iterator.
    #[must_use]
    fn from_lending_iter(lending_iter: Self::LendingIter) -> Self;
}

/// A `SkiplistIterator` provides access to the entries of the [`Skiplist`] it references.
///
/// Conceptually, it is circular, and its initial position is before the first entry and after the
/// last entry. As such, it is not a [`FusedIterator`], as continuing to call `next()` at the
/// end of iteration wraps around to the start. (Note that if the skiplist is empty, then the
/// iterator will remain at that phantom position.)
///
/// Implementations may or may not be threadsafe. Even if an implementation is threadsafe,
/// newly-added entries may or may not be seen immediately by other threads.
///
/// In most implementations, so long as the source skiplist (or a reference-counted clone
/// associated with that skiplist) has not been dropped or otherwise invalidated, aside from being
/// moved, the returned entry references are likely still usable. If an implementation of
/// `SkiplistIterator` is trusted and clearly indicates that lifetime extension is permitted, it
/// should be sound to perform unsafe lifetime extension of the returned entries.
///
/// [`FusedIterator`]: std::iter::FusedIterator
pub trait SkiplistIterator<'a>: Iterator<Item = &'a [u8]> {
    /// Determine whether the iterator is currently at any value in the skiplist.
    /// If the iterator is invalid, then it is conceptually one position before the first entry
    /// and one position after the last entry. (Or, there may be no entries.)
    ///
    /// [`current()`] will be `Some` if and only if the iterator is valid.
    ///
    /// [`current()`]: SkiplistIterator::current
    /// [`next()`]: Iterator::next
    #[must_use]
    fn is_valid(&self) -> bool;

    /// Reset the iterator to its initial position, before the first entry and after the last
    /// entry (if there are any entries in the skiplist). The iterator will then not be [valid].
    ///
    /// [valid]: SkiplistIterator::is_valid
    fn reset(&mut self);

    /// Get the current value the iterator is at, if the iterator is valid.
    #[must_use]
    fn current(&self) -> Option<&'a [u8]>;

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was on the first entry of the skiplist.
    fn prev(&mut self) -> Option<&'a [u8]>;

    /// Move the iterator to the smallest entry which is greater or equal than the provided
    /// `min_bound`.
    ///
    /// If there is no such entry, the iterator becomes [invalid], and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the skiplist).
    ///
    /// [invalid]: SkiplistIterator::is_valid
    fn seek(&mut self, min_bound: &[u8]);

    /// Move the iterator to the greatest entry which is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such entry, the iterator becomes [invalid], and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the skiplist).
    ///
    /// [invalid]: SkiplistIterator::is_valid
    fn seek_before(&mut self, strict_upper_bound: &[u8]);

    /// Move the iterator to the smallest entry in the skiplist.
    ///
    /// If the skiplist is empty, the iterator is [invalid].
    ///
    /// [invalid]: SkiplistIterator::is_valid
    fn seek_to_first(&mut self);

    /// Move the iterator to the greatest entry in the skiplist.
    ///
    /// If the skiplist is empty, the iterator is [invalid].
    ///
    /// [invalid]: SkiplistIterator::is_valid
    fn seek_to_last(&mut self);
}

/// A `SkiplistLendingIterator` provides access to the entries of the [`Skiplist`] it owns.
///
/// Conceptually, it is circular, and its initial position is before the first entry and after the
/// last entry. As such, it is not a [`FusedIterator`], as continuing to call `next()` at the
/// end of iteration wraps around to the start. (Note that if the skiplist is empty, then the
/// iterator will remain at that phantom position.)
///
/// Implementations may or may not be threadsafe. Even if an implementation is threadsafe,
/// newly-added entries may or may not be seen immediately by other threads.
///
/// Since getting another entry requires another reference to `self`, only one entry can be used at
/// a time. However, mutable borrows likely do not actually invalidate the returned entries; so long
/// as the skiplist iterator (and/or a reference-counted clone of the skiplist it came from, if
/// available) has not been dropped or otherwise invalidated, in most implementations, the entries
/// should still be usable. If an implementation of `Skiplist` is trusted and clearly indicates
/// that lifetime extension is permitted, it should be sound to perform unsafe lifetime extension
/// of the returned entries.
///
/// [`FusedIterator`]: std::iter::FusedIterator
pub trait SkiplistLendingIterator {
    /// Determine whether the iterator is currently at any value in the skiplist.
    /// If the iterator is invalid, then it is conceptually one position before the first entry
    /// and one position after the last entry. (Or, there may be no entries.)
    ///
    /// [`current()`] will be `Some` if and only if the iterator is valid.
    ///
    /// [`current()`]: SkiplistLendingIterator::current
    /// [`next()`]: Iterator::next
    #[must_use]
    fn is_valid(&self) -> bool;

    /// Reset the iterator to its initial position, before the first entry and after the last
    /// entry (if there are any entries in the skiplist). The iterator will then not be [valid].
    ///
    /// [valid]: SkiplistLendingIterator::is_valid
    fn reset(&mut self);

    /// Move the iterator one position forwards, and return the entry at that position.
    /// Returns `None` if the iterator was on the last entry of the skiplist.
    ///
    /// For most implementations, the returned entry is valid at least until the
    /// source `Self` value is dropped or invalidated in some way other than moving the `Self`
    /// value. Implementations should clearly note if this is the case.
    fn next(&mut self) -> Option<&[u8]>;

    /// Get the current value the iterator is at, if the iterator is valid.
    ///
    /// For most implementations, the returned entry is valid at least until the
    /// source `Self` value is dropped or invalidated in some way other than moving the `Self`
    /// value. Implementations should clearly note if this is the case.
    #[must_use]
    fn current(&self) -> Option<&[u8]>;

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was on the first entry of the skiplist.
    ///
    /// For most implementations, the returned entry is valid at least until the
    /// source `Self` value is dropped or invalidated in some way other than moving the `Self`
    /// value. Implementations should clearly note if this is the case.
    fn prev(&mut self) -> Option<&[u8]>;

    /// Move the iterator to the smallest entry which is greater or equal than the provided
    /// `min_bound`.
    ///
    /// If there is no such entry, the iterator becomes [invalid], and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the skiplist).
    ///
    /// [invalid]: SkiplistLendingIterator::is_valid
    fn seek(&mut self, min_bound: &[u8]);

    /// Move the iterator to the greatest entry which is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such entry, the iterator becomes [invalid], and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the skiplist).
    ///
    /// [invalid]: SkiplistLendingIterator::is_valid
    fn seek_before(&mut self, strict_upper_bound: &[u8]);

    /// Move the iterator to the smallest entry in the skiplist.
    ///
    /// If the skiplist is empty, the iterator is [invalid].
    ///
    /// [invalid]: SkiplistLendingIterator::is_valid
    fn seek_to_first(&mut self);

    /// Move the iterator to the greatest entry in the skiplist.
    ///
    /// If the skiplist is empty, the iterator is [invalid].
    ///
    /// [invalid]: SkiplistLendingIterator::is_valid
    fn seek_to_last(&mut self);
}

/// Interface for comparing entries in a [`Skiplist`].
///
/// The comparison function should provide a total order on byte slices, just as [`Ord`] would.
///
/// Note that none of the axioms that define a total order require that two elements which compare
/// as equal are "*truly*" equal in some more fundamental sense; that is, byte slices which are
/// distinct (according to `[u8]`'s [`Eq`] implementation) may compare as equal in the provided
/// total order and corresponding equivalence relation.
///
/// Unsafe code is *not* allowed to rely on the correctness of implementations; that is, an
/// incorrect `Comparator` implementation may cause severe logic errors, but must not cause
/// memory unsafety.
pub trait Comparator {
    /// Compare two entries in a [`Skiplist`].
    ///
    /// This method is analogous to [`Ord::cmp`], and should provide a total order on byte slices.
    ///
    /// Note that none of the axioms that define a total order require that two elements which
    /// compare as equal are "*truly*" equal in some more fundamental sense; that is, byte slices
    /// which are distinct (according to `[u8]`'s [`Eq`] implementation) may compare as equal in
    /// the provided total order and corresponding equivalence relation.
    ///
    /// Unsafe code is *not* allowed to rely on the correctness of implementations; that is, an
    /// incorrect implementation may cause severe logic errors, but must not cause
    /// memory unsafety.
    #[must_use]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;
}

impl<C: FragileContainer<dyn Comparator>> Comparator for C {
    #[inline]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        // I'm slightly paranoid about the type coercion coercing to the wrong thing,
        // but doing this line-by-line is probably unnecessary.
        let inner = self.get_ref();
        let inner: &dyn Comparator = &*inner;
        inner.cmp(lhs, rhs)
    }
}

impl<T, C> Comparator for GenericContainer<T, C>
where
    T: ?Sized + Comparator,
    C: ?Sized + FragileContainer<T>,
{
    #[inline]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        let inner = self.container.get_ref();
        let inner: &T = &inner;
        inner.cmp(lhs, rhs)
    }
}

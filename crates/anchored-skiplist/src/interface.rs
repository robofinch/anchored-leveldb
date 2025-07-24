use std::cmp::Ordering;

use generic_container::{FragileContainer, GenericContainer};


// TODO: add comments describing the time complexity of various operations.


/// A minimal [skiplist] interface which allows entries to be inserted but never removed.
///
/// Implementations may or may not be threadsafe. Even if an implementation is threadsafe,
/// newly-added entries may or may not be seen immediately by other threads.
///
/// [skiplist]: https://en.wikipedia.org/wiki/Skip_list
pub trait Skiplist<Cmp: Comparator>: Sized {
    type Iter<'a>:    SkiplistIterator<'a> where Self: 'a;
    type LendingIter: SkiplistLendingIterator;

    /// Create and insert an entry of length `entry_len` into the skiplist, initializing the entry
    /// with `init_entry`.
    ///
    /// Even if the entry compares equal to something already in the skiplist, it is added.
    ///
    /// # Panics or Deadlocks
    /// Implementatations may panic or deadlock if the `init_entry` callback attempts to
    /// insert anything into this skiplist.
    fn insert_with<F: FnOnce(&mut [u8])>(&mut self, entry_len: usize, init_entry: F);

    /// Insert the provided data into the skiplist, incurring a copy to create an owned version of
    /// the entry.
    ///
    /// Even if the entry compares equal to something already in the skiplist, it is added.
    #[inline]
    fn insert_copy(&mut self, entry: &[u8]) {
        self.insert_with(
            entry.len(),
            |created_entry| created_entry.copy_from_slice(entry),
        );
    }

    /// Check whether the entry, or something which compares as equal to the entry, is in
    /// the skiplist.
    #[must_use]
    fn contains(&self, entry: &[u8]) -> bool;

    /// Get an iterator that can seek through the skiplist to read entries.
    #[must_use]
    fn iter(&self) -> Self::Iter<'_>;

    /// Get a lending iterator that can seek through the skiplist to read entries.
    ///
    /// Unlike a normal iterator, the lifetime of the item returned by `next` (and, here, `prev`
    /// or `current`) is not fixed, and instead varies with the input `self` reference. Since
    /// getting another item requires another reference to `self`, only one item can be used at a
    /// time. However, in the case of these skiplist iterators, mutable borrows need not invalidate
    /// the returned entries; so long as the skiplist iterator (and/or a reference-counted clone of
    /// the skiplist it came from, if available) has not been dropped or otherwise invalidated, the
    /// entries are still usable. If an implementation of `Skiplist` is trusted, it should be sound
    /// to perform unsafe lifetime extension of the returned entries.
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
    fn seek_to_end(&mut self);
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
/// Since getting another item requires another reference to `self`, only one item can be used at a
/// time. However, mutable borrows do not actually invalidate the returned entries; so long as the
/// skiplist iterator (and/or a reference-counted clone of the skiplist it came from, if available)
/// has not been dropped or otherwise invalidated, the entries are still usable. If an
/// implementation of `Skiplist` is trusted, it should be sound to perform unsafe lifetime extension
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
    /// [`current()`]: SkiplistIterator::current
    /// [`next()`]: Iterator::next
    #[must_use]
    fn is_valid(&self) -> bool;

    /// Reset the iterator to its initial position, before the first entry and after the last
    /// entry (if there are any entries in the skiplist). The iterator will then not be [valid].
    ///
    /// [valid]: SkiplistIterator::is_valid
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
    /// [invalid]: SkiplistIterator::is_valid
    fn seek(&mut self, min_bound: &[u8]);

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
    fn seek_to_end(&mut self);
}

/// Interface for comparing entries in a [`Skiplist`].
pub trait Comparator {
    /// Compare two entries in a [`Skiplist`].
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

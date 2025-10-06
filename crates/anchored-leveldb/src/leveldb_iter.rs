use crate::table_traits::trait_equivalents::LevelDBComparator;
use crate::format::{InternalEntry, LookupKey};


/// An `InternalIterator` provides access to the internal entries of some of a LevelDB database's
/// tables and/or memtables.
///
/// The iterator's entries must be sorted first with respect to `Cmp` applied to the user keys,
/// then by decreasing order of sequence number, and lastly by decreasing order of [`EntryType`].
///
/// See [`CursorLendingIterator`] and [`Seekable`] for more about the iterator-related functions.
/// See [`InternalComparator`] for more about the sorting order.
///
/// [`EntryType`]: crate::public_format::EntryType
/// [`InternalComparator`]: crate::table_traits::adapters::InternalComparator
/// [`CursorLendingIterator`]: seekable_iterator::CursorLendingIterator
/// [`Seekable`]: seekable_iterator::Seekable
pub(crate) trait InternalIterator<Cmp: LevelDBComparator> {
    /// Determine whether the iterator is currently at any value in the collection.
    /// If the iterator is invalid, then it is conceptually one position before the first entry
    /// and one position after the last entry. (Or, there may be no entries.)
    ///
    /// [`current()`] will be `Some` if and only if the iterator is valid.
    ///
    /// [`current()`]: InternalIterator::current
    #[must_use]
    fn valid(&self) -> bool;

    /// Move the iterator one position forwards, and return the entry at that position.
    /// Returns `None` if the iterator was at the last entry.
    fn next(&mut self) -> Option<InternalEntry<'_>>;

    /// Get the current value the iterator is at, if the iterator is [valid].
    ///
    /// [valid]: InternalIterator::valid
    #[must_use]
    fn current(&self) -> Option<InternalEntry<'_>>;

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was at the first entry.
    fn prev(&mut self) -> Option<InternalEntry<'_>>;

    /// Reset the iterator to its initial position, before the first entry and after the last
    /// entry (if there are any entries in the collection).
    ///
    /// The iterator becomes `!valid()`, and is conceptually one position before the first entry
    /// and one position after the last entry (if there are any entries in the collection).
    fn reset(&mut self);

    /// Move the iterator to the smallest key which is greater or equal than the provided
    /// `min_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    fn seek(&mut self, min_bound: LookupKey<'_>);

    /// Move the iterator to the greatest key which is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// Some implementations may have worse performance for `seek_before` than [`seek`].
    ///
    /// [`seek`]: InternalIterator::seek
    fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>);

    /// Move the iterator to the smallest key in the collection.
    ///
    /// If the collection is empty, the iterator is `!valid()`.
    fn seek_to_first(&mut self);

    /// Move the iterator to the greatest key in the collection.
    ///
    /// If the collection is empty, the iterator is `!valid()`.
    fn seek_to_last(&mut self);
}

#[derive(Debug)]
pub(crate) enum UnsyncInternalIter</*Cmp: LevelDBComparator*/> {
    Memtable(),
    Table(),
    Level(),
}

// pub struct InternalIterAdapter

// pub struct MergedInternalIter

// pub struct UnsyncLevelDBIter

use crate::leveldb_generics::{LdbOptionalTableIter, LdbTableIter, LevelDBGenerics};
use crate::memtable::{MemtableIter, MemtableLendingIter, MemtableSkiplist, UnsyncMemtableSkiplist};
use crate::table_file::read_table::InternalTableIter;
use crate::table_traits::trait_equivalents::LevelDBComparator;
use crate::format::{InternalEntry, LookupKey};
use crate::version::level_iter::DisjointLevelIter;


/// An `InternalIterator` provides access to the internal entries of some of a LevelDB database's
/// tables and/or memtables.
///
/// The iterator's entries should be sorted first with respect to `Cmp` applied to the user keys,
/// then by decreasing order of sequence number, and lastly by decreasing order of [`EntryType`].
///
/// However, if database corruption occurs, all bets are off in regards to exactly what is returned;
/// it is only guaranteed that no panics (TODO: fulfill this promise) or memory unsafety will
/// occur in such a case. (TODO: check whether `lender` and perhaps `lending-iterator` fulfill that
/// same guarantee; in any case, make sure it's documented that a corrupted database can result
/// in nonsensical iterator results.)
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

pub(crate) enum InternalIter<LDBG: LevelDBGenerics> {
    Memtable(MemtableLendingIter<LDBG::Cmp, LDBG::Skiplist>),
    Table(InternalTableIter<LDBG>),
    Level(DisjointLevelIter<LDBG>),
}

// TODO: probably need to adapt `InternalIter` to a `SeekableLendingIterator` for use in a
// `seekable_iterator::MergingIter`.

// macro_rules! delegate {
//     (
//         $(
//             $(#[$meta:meta])?
//             fn $fn_name:ident(
//                 & $($mut:ident)?| $self:ident $(, $arg:ident: $arg_ty:ty)?
//             ) $(-> $return_ty:ty)?;
//         )*
//     ) => {
//         $(
//             $(#[$meta])?
//             fn $fn_name(
//                 & $($mut)? $self $(, $arg: $arg_ty)?
//             ) $(-> $return_ty)? {
//                 match $self {
//                     Self::Memtable(iter) => iter.$fn_name($($arg)?),
//                     Self::Table(iter)    => iter.$fn_name($($arg)?),
//                     Self::Level(iter)    => iter.$fn_name($($arg)?),
//                 }
//             }
//         )*
//     };
// }

// impl<LDBG: LevelDBGenerics> InternalIterator<LDBG::Cmp> for InternalIter<LDBG> {
//     delegate! {
//         #[must_use]
//         fn valid(&| self) -> bool;

//         fn next(&mut| self) -> Option<InternalEntry<'_>>;
//         #[must_use]
//         fn current(&| self) -> Option<InternalEntry<'_>>;
//         fn prev(&mut| self) -> Option<InternalEntry<'_>>;

//         fn reset(&mut| self);
//         fn seek(&mut| self, min_bound: LookupKey<'_>);
//         fn seek_before(&mut| self, strict_upper_bound: LookupKey<'_>);
//         fn seek_to_first(&mut| self);
//         fn seek_to_last(&mut| self);
//     }
// }

// TODO: update anchored-pool and seekable-iterator
// pub struct GenericLevelDBIter<LDBG>

#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style lifetime extension"),
)]

#[cfg(not(feature = "polonius"))]
use std::slice;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::MirroredClone as _;
use seekable_iterator::{
    CursorLendingIterator, ItemToKey, LendItem, LentItem, MergingIter, Seekable,
};

use anchored_sstable::adapters::ComparatorAdapter;

use crate::leveldb_generics::LevelDBGenerics;
use crate::memtable::MemtableLendingIter;
use crate::public_format::EntryType;
use crate::snapshot::Snapshot;
use crate::table_file::read_table::InternalTableIter;
use crate::table_traits::adapters::InternalComparator;
use crate::table_traits::trait_equivalents::LevelDBComparator;
use crate::format::{
    EncodedInternalEntry, InternalKey, LookupKey, UserKey, UserValue,
};
use crate::version::level_iter::DisjointLevelIter;


/// An `InternalIterator` provides access to the internal entries of a LevelDB database's
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
    fn next(&mut self) -> Option<EncodedInternalEntry<'_>>;

    /// Get the current value the iterator is at, if the iterator is [valid].
    ///
    /// [valid]: InternalIterator::valid
    #[must_use]
    fn current(&self) -> Option<EncodedInternalEntry<'_>>;

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was at the first entry.
    fn prev(&mut self) -> Option<EncodedInternalEntry<'_>>;

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

impl<LDBG: LevelDBGenerics> Debug for InternalIter<LDBG> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
       let iter_type = match self {
            Self::Memtable(_) => "InternalIter::Memtable",
            Self::Table(_)    => "InternalIter::Table",
            Self::Level(_)    => "InternalIter::Level",
        };
        f.debug_tuple(iter_type).finish_non_exhaustive()
    }
}

// TODO: the below is so very, very scuffed. At this point, I just want to get something
// working, but then I massively need to clean this up.

macro_rules! delegate {
    (
        $(
            $(#[$meta:meta])*
            fn $fn_name:ident(
                & $($mut:ident)?| $self:ident $(, $arg:ident: $arg_ty:ty)?
            ) $(-> $return_ty:ty)?;
        )*
    ) => {
        $(
            $(#[$meta])*
            fn $fn_name(
                & $($mut)? $self $(, $arg: $arg_ty)?
            ) $(-> $return_ty)? {
                match $self {
                    Self::Memtable(iter) => iter.$fn_name($($arg)?),
                    Self::Table(iter)    => iter.$fn_name($($arg)?),
                    Self::Level(iter)    => iter.$fn_name($($arg)?),
                }
            }
        )*
    };
}

impl<LDBG: LevelDBGenerics> InternalIterator<LDBG::Cmp> for InternalIter<LDBG> {
    delegate! {
        #[inline]
        fn valid(&| self) -> bool;

        fn next(&mut| self) -> Option<EncodedInternalEntry<'_>>;
        #[inline]
        fn current(&| self) -> Option<EncodedInternalEntry<'_>>;
        fn prev(&mut| self) -> Option<EncodedInternalEntry<'_>>;

        fn reset(&mut| self);
        fn seek(&mut| self, min_bound: LookupKey<'_>);
        fn seek_before(&mut| self, strict_upper_bound: LookupKey<'_>);
        fn seek_to_first(&mut| self);
        fn seek_to_last(&mut| self);
    }
}

impl<'a, LDBG: LevelDBGenerics> LendItem<'a> for InternalIter<LDBG> {
    type Item = EncodedInternalEntry<'a>;
}

impl<LDBG: LevelDBGenerics> ItemToKey<[u8]> for InternalIter<LDBG> {
    fn item_to_key(item: LentItem<'_, Self>) -> &'_ [u8] {
        item.encoded_internal_key().0
    }
}

impl<LDBG: LevelDBGenerics> CursorLendingIterator for InternalIter<LDBG> {
    #[inline]
    fn valid(&self) -> bool {
        InternalIterator::valid(self)
    }

    fn next(&mut self) -> Option<LentItem<'_, Self>> {
        InternalIterator::next(self)
    }

    #[inline]
    fn current(&self) -> Option<LentItem<'_, Self>> {
        InternalIterator::current(self)
    }

    fn prev(&mut self) -> Option<LentItem<'_, Self>> {
        InternalIterator::prev(self)
    }
}

impl<LDBG: LevelDBGenerics> Seekable<[u8], ComparatorAdapter<InternalComparator<LDBG::Cmp>>>
for InternalIter<LDBG>
{
    fn reset(&mut self) {
        InternalIterator::reset(self);
    }

    fn seek(&mut self, min_bound: &[u8]) {
        InternalIterator::seek(self, LookupKey::new_unchecked(min_bound));
    }

    fn seek_before(&mut self, strict_upper_bound: &[u8]) {
        InternalIterator::seek_before(self, LookupKey::new_unchecked(strict_upper_bound));
    }

    fn seek_to_first(&mut self) {
        InternalIterator::seek_to_first(self);
    }

    fn seek_to_last(&mut self) {
        InternalIterator::seek_to_last(self);
    }
}

pub(crate) struct InnerGenericDBIter<LDBG: LevelDBGenerics> {
    /// If `valid()`, its `current()` must be at a `Value` entry whose sequence number is
    /// the greatest sequence number less than `self`'s sequence number, among the sequence numbers
    /// of entries for the user key of `current()`.
    iter:     MergingIter<
        [u8],
        ComparatorAdapter<InternalComparator<LDBG::Cmp>>,
        InternalIter<LDBG>,
    >,
    cmp:      InternalComparator<LDBG::Cmp>,
    snapshot: Snapshot<LDBG::Refcounted, LDBG::RwCell>,
    /// Either empty, or the current user key.
    current:  Vec<u8>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics> InnerGenericDBIter<LDBG> {
    #[must_use]
    pub fn new(
        cmp:      InternalComparator<LDBG::Cmp>,
        iters:    Vec<InternalIter<LDBG>>,
        snapshot: Snapshot<LDBG::Refcounted, LDBG::RwCell>,
    ) -> Self {
        Self {
            iter:    MergingIter::new(iters, ComparatorAdapter(cmp.mirrored_clone())),
            cmp,
            snapshot,
            current: Vec::new(),
        }
    }

    /// Scan in the indicated direction until either the end of the iterator or an entry with a
    /// user key different to `self.current`' is reached.
    ///
    /// This function assumes that `!self.current.is_empty()`, and clears `self.current`
    /// before returning.
    fn scan_to_different_user_key<const NEXT: bool>(&mut self) {
        loop {
            let next_or_prev = if NEXT {
                self.iter.next()
            } else {
                self.iter.prev()
            };

            let Some(next_or_prev) = next_or_prev else {
                break;
            };
            let next_or_prev_user = InternalKey::decode(next_or_prev.encoded_internal_key())
                .unwrap().user_key;

            if self.cmp.cmp_user(UserKey(&self.current), next_or_prev_user).is_ne() {
                break;
            }
        }

        self.current.clear();
    }

    /// Seek before the user key in `self.current`, using `self.iter.seek_before(_)`.
    ///
    /// This function assumes that `!self.current.is_empty()`, and clears `self.current`
    /// before returning.
    fn seek_before_current(&mut self) {
        let mut lookup_buffer = Vec::new();
        let lookup_key = LookupKey::new(
            &mut lookup_buffer,
            UserKey(&self.current),
            self.snapshot.sequence_number(),
        );

        self.iter.seek(lookup_key.encoded_internal_key().0);
        self.current.clear();
    }

    /// Return the next non-deleted value with a LE sequence number, starting at wherever
    /// `self.iter.current()` is.
    ///
    /// `self.current` is assumed to be empty (regardless of whether the iterator is valid or not).
    fn inner_next(&mut self) -> Option<(UserKey<'_>, UserValue<'_>)> {
        loop {
            // Scan to the next entry with a LE sequence number
            let next = self.iter.current()?;
            let decoded_next = InternalKey::decode(next.encoded_internal_key()).unwrap();

            if decoded_next.sequence_number > self.snapshot.sequence_number() {
                self.iter.next()?;
                continue;
            }

            // Either use `current` as a temporary buffer to seek forward, or genuinely
            // as the current user key of the entry we're returning.
            self.current.extend(next.encoded_internal_key().user_key().unwrap().0);

            match decoded_next.entry_type {
                EntryType::Deletion => {
                    // This key is deleted. Scan to the next user key.
                    // Clears `self.current`.
                    self.scan_to_different_user_key::<true>();
                }
                EntryType::Value => {
                    // `next` contains a Value entry with a LE sequence number, of a user key
                    // following that of the previous `self.current()` entry, and even if the
                    // value of the user key has since been updated or deleted, this is the
                    // current value as of sequence number `self.snapshot.sequence_number()`.

                    let user_key = decoded_next.user_key.0;
                    let user_value = next.value_bytes();

                    // SAFETY: `user_key.as_ptr()` is non-null, properly aligned, valid for reads of
                    // `user_key.len()` bytes, points to `key.len()`-many valid bytes, and doesn't
                    // have too long of a length, since it came from a valid slice.
                    // The sole remaining constraint is the lifetime. Rust's aliasing rules
                    // are clearly satisfied, since this compiles under Polonius.
                    #[cfg(not(feature = "polonius"))]
                    let user_key = unsafe {
                        slice::from_raw_parts(user_key.as_ptr(), user_key.len())
                    };
                    // SAFETY: Same as above.
                    #[cfg(not(feature = "polonius"))]
                    let user_value = unsafe {
                        slice::from_raw_parts(user_value.as_ptr(), user_value.len())
                    };

                    return Some((UserKey(user_key), UserValue(user_value)));
                }
            }
        }
    }

    /// Return the previous non-deleted value with the greatest LE sequence number for the current
    /// user key, starting at wherever `self.iter.current()` is.
    ///
    /// `self.current` is assumed to be empty (regardless of whether the iterator is valid or not).
    fn inner_prev(&mut self) -> Option<(UserKey<'_>, UserValue<'_>)> {
        loop {
            let prev_key = InternalKey::decode(
                self.iter.current()?.encoded_internal_key(),
            ).unwrap();
            let prev_sequence_number = prev_key.sequence_number;

            if prev_sequence_number > self.snapshot.sequence_number() {
                // Go to the preceding user key.
                self.current.extend(prev_key.user_key.0);
                self.scan_to_different_user_key::<false>();
                continue;
            }

            self.current.extend(prev_key.user_key.0);

            // Seek to the preceding entry which either has a greater user key, a greater sequence
            // number, or is None.
            while let Some(maybe_before_prev) = self.iter.prev() {
                let maybe_before_prev = InternalKey::decode(
                    maybe_before_prev.encoded_internal_key(),
                ).unwrap();

                if maybe_before_prev.sequence_number > self.snapshot.sequence_number()
                    || self.cmp.cmp_user(maybe_before_prev.user_key, UserKey(&self.current)).is_gt()
                {
                    break;
                }
            }

            // We reached one entry _before_ the desired prev.
            let Some(prev) = self.iter.next() else {
                // Note that we _should_ have that `self.iter.next()` is `Some`, but maybe
                // corruption was encountered (which could result in a `None` return).
                // In that case, consider the current key to be corrupt. We could try to rectify
                // the situation, but that risks an infinite loop. Go further backwards.
                self.seek_before_current();
                continue;
            };

            let decoded_prev_key = InternalKey::decode(prev.encoded_internal_key()).unwrap();

            // NOTE: if corruption was encountered and skipped past, we _could_, hypothetically,
            // have gotten something with a completely wrong user key and sequence number.
            // mghhhhhhh I don't like that. TODO: update MergingIter with some sort of `peek_prev`
            // (and `peek_next`) methods which take callbacks. `peek_prev` support would also
            // be needed down the chain in `InternalIter`.
            match self.cmp.cmp_user(decoded_prev_key.user_key, UserKey(&self.current)) {
                // We somehow went too far backwards. Continue on to the next iteration, I guess.
                Ordering::Less => continue,
                Ordering::Equal => {
                    if decoded_prev_key.sequence_number > self.snapshot.sequence_number() {
                        // We somehow got to too large of a sequence number. As above, we
                        // _could_ try to go forwards to rectify the situation, but... any
                        // such attempt might fall into an infinite loop. Continue backwards.
                        continue;
                    } else {
                        // This is the desired situation. Continue on to attempt return.
                    }
                }
                Ordering::Greater => {
                    // This is bad. `self.iter.next()` went too far forwards. Use `seek_before`
                    // to continue backwards, to avoid falling into an infinite loop.
                    self.seek_before_current();
                    continue;
                }
            }

            match decoded_prev_key.entry_type {
                EntryType::Deletion => {
                    // This key is deleted. Scan to the next user key.
                    // Clears `self.current`.
                    self.scan_to_different_user_key::<false>();
                }
                EntryType::Value => {
                    // `prev` contains a Value entry with a LE sequence number, of a user key
                    // preceding that of the previous `self.current()` entry, and even if the
                    // value of the user key has since been updated or deleted, this is the
                    // current value as of sequence number `self.snapshot.sequence_number()`.

                    let user_key = decoded_prev_key.user_key.0;
                    let user_value = prev.value_bytes();

                    // SAFETY: `user_key.as_ptr()` is non-null, properly aligned, valid for reads of
                    // `user_key.len()` bytes, points to `key.len()`-many valid bytes, and doesn't
                    // have too long of a length, since it came from a valid slice.
                    // The sole remaining constraint is the lifetime. Rust's aliasing rules
                    // are clearly satisfied, since this compiles under Polonius.
                    #[cfg(not(feature = "polonius"))]
                    let user_key = unsafe {
                        slice::from_raw_parts(user_key.as_ptr(), user_key.len())
                    };
                    // SAFETY: Same as above.
                    #[cfg(not(feature = "polonius"))]
                    let user_value = unsafe {
                        slice::from_raw_parts(user_value.as_ptr(), user_value.len())
                    };

                    return Some((UserKey(user_key), UserValue(user_value)));
                }
            }
        }
    }
}

/// A `InnerGenericDBIter` provides access to the user entries of some of a LevelDB database.
///
/// However, if database corruption occurs, all bets are off in regards to exactly what is returned;
/// it is only guaranteed that no panics (TODO: fulfill this promise) or memory unsafety will
/// occur in such a case. (TODO: check whether `lender` and perhaps `lending-iterator` fulfill that
/// same guarantee; in any case, make sure it's documented that a corrupted database can result
/// in nonsensical iterator results.)
///
/// See [`CursorLendingIterator`] and [`Seekable`] for more about the iterator-related functions.
///
/// [`CursorLendingIterator`]: seekable_iterator::CursorLendingIterator
/// [`Seekable`]: seekable_iterator::Seekable
#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics> InnerGenericDBIter<LDBG> {
    /// Determine whether the iterator is currently at any value in the collection.
    /// If the iterator is invalid, then it is conceptually one position before the first entry
    /// and one position after the last entry. (Or, there may be no entries.)
    ///
    /// [`current()`] will be `Some` if and only if the iterator is valid.
    ///
    /// [`current()`]: InternalIterator::current
    #[inline]
    #[must_use]
    pub fn valid(&self) -> bool {
        self.iter.valid()
    }

    /// Move the iterator one position forwards, and return the entry at that position.
    /// Returns `None` if the iterator was at the last entry.
    pub fn next(&mut self) -> Option<(UserKey<'_>, UserValue<'_>)> {
        if self.current.is_empty() {
            // If we return `None` here, we're in a valid state since `self.current` is empty.
            // Same goes for later early returns where we know `self.current` is empty.
            self.iter.next()?;
        } else {
            // Seek forwards until `self.iter.current()` has a different user key.
            // Clears `self.current`.
            self.scan_to_different_user_key::<true>();
        }

        // Once we get here, we need to get the next non-deleted value with a LE sequence number.
        // We don't need to compare with `self.current`.
        self.inner_next()
    }

    /// Get the current value the iterator is at, if the iterator is [valid].
    ///
    /// [valid]: InternalIterator::valid
    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<(UserKey<'_>, UserValue<'_>)> {
        self.iter.current().map(|entry| {
            let entry = entry.decode();
            (entry.user_key, entry.value.expect("invariant of InnerGenericDBIter"))
        })
    }

    /// Move the iterator one position back, and return the entry at that position.
    /// Returns `None` if the iterator was at the first entry.
    ///
    /// # Speed Warning
    /// Backwards iteration is much slower than forwards iteration.
    pub fn prev(&mut self) -> Option<(UserKey<'_>, UserValue<'_>)> {
        if self.current.is_empty() {
            // If we return `None` here, we're in a valid state since `self.current` is empty.
            // Same goes for later early returns where we know `self.current` is empty.
            self.iter.prev()?;
        } else {
            // Seek backwards until `self.iter.current()` has a different user key.
            // Clears `self.current`.
            self.scan_to_different_user_key::<false>();
        }

        // Get the previous non-deleted value with a LE sequence number.
        self.inner_prev()
    }

    /// Reset the iterator to its initial position, before the first entry and after the last
    /// entry (if there are any entries in the collection).
    ///
    /// The iterator becomes `!valid()`, and is conceptually one position before the first entry
    /// and one position after the last entry (if there are any entries in the collection).
    pub fn reset(&mut self) {
        self.iter.reset();
    }

    /// Move the iterator to the smallest key which is greater or equal than the provided
    /// `min_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    pub fn seek(&mut self, min_bound: UserKey<'_>) {
        let mut lookup_buffer = Vec::new();
        let lookup_key = LookupKey::new(
            &mut lookup_buffer,
            min_bound,
            self.snapshot.sequence_number(),
        );

        self.iter.seek(lookup_key.encoded_internal_key().0);

        // Get the next non-deleted value with a LE sequence number.
        drop(lookup_buffer);
        self.current.clear();
        self.inner_next();
    }

    /// Move the iterator to the greatest key which is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the collection).
    ///
    /// # Speed Warning
    /// Backwards iteration is much slower than forwards iteration.
    ///
    /// [`seek`]: InternalIterator::seek
    pub fn seek_before(&mut self, strict_upper_bound: UserKey<'_>) {
        let mut lookup_buffer = Vec::new();
        let lookup_key = LookupKey::new(
            &mut lookup_buffer,
            strict_upper_bound,
            self.snapshot.sequence_number(),
        );

        self.iter.seek_before(lookup_key.encoded_internal_key().0);

        // Get the previous non-deleted value with a LE sequence number.
        drop(lookup_buffer);
        self.current.clear();
        self.inner_prev();
    }

    /// Move the iterator to the smallest key in the collection.
    ///
    /// If the collection is empty, the iterator is `!valid()`.
    pub fn seek_to_first(&mut self) {
        self.iter.seek_to_first();

        // Get the next non-deleted value with a LE sequence number.
        self.current.clear();
        self.inner_next();
    }

    /// Move the iterator to the greatest key in the collection.
    ///
    /// If the collection is empty, the iterator is `!valid()`.
    ///
    /// # Speed Warning
    /// Backwards iteration is much slower than forwards iteration.
    pub fn seek_to_last(&mut self) {
        self.iter.seek_to_last();

        // Get the previous non-deleted value with a LE sequence number.
        self.current.clear();
        self.inner_prev();
    }
}

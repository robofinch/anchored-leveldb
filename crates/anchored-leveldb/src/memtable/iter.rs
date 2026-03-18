#![expect(unsafe_code, reason = "deconstruct a type (`MemtableLendingIter`) which impls Drop")]

use std::mem::ManuallyDrop;

use crate::pub_traits::cmp_and_policy::LevelDBComparator;
use crate::typed_bytes::{EncodedInternalEntry, LookupKey};
use super::pool::MemtablePool;
use super::format::{MemtableSkiplistIter, MemtableSkiplistLendingIter};


pub(crate) struct MemtableIter<'a, Cmp> {
    iter: MemtableSkiplistIter<'a, Cmp>,
}

impl<'a, Cmp> MemtableIter<'a, Cmp> {
    #[inline]
    #[must_use]
    pub(super) const fn new(iter: MemtableSkiplistIter<'a, Cmp>) -> Self {
        Self { iter }
    }
}

impl<'a, Cmp: LevelDBComparator> Iterator for MemtableIter<'a, Cmp> {
    type Item = EncodedInternalEntry<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    #[inline]
    fn fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        self.iter.fold(init, f)
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, Cmp: LevelDBComparator> MemtableIter<'a, Cmp> {
    /// Get an entry with the given user key and a sequence number less than or equal to the given
    /// sequence number in the lookup key, if there is such an entry in the memtable. If there are
    /// multiple such entries, the one with the greatest sequence number is returned.
    #[must_use]
    pub(super) fn get(mut self, lookup_key: LookupKey<'_>) -> Option<EncodedInternalEntry<'a>> {
        // Since `MemtableComparator` sorts sequence numbers and entry types in decreasing order,
        // and since we use `EntryType::MAX_TYPE` in the lookup key,
        // this either finds:
        // 1. nothing
        // 2. an entry with a different user key
        // 3. an entry with the correct user key and a sequence number less than or equal to
        //    the sequence number being looked up; if that describes multiple entries,
        //    the one with the greatest sequence number is returned.
        self.seek(lookup_key);

        if let Some(entry) = self.current() {
            if self.iter.skiplist_cmp().cmp_user(entry.user_key(), lookup_key.0).is_eq() {
                // Case 3
                return Some(entry);
            }
        }

        // Case 1 or 2
        None
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.iter.valid()
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<EncodedInternalEntry<'a>> {
        self.iter.current()
    }

    #[must_use]
    pub fn prev(&mut self) -> Option<EncodedInternalEntry<'a>> {
        // Every internal key in the memtable is given a unique sequence number, so there are no
        // duplicate keys.
        self.iter.prev_without_duplicates()
    }

    pub const fn reset(&mut self) {
        self.iter.reset();
    }

    pub fn seek(&mut self, lower_bound: LookupKey<'_>) {
        self.iter.seek(lower_bound.as_internal_key());
    }

    pub fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        self.iter.seek_before(strict_upper_bound.as_internal_key());
    }

    pub fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    pub fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }
}

pub(crate) struct MemtableLendingIter<Cmp: LevelDBComparator> {
    iter: ManuallyDrop<MemtableSkiplistLendingIter<Cmp>>,
    pool: MemtablePool<Cmp>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: LevelDBComparator> MemtableLendingIter<Cmp> {
    #[inline]
    #[must_use]
    pub(super) const fn new(
        iter: MemtableSkiplistLendingIter<Cmp>,
        pool: MemtablePool<Cmp>,
    ) -> Self {
        Self {
            iter: ManuallyDrop::new(iter),
            pool,
        }
    }

    #[inline]
    #[must_use]
    pub fn valid(&self) -> bool {
        self.iter.valid()
    }

    #[inline]
    #[must_use]
    pub fn next(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.next()
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.current()
    }

    #[must_use]
    pub fn prev(&mut self) -> Option<EncodedInternalEntry<'_>> {
        // Every internal key in the memtable is given a unique sequence number, so there are no
        // duplicate keys.
        self.iter.prev_without_duplicates()
    }

    pub fn reset(&mut self) {
        self.iter.reset();
    }

    pub fn seek(&mut self, lower_bound: LookupKey<'_>) {
        self.iter.seek(lower_bound.as_internal_key());
    }

    pub fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        self.iter.seek_before(strict_upper_bound.as_internal_key());
    }

    pub fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    pub fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }
}

impl<Cmp: LevelDBComparator> Drop for MemtableLendingIter<Cmp> {
    fn drop(&mut self) {
        // SAFETY: Since this is the destructor of the struct which owns the `ManuallyDrop`
        // value, and since we do not use it again in this function (not even by moving it),
        // this is sound.
        let iter = unsafe { ManuallyDrop::take(&mut self.iter) };
        self.pool.return_reader(iter.into_skiplist());
    }
}

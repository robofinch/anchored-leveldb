#![expect(unsafe_code, reason = "allow skiplists to be externally synchronized")]

mod skiplists;


use clone_behavior::{Fast, MirroredClone, Speed};
use seekable_iterator::{CursorIterator as _, CursorLendingIterator as _, Seekable as _};

use crate::{
    leveldb_iter::InternalIterator, public_format::WriteEntry,
    table_traits::trait_equivalents::LevelDBComparator, write_batch::WriteBatch,
};
use crate::format::{
    EncodedMemtableEntry, InternalEntry, LookupKey,
    MemtableEntry, MemtableEntryEncoder, SequenceNumber,
};
pub(crate) use self::skiplists::{
    MemtableSkiplist, SyncMemtableSkiplist, SyncWriteAccess,
    UnsyncMemtableSkiplist, UnsyncWriteAccess, WriteAccess,
};


#[derive(Debug)]
pub(crate) struct Memtable<Cmp, Skiplist> {
    /// Only [`Memtable::insert_write_entry`] inserts anything into this skiplist.
    list:              Skiplist,
    cmp:               Cmp,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp, Skiplist> Memtable<Cmp, Skiplist>
where
    Cmp:      LevelDBComparator + MirroredClone<Fast>,
    Skiplist: MemtableSkiplist<Cmp>,
{
    #[inline]
    #[must_use]
    pub fn new(cmp: Cmp) -> Self {
        Self {
            list: Skiplist::new(cmp.fast_mirrored_clone()),
            cmp,
        }
    }

    /// # Safety
    /// The `MemtableWriteAccess` borrow must be unique across all reference-counted clones of a
    /// given `Memtable` for its entire lifetime.
    ///
    /// It suffices for `externally_synchronized` to only ever be called on a certain instance of
    /// the `Memtable`; the `&mut` borrow can ensure that access is unique for a single
    /// `Self` instance, and if the `externally_synchronized` method is never used on other
    /// reference-counted clones of `self`, then access is unique across all of them.
    unsafe fn externally_synchronized(&mut self) -> MemtableWriteAccess<'_, Cmp, Skiplist> {
        // SAFETY:
        // The caller guarantees that the `MemtableWriteAccess` borrow and thus also
        // the contained `list_write_access` borrow is unique, for its entire lifetime, across all
        // reference-counted clones of the `Memtable` and thus also its skiplist.
        // We never let the skiplist leak outside of the memtable.
        let list_write_access = unsafe { self.list.externally_synchronized() };
        MemtableWriteAccess {
            list: list_write_access,
        }
    }

    /// Get an entry with the given user key and a sequence number less than or equal to the given
    /// sequence number in the lookup key, if there is such an entry in the memtable. If there are
    /// multiple such entries, the one with the greatest sequence number is returned.
    #[must_use]
    pub fn get<'a>(&'a self, lookup_key: LookupKey<'_>) -> Option<InternalEntry<'a>> {
        let mut iter = self.iter();
        // Since `MemtableComparator` sorts sequence numbers and entry types in decreasing order,
        // and since we use `EntryType::MAX_TYPE` in the lookup key,
        // this either finds:
        // 1. nothing
        // 2. an entry with a different user key
        // 3. an entry with the correct user key and a sequence number less than or equal to
        //    the sequence number being looked up; if that describes multiple entries,
        //    the one with the greatest sequence number is returned.
        iter.seek(lookup_key);

        if let Some(memtable_entry) = iter.current() {
            let memtable_entry = MemtableEntry::decode(memtable_entry);

            if self.cmp.cmp(memtable_entry.user_key.0, lookup_key.user_key().0).is_eq() {
                // Case 3
                return Some(memtable_entry.internal_entry());
            }
        }

        // Case 1 or 2
        None
    }

    /// Returns a close lower bound for the total number of bytes allocated by this memtable.
    #[inline]
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        self.list.allocated_bytes()
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> MemtableIter<'_, Cmp, Skiplist> {
        MemtableIter::new(self)
    }

    #[inline]
    #[must_use]
    pub fn lending_iter(self) -> MemtableLendingIter<Cmp, Skiplist> {
        MemtableLendingIter::new(self)
    }
}

impl<Cmp, Skiplist, S> MirroredClone<S> for Memtable<Cmp, Skiplist>
where
    Cmp:      LevelDBComparator + MirroredClone<S>,
    Skiplist: MemtableSkiplist<Cmp> + MirroredClone<S>,
    S:        Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            list: self.list.mirrored_clone(),
            cmp:  self.cmp.mirrored_clone(),
        }
    }
}

impl<Cmp, Skiplist> Memtable<Cmp, Skiplist>
where
    Cmp:      LevelDBComparator + MirroredClone<Fast> + Default,
    Skiplist: MemtableSkiplist<Cmp>,
{
    #[inline]
    fn default() -> Self {
        Self::new(Cmp::default())
    }
}

#[derive(Debug)]
pub(crate) struct MemtableWriteAccess<'a, Cmp, Skiplist>
where
    Cmp:      LevelDBComparator,
    Skiplist: MemtableSkiplist<Cmp> + 'a,
{
    list: Skiplist::WriteAccess<'a>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp, Skiplist> MemtableWriteAccess<'_, Cmp, Skiplist>
where
    Cmp:      LevelDBComparator + MirroredClone<Fast>,
    Skiplist: MemtableSkiplist<Cmp>,
{
    /// Writes an entry into this memtable, given a unique [`SequenceNumber`].
    pub fn insert_write_entry(
        &mut self,
        write_entry:     WriteEntry<'_>,
        sequence_number: SequenceNumber,
    ) {
        // This is the only function which inserts something into the inner skiplist.
        let (entry_len, encoder) = MemtableEntryEncoder::start_encode(
            write_entry,
            sequence_number,
        );
        // Note that the `EncodedMemtableEntry` compares distinct from any other inserted
        // entry, because its sequence number is different.
        self.list.insert_with(entry_len, |entry| encoder.encode_to(entry));
    }

    /// Writes every entry in a [`WriteBatch`] into this memtable, giving a unique
    /// [`SequenceNumber`] to each.
    ///
    /// # Panics or Correctness Errors
    /// This function assumes and does not verify that
    /// `last_sequence_number.checked_add_u32(write_batch.num_entries())`
    /// is a `Some` value.
    ///
    /// That condition must be checked prior to calling this function.
    ///
    /// Additionally, this function assumes that no entry with a sequence number greater than
    /// `last_sequence_number` has been previously inserted into the memtable.
    pub fn insert_write_batch(
        &mut self,
        write_batch:          &WriteBatch,
        last_sequence_number: SequenceNumber,
    ) {
        let mut prev_seq_num = last_sequence_number.inner();

        write_batch.iter().for_each(|write_entry| {
            let current_seq_num = SequenceNumber::new_unchecked(prev_seq_num + 1);
            self.insert_write_entry(write_entry, current_seq_num);
            prev_seq_num += 1;
        });
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MemtableIter<'a, Cmp: LevelDBComparator, Skiplist: MemtableSkiplist<Cmp> + 'a> {
    iter: Skiplist::Iter<'a>,
}

impl<'a, Cmp: LevelDBComparator, Skiplist: MemtableSkiplist<Cmp>> MemtableIter<'a, Cmp, Skiplist> {
    #[inline]
    #[must_use]
    fn new(list: &'a Memtable<Cmp, Skiplist>) -> Self {
        Self {
            iter: list.list.iter(),
        }
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, Cmp: LevelDBComparator, Skiplist: MemtableSkiplist<Cmp>> MemtableIter<'a, Cmp, Skiplist> {
    #[inline]
    pub fn valid(&self) -> bool {
        self.iter.valid()
    }

    #[inline]
    pub fn next(&mut self) -> Option<EncodedMemtableEntry<'a>> {
        // Since the sole place we insert anything into the inner list is `encoder.encode_to`
        // inside `Self::insert_write_entry`, we know that every entry in the inner list
        // is a valid `EncodedMemtableEntry` (barring a bug).
        self.iter.next().map(EncodedMemtableEntry::new_unchecked)
    }

    #[inline]
    pub fn current(&self) -> Option<EncodedMemtableEntry<'a>> {
        // See above
        self.iter.current().map(EncodedMemtableEntry::new_unchecked)
    }

    pub fn prev(&mut self) -> Option<EncodedMemtableEntry<'a>> {
        // See above
        self.iter.prev().map(EncodedMemtableEntry::new_unchecked)
    }

    pub fn reset(&mut self) {
        self.iter.reset();
    }

    pub fn seek(&mut self, min_bound: LookupKey<'_>) {
        self.iter.seek(min_bound.encoded_memtable_entry().inner());
    }

    pub fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        self.iter.seek_before(strict_upper_bound.encoded_memtable_entry().inner());
    }

    pub fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    pub fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MemtableLendingIter<Cmp: LevelDBComparator, Skiplist: MemtableSkiplist<Cmp>> {
    iter: Skiplist::LendingIter,
}

impl<Cmp: LevelDBComparator, Skiplist: MemtableSkiplist<Cmp>> MemtableLendingIter<Cmp, Skiplist> {
    #[inline]
    #[must_use]
    fn new(list: Memtable<Cmp, Skiplist>) -> Self {
        Self {
            iter: list.list.lending_iter(),
        }
    }
}

impl<Cmp: LevelDBComparator, Skiplist: MemtableSkiplist<Cmp>> InternalIterator<Cmp>
for MemtableLendingIter<Cmp, Skiplist>
{
    #[inline]
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    #[inline]
    fn next(&mut self) -> Option<InternalEntry<'_>> {
        self.iter.next().map(|encoded_memtable_entry| {
            // Since the sole place we insert anything into the inner list is `encoder.encode_to`
            // inside `Self::insert_write_entry`, we know that every entry in the inner list
            // is a valid `EncodedMemtableEntry` (barring a bug).
            let memtable_entry = EncodedMemtableEntry::new_unchecked(encoded_memtable_entry);
            let memtable_entry = MemtableEntry::decode(memtable_entry);

            memtable_entry.internal_entry()
        })
    }

    #[inline]
    fn current(&self) -> Option<InternalEntry<'_>> {
        self.iter.current().map(|encoded_memtable_entry| {
            // See above
            let memtable_entry = EncodedMemtableEntry::new_unchecked(encoded_memtable_entry);
            let memtable_entry = MemtableEntry::decode(memtable_entry);

            memtable_entry.internal_entry()
        })
    }

    fn prev(&mut self) -> Option<InternalEntry<'_>> {
        self.iter.prev().map(|encoded_memtable_entry| {
            // See above
            let memtable_entry = EncodedMemtableEntry::new_unchecked(encoded_memtable_entry);
            let memtable_entry = MemtableEntry::decode(memtable_entry);

            memtable_entry.internal_entry()
        })
    }

    fn reset(&mut self) {
        self.iter.reset();
    }

    fn seek(&mut self, min_bound: LookupKey<'_>) {
        self.iter.seek(min_bound.encoded_memtable_entry().inner());
    }

    fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        self.iter.seek_before(strict_upper_bound.encoded_memtable_entry().inner());
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }
}

#![warn(warnings)]

use std::{cell::Cell, cmp::Ordering, rc::Rc};

use clone_behavior::{AnySpeed, MirroredClone};
use seekable_iterator::{CursorIterator as _, Seekable as _};

use anchored_skiplist::{ConcurrentSkiplist, Skiplist as _, concurrent::LendingIter};

use crate::{public_format::WriteEntry, write_batch::WriteBatch};
use crate::{
    format::{
        EncodedMemtableEntry, InternalEntry, LookupKey,
        MemtableEntry, MemtableEntryEncoder, SequenceNumber,
    },
    table_traits::{adapters::MemtableComparator, trait_equivalents::LevelDBComparator},
};


// TODO: ExternallySynchronizedMemtable (no need for ThreadsafeMemtable
// and LockedThreadsafeMemtable); might want SimpleMemtable

#[derive(Debug)]
pub struct UnsyncMemtable<Cmp> {
    /// Only [`UnsyncMemtable::insert_write_entry`] inserts anything into this skiplist.
    list:              ConcurrentSkiplist<MemtableComparator<Cmp>>,
    cmp:               Cmp,
    total_entry_bytes: Rc<Cell<usize>>,
}

impl<Cmp: LevelDBComparator + MirroredClone<AnySpeed>> UnsyncMemtable<Cmp> {
    #[inline]
    #[must_use]
    pub fn new(cmp: Cmp) -> Self {
        Self {
            list:              ConcurrentSkiplist::new(MemtableComparator(cmp.mirrored_clone())),
            cmp,
            total_entry_bytes: Rc::new(Cell::new(0)),
        }
    }

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
        self.total_entry_bytes.set(self.total_entry_bytes.get() + entry_len);
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

    /// Get an entry with the given user key and a sequence number less than or equal to the given
    /// sequence number in the lookup key, if there is such an entry in the memtable. If there are
    /// multiple such entries, the one with the greatest sequence number is returned.
    #[must_use]
    pub fn get<'a>(&'a self, lookup_key: LookupKey<'_>) -> Option<InternalEntry<'a>> {
        let mut list_iter = self.list.iter();
        // Since `MemtableComparator` sorts sequence numbers and entry types in decreasing order,
        // and since we use `EntryType::MAX_TYPE` in the lookup key,
        // this either finds:
        // 1. nothing
        // 2. an entry with a different user key
        // 3. an entry with the correct user key and a sequence number less than or equal to
        //    the sequence number being looked up; if that describes multiple entries,
        //    the one with the greatest sequence number is returned.
        list_iter.seek(lookup_key.memtable_entry().inner());

        if let Some(memtable_entry) = list_iter.current() {
            // Since the sole place we insert anything into the inner list is `encoder.encode_to`
            // inside `Self::insert_write_entry`, we know that every entry in the inner list
            // is a valid `EncodedMemtableEntry` (barring a bug).
            let memtable_entry = EncodedMemtableEntry::new_unchecked(memtable_entry);
            let memtable_entry = MemtableEntry::decode(memtable_entry);

            if self.cmp.cmp(
                memtable_entry.user_key.0,
                lookup_key.user_key().0,
            ) == Ordering::Equal {
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
        // TODO: expose information from the Bump in ConcurrentSkiplist
        self.total_entry_bytes.get()
    }

    #[inline]
    #[must_use]
    pub fn lending_iter(self) -> UnsyncMemtableIter<Cmp> {
        UnsyncMemtableIter::new(self)
    }
}

impl<Cmp> UnsyncMemtable<Cmp>
where
    Cmp: LevelDBComparator + MirroredClone<AnySpeed>,
{
    #[inline]
    #[must_use]
    pub fn refcounted_clone(&self) -> Self {
        Self {
            list:              self.list.refcounted_clone(),
            cmp:               self.cmp.mirrored_clone(),
            total_entry_bytes: Rc::clone(&self.total_entry_bytes),
        }
    }
}

impl<Cmp> Default for UnsyncMemtable<Cmp>
where
    Cmp: Default + LevelDBComparator + MirroredClone<AnySpeed>,
{
    #[inline]
    fn default() -> Self {
        Self::new(Cmp::default())
    }
}

#[derive(Debug, Clone)]
pub struct UnsyncMemtableIter<Cmp: LevelDBComparator> {
    iter: LendingIter<MemtableComparator<Cmp>>,
}

impl<Cmp: LevelDBComparator> UnsyncMemtableIter<Cmp> {
    #[inline]
    #[must_use]
    fn new(list: UnsyncMemtable<Cmp>) -> Self {
        Self {
            iter: list.list.lending_iter(),
        }
    }
}

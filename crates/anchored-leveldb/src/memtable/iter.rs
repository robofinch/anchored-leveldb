use clone_behavior::{Fast, MirroredClone, Speed};
use integer_encoding::VarIntWriter as _;
use seekable_iterator::{CursorIterator as _, CursorLendingIterator as _, Seekable as _};

use crate::{
    leveldb_iter::InternalIterator, public_format::WriteEntry,
    table_traits::LevelDBComparator, write_batch::WriteBatch,
};
use crate::format::{
    EncodedInternalEntry, EncodedMemtableEntry, InternalEntry, LookupKey,
    MemtableEntry, MemtableEntryEncoder, SequenceNumber,
};
use super::{Memtable, skiplists::MemtableSkiplist};


#[derive(Debug, Clone)]
pub(crate) struct MemtableIter<'a, Cmp: LevelDBComparator, Skiplist: MemtableSkiplist<Cmp> + 'a> {
    iter: Skiplist::Iter<'a>,
}

impl<'a, Cmp: LevelDBComparator, Skiplist: MemtableSkiplist<Cmp>> MemtableIter<'a, Cmp, Skiplist> {
    #[inline]
    #[must_use]
    pub(super) fn new(list: &'a Memtable<Cmp, Skiplist>) -> Self {
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
        // TODO: improve the skiplist interface and make this unnecessary.
        let mut buffer = Vec::new();
        let encoded_internal_key = min_bound.encoded_internal_key();
        let encoded_internal_key_len_u32 = u32::try_from(encoded_internal_key.0.len()).unwrap();
        buffer.write_varint(encoded_internal_key_len_u32).unwrap();
        buffer.extend(encoded_internal_key.0);
        buffer.push(0);

        self.iter.seek(&buffer);
    }

    pub fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        // TODO: improve the skiplist interface and make this unnecessary.
        let mut buffer = Vec::new();
        let encoded_internal_key = strict_upper_bound.encoded_internal_key();
        let encoded_internal_key_len_u32 = u32::try_from(encoded_internal_key.0.len()).unwrap();
        buffer.write_varint(encoded_internal_key_len_u32).unwrap();
        buffer.extend(encoded_internal_key.0);
        buffer.push(0);

        self.iter.seek_before(&buffer);
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
    pub(super) fn new(list: Memtable<Cmp, Skiplist>) -> Self {
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

    fn next(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.next().map(|encoded_memtable_entry| {
            // Since the sole place we insert anything into the inner list is `encoder.encode_to`
            // inside `Self::insert_write_entry`, we know that every entry in the inner list
            // is a valid `EncodedMemtableEntry` (barring a bug).
            let memtable_entry = EncodedMemtableEntry::new_unchecked(encoded_memtable_entry);

            let (key, value) = memtable_entry.key_and_value();
            EncodedInternalEntry::new(key, value.data())
        })
    }

    #[inline]
    fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.current().map(|encoded_memtable_entry| {
            // See above
            let memtable_entry = EncodedMemtableEntry::new_unchecked(encoded_memtable_entry);

            let (key, value) = memtable_entry.key_and_value();
            EncodedInternalEntry::new(key, value.data())
        })
    }

    fn prev(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.prev().map(|encoded_memtable_entry| {
            // See above
            let memtable_entry = EncodedMemtableEntry::new_unchecked(encoded_memtable_entry);

            let (key, value) = memtable_entry.key_and_value();
            EncodedInternalEntry::new(key, value.data())
        })
    }

    fn reset(&mut self) {
        self.iter.reset();
    }

    fn seek(&mut self, min_bound: LookupKey<'_>) {
        // TODO: improve the skiplist interface and make this unnecessary.
        let mut buffer = Vec::new();
        let encoded_internal_key = min_bound.encoded_internal_key();
        let encoded_internal_key_len_u32 = u32::try_from(encoded_internal_key.0.len()).unwrap();
        buffer.write_varint(encoded_internal_key_len_u32).unwrap();
        buffer.extend(encoded_internal_key.0);
        buffer.push(0);

        self.iter.seek(&buffer);
    }

    fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        // TODO: improve the skiplist interface and make this unnecessary.
        let mut buffer = Vec::new();
        let encoded_internal_key = strict_upper_bound.encoded_internal_key();
        let encoded_internal_key_len_u32 = u32::try_from(encoded_internal_key.0.len()).unwrap();
        buffer.write_varint(encoded_internal_key_len_u32).unwrap();
        buffer.extend(encoded_internal_key.0);
        buffer.push(0);

        self.iter.seek_before(&buffer);
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }
}

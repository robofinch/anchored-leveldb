use crate::all_errors::types::OutOfSequenceNumbers;
use crate::{
    pub_typed_bytes::{EntryType, ReadPrefixedBytes as _, SequenceNumber, ShortSlice},
    typed_bytes::{InternalEntry, InternalKey, InternalKeyTag, MaybeUserValue, UserKey},
};
use super::batches::{BorrowedWriteBatch, ChainedWriteBatches};


#[derive(Debug, Clone, Copy)]
pub enum WriteEntry<'a> {
    Value {
        key:   &'a [u8],
        value: &'a [u8],
    },
    Deletion {
        key:   &'a [u8],
    },
}

#[derive(Debug, Clone)]
pub struct WriteBatchIter<'a> {
    /// The remaining entries to iterate over.
    entries: &'a [u8],
}

impl<'a> WriteBatchIter<'a> {
    #[inline]
    #[must_use]
    pub const fn new(batch: BorrowedWriteBatch<'a>) -> Self {
        Self {
            entries: batch.entries(),
        }
    }

    #[inline]
    #[must_use]
    pub const fn remaining_entries(&self) -> &'a [u8] {
        self.entries
    }
}

impl<'a> Iterator for WriteBatchIter<'a> {
    type Item = WriteEntry<'a>;

    #[expect(
        clippy::expect_used,
        reason = "BorrowedWriteBatch values are guaranteed to have valid entries",
    )]
    fn next(&mut self) -> Option<Self::Item> {
        let (&[entry_type], remaining) = self.entries.split_first_chunk::<1>()?;
        self.entries = remaining;

        let entry_type = EntryType::try_from(entry_type)
            .expect("bug: write batch entry type not properly validated");

        let key = self.entries.read_prefixed_bytes()
            .expect("bug: write batch key bytes not properly validated")
            .unprefixed_inner()
            .inner();

        match entry_type {
            EntryType::Deletion => {
                Some(WriteEntry::Deletion { key })
            }
            EntryType::Value => {
                let value = self.entries.read_prefixed_bytes()
                    .expect("bug: write batch value bytes not properly validated")
                    .unprefixed_inner()
                    .inner();

                Some(WriteEntry::Value { key, value })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChainedWriteBatchIter<'a> {
    prev_sequence: SequenceNumber,
    current_batch: &'a [u8],
    batches:       &'a [&'a [u8]],
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> ChainedWriteBatchIter<'a> {
    /// If there are enough sequence numbers for every entry in these write batches
    /// (starting from `prev_sequence.checked_add(1).unwrap()` for the first entry and
    /// `prev_sequence.checked_add_u32(self.num_entries()).unwrap()` for the last), returns
    /// an iterator over the write batches and the last sequence number which will be assigned
    /// to an entry.
    ///
    /// # Errors
    /// Returns an error if `prev_sequence.checked_add_u32(self.num_entries()).is_err()`.
    #[inline]
    pub fn new(
        prev_sequence: SequenceNumber,
        batches:       &'a ChainedWriteBatches<'a>,
    ) -> Result<(Self, SequenceNumber), OutOfSequenceNumbers> {
        Ok((
            Self {
                prev_sequence,
                current_batch: &[],
                batches:       batches.batches(),
            },
            prev_sequence.checked_add_u32(batches.num_entries())?,
        ))
    }
}

impl<'a> Iterator for ChainedWriteBatchIter<'a> {
    type Item = InternalEntry<'a>;

    #[expect(
        clippy::expect_used,
        reason = "BorrowedWriteBatch values are guaranteed to have valid entries",
    )]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_batch.is_empty() {
            let (&[next_batch], remaining) = self.batches.split_first_chunk::<1>()?;
            self.current_batch = next_batch;
            self.batches = remaining;
        }

        let (&[entry_type], remaining) = self.current_batch.split_first_chunk::<1>()?;
        self.current_batch = remaining;

        let entry_type = EntryType::try_from(entry_type)
            .expect("bug: write batch entry type not properly validated");

        let key = self.current_batch.read_prefixed_bytes()
            .expect("bug: write batch key bytes not properly validated")
            .unprefixed_inner()
            .inner();
        let key = UserKey::new(key)
            .expect("bug: write batch key length not properly validated");

        let sequence_number = self.prev_sequence
            .checked_add(1)
            .expect("bug: write batch num_entries not properly validated");
        let key_tag = InternalKeyTag::new(sequence_number, entry_type);
        self.prev_sequence = sequence_number;

        let value = match entry_type {
            EntryType::Deletion => {
                MaybeUserValue(ShortSlice::EMPTY)
            }
            EntryType::Value => {
                let value = self.current_batch.read_prefixed_bytes()
                    .expect("bug: write batch value bytes not properly validated")
                    .unprefixed_inner();
                MaybeUserValue(value)
            }
        };

        Some(InternalEntry(InternalKey(key, key_tag), value))
    }
}

use std::cmp::Ordering;

use crate::utils::WriteVarint as _;
use crate::{
    all_errors::types::{
        OutOfSequenceNumbers, PushBatchError, WriteBatchDeleteError, WriteBatchPutError,
        WriteBatchValidationError,
    },
    pub_typed_bytes::{EntryType, ReadPrefixedBytes as _, SequenceNumber},
    typed_bytes::{UserKey, UserValue},
};
use super::iter::{ChainedWriteBatchIter, WriteBatchIter, WriteEntry};


/// # Note on format
///
/// The format is mostly compatible with the `WriteBatch` format persisted to disk and used in
/// Google's original LevelDB implementation; the sole difference is that `entries` lacks a
/// 12-byte header (containing a sequence number followed by `num_entries`) present in the
/// persistent format.
///
/// # Format
///
/// Each encoded entry is a byte slice beginning with:
/// - `type_tag`, the one-byte [`EntryType`] value corresponding to the entry.
/// - `key_len`, a varint32,
/// - `key`, a byte slice of length `key_len`,
///
/// Additionally, in the [`EntryType::Value`] case, following the `key` slice are:
/// - `value_len`, a varint32,
/// - `value`, a byte slice of length `value_len`.
///
/// No data follows the `key` slice in the [`EntryType::Deletion`] case.
#[derive(Debug, Clone)]
pub struct WriteBatch {
    num_entries: u32,
    entries:     Vec<u8>,
}

impl WriteBatch {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            num_entries: 0,
            entries:     Vec::new(),
        }
    }

    /// The `buffer` is cleared and used solely for its capacity.
    #[inline]
    #[must_use]
    pub fn new_with_buffer(mut buffer: Vec<u8>) -> Self {
        buffer.clear();
        Self {
            num_entries: 0,
            entries:     buffer,
        }
    }

    /// Reset the write batch to its initial empty state, keeping only buffer capacity.
    pub fn clear(&mut self) {
        self.num_entries = 0;
        self.entries.clear();
    }

    #[inline]
    pub fn validate(
        num_entries: u32,
        entries:     Vec<u8>,
    ) -> Result<Self, WriteBatchValidationError> {
        BorrowedWriteBatch::validate(num_entries, &entries)?;

        Ok(Self { num_entries, entries })
    }

    /// # Errors
    /// Returns an error in any of the following circumstances:
    /// - There were already `u32::MAX` entries in this write batch.
    /// - `key.len()` exceeds `u32::MAX - 8`.
    /// - `value.len()` exceeds `u32::MAX`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), WriteBatchPutError> {
        let incremented = self.num_entries
            .checked_add(1)
            .ok_or(WriteBatchPutError::MaxEntries)?;

        let key = UserKey::new(key).ok_or(WriteBatchPutError::KeyTooLong)?;
        let value = UserValue::new(value).ok_or(WriteBatchPutError::ValueTooLong)?;

        let total_len_lower_bound = 3_usize
            .saturating_add(usize::from(key.len()))
            .saturating_add(usize::from(value.len()));
        self.entries.reserve(total_len_lower_bound);

        self.num_entries = incremented;
        self.entries.push(u8::from(EntryType::Value));
        self.entries.write_varint32(u32::from(key.len()));
        self.entries.extend(key.inner());
        self.entries.write_varint32(u32::from(value.len()));
        self.entries.extend(value.inner());

        Ok(())
    }

    /// # Errors
    /// Returns an error in any of the following circumstances:
    /// - There were already `u32::MAX` entries in this write batch.
    /// - `key.len()` exceeds `u32::MAX - 8`.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), WriteBatchDeleteError> {
        let incremented = self.num_entries
            .checked_add(1)
            .ok_or(WriteBatchDeleteError::MaxEntries)?;

        let key = UserKey::new(key).ok_or(WriteBatchDeleteError::KeyTooLong)?;

        self.num_entries = incremented;
        self.entries.push(u8::from(EntryType::Deletion));
        self.entries.write_varint32(u32::from(key.len()));
        self.entries.extend(key.inner());

        Ok(())
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> WriteBatchIter<'_> {
        WriteBatchIter::new(self.borrow())
    }

    #[inline]
    #[must_use]
    pub const fn num_entries(&self) -> u32 {
        self.num_entries
    }

    #[inline]
    #[must_use]
    pub const fn entries(&self) -> &Vec<u8> {
        &self.entries
    }

    #[inline]
    #[must_use]
    pub fn into_buffer(self) -> Vec<u8> {
        self.entries
    }

    #[inline]
    #[must_use]
    pub fn borrow(&self) -> BorrowedWriteBatch<'_> {
        BorrowedWriteBatch {
            num_entries: self.num_entries,
            entries:     &self.entries,
        }
    }

    /// Append the entry data of a different write batch onto this write batch.
    ///
    /// When possible, copying the data should be avoided in favor of using [`ChainedWriteBatches`].
    ///
    /// # Errors
    /// Returns an error if the total number of entries in the two write batches exceeds
    /// [`u32::MAX`].
    pub fn push_batch(&mut self, other: BorrowedWriteBatch<'_>) -> Result<(), PushBatchError> {
        self.num_entries = self.num_entries
            .checked_add(other.num_entries)
            .ok_or(PushBatchError)?;
        self.entries.extend(other.entries);
        Ok(())
    }
}

impl Default for WriteBatch {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> IntoIterator for &'a WriteBatch {
    type IntoIter = WriteBatchIter<'a>;
    type Item     = WriteEntry<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BorrowedWriteBatch<'a> {
    num_entries: u32,
    entries:     &'a [u8],
}

impl<'a> BorrowedWriteBatch<'a> {
    pub fn validate(
        num_entries: u32,
        entries:     &'a [u8],
    ) -> Result<Self, WriteBatchValidationError> {
        let mut entries_read: u32 = 0;
        let mut cursor = entries;

        // Loop through reading every `entry_type` `key_len` `key` [`value_len` `value`] entry.
        while let Some((&[entry_type], remaining)) = cursor.split_first_chunk::<1>() {
            cursor = remaining;

            // Possible error: invalid entry type
            let entry_type = EntryType::try_from(entry_type)
                .map_err(|()| WriteBatchValidationError::UnknownEntryType(entry_type))?;

            // Parse `key_len` and `key`.
            // Possible errors: either `key_len` is invalid, or there weren't at least `key_len`
            // additional bytes to form `key` from.
            let key = cursor.read_prefixed_bytes()
                .map_err(WriteBatchValidationError::from_prefixed_bytes_err)?;

            UserKey::new(key.unprefixed_inner())
                .ok_or(WriteBatchValidationError::KeyTooLong)?;

            match entry_type {
                EntryType::Deletion => {
                    // Nothing else to read for this entry.
                },
                EntryType::Value => {
                    // Parse `value_len` and `value`.
                    // Possible errors: either `value_len` is invalid, or there weren't at
                    // least `value_len` additional bytes to form `value` from.
                    cursor.read_prefixed_bytes()
                        .map_err(WriteBatchValidationError::from_prefixed_bytes_err)?;

                    // Note that `PrefixedBytes` is prefixed by a varint32, so it's impossible
                    // for the value to be too long.
                }
            }

            // Possible error: `num_entries` did not equal the actual number of entries in the
            // write batch. (Since `num_entries <= u32::MAX`, overflow implies there's too many.)
            entries_read = entries_read.checked_add(1)
                .ok_or(WriteBatchValidationError::TooManyEntries)?;
        }

        match entries_read.cmp(&num_entries) {
            Ordering::Less => Err(WriteBatchValidationError::TooFewEntries),
            Ordering::Equal => Ok(Self {
                num_entries,
                entries,
            }),
            Ordering::Greater => Err(WriteBatchValidationError::TooManyEntries),
        }
    }

    #[inline]
    #[must_use]
    pub fn to_owned(self) -> WriteBatch {
        WriteBatch {
            num_entries: self.num_entries,
            entries:     self.entries.to_owned(),
        }
    }

    #[inline]
    #[must_use]
    pub const fn iter(self) -> WriteBatchIter<'a> {
        WriteBatchIter::new(self)
    }

    #[inline]
    #[must_use]
    pub const fn num_entries(&self) -> u32 {
        self.num_entries
    }

    #[inline]
    #[must_use]
    pub const fn entries(&self) -> &'a [u8] {
        self.entries
    }
}

impl<'a> IntoIterator for BorrowedWriteBatch<'a> {
    type IntoIter = WriteBatchIter<'a>;
    type Item     = WriteEntry<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug, Clone)]
pub struct ChainedWriteBatches<'a> {
    num_entries: u32,
    batches:     Vec<&'a [u8]>,
}

impl<'a> ChainedWriteBatches<'a> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            num_entries: 0,
            batches:     Vec::new(),
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.batches.clear();
    }

    #[inline]
    pub fn push_batch(&mut self, batch: BorrowedWriteBatch<'a>) -> Result<(), PushBatchError> {
        self.num_entries = self.num_entries
            .checked_add(batch.num_entries)
            .ok_or(PushBatchError)?;

        self.batches.push(batch.entries());
        Ok(())
    }

    #[inline]
    #[must_use]
    pub const fn num_entries(&self) -> u32 {
        self.num_entries
    }

    #[inline]
    #[must_use]
    pub fn batches(&self) -> &[&'a [u8]] {
        &self.batches
    }

    /// If there are enough sequence numbers for every entry in these write batches
    /// (starting from `prev_sequence.checked_add(1).unwrap()` for the first entry and
    /// `prev_sequence.checked_add_u32(self.num_entries()).unwrap()` for the last), returns
    /// an iterator over the write batches and the last sequence number which will be assigned
    /// to an entry.
    ///
    /// # Errors
    /// Returns an error if `prev_sequence.checked_add_u32(self.num_entries()).is_err()`.
    #[inline]
    pub(crate) fn try_get_iter(
        &'a self,
        prev_sequence: SequenceNumber,
    ) -> Result<(ChainedWriteBatchIter<'a>, SequenceNumber), OutOfSequenceNumbers> {
        ChainedWriteBatchIter::new(prev_sequence, self)
    }
}

impl Default for ChainedWriteBatches<'_> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

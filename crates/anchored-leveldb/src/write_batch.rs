use integer_encoding::VarIntWriter as _;

use crate::format::{EntryType, LengthPrefixedBytes, SequenceNumber, WriteEntry};


#[derive(Debug, Clone)]
pub struct WriteBatch {
    num_entries:        u32,
    /// See [`UnvalidatedWriteBatch::headerless_entries`] for what the format is.
    ///
    /// Unlike in the case of `UnvalidatedWriteBatch`, this field is guaranteed to have the
    /// described format.
    headerless_entries: Vec<u8>,
}

impl WriteBatch {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_buffer(Vec::new())
    }

    #[inline]
    #[must_use]
    pub fn new_with_buffer(buffer: Vec<u8>) -> Self {
        Self {
            num_entries:        0,
            headerless_entries: buffer,
        }
    }

    /// # Errors
    /// Returns an error if `key.len()` or `value.len()` exceed `u32::MAX` or if there were
    /// already `u32::MAX` entries in the write batch.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        Self::validated_put(&mut self.num_entries, &mut self.headerless_entries, key, value)
    }

    /// # Errors
    /// Returns an error if `key.len()` exceeds `u32::MAX` or if there were already `u32::MAX`
    /// entries in the write batch.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), ()> {
        Self::validated_delete(&mut self.num_entries, &mut self.headerless_entries, key)
    }

    /// Appends an entire write batch to the end of this write batch.
    pub fn push_batch(&mut self, other: &Self) {
        self.num_entries += other.num_entries;
        self.headerless_entries.extend(&other.headerless_entries);
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> WriteBatchIter<'_> {
        WriteBatchIter::new(self)
    }

    /// Reset the write batch to its initial empty state, keeping only buffer capacity.
    pub fn clear(&mut self) {
        self.num_entries = 0;
        self.headerless_entries.clear();
    }
}

impl WriteBatch {
    #[inline]
    #[must_use]
    pub fn num_entries(&self) -> u32 {
        self.num_entries
    }

    #[inline]
    #[must_use]
    pub fn headerless_entries(&self) -> &Vec<u8> {
        &self.headerless_entries
    }
}

impl WriteBatch {
    /// Helper function for both `WriteBatch` and `UnvalidatedWriteBatch`.
    ///
    /// # Errors
    /// Returns an error if `key.len()` or `value.len()` exceed `u32::MAX` or if there were
    /// already `u32::MAX` entries.
    fn validated_put(
        num_entries:        &mut u32,
        headerless_entries: &mut Vec<u8>,
        key:                &[u8],
        value:              &[u8],
    ) -> Result<(), ()> {
        let incremented = num_entries.checked_add(1).ok_or(())?;

        let key_len = u32::try_from(key.len()).map_err(|_| ())?;
        let value_len = u32::try_from(value.len()).map_err(|_| ())?;

        headerless_entries.write_varint(key_len);
        headerless_entries.extend(key);
        headerless_entries.push(u8::from(EntryType::Value));
        headerless_entries.write_varint(value_len);
        headerless_entries.extend(value);
        *num_entries = incremented;

        Ok(())
    }

    /// Helper function for both `WriteBatch` and `UnvalidatedWriteBatch`.
    ///
    /// # Errors
    /// Returns an error if `key.len()` exceeds `u32::MAX` or if there were already `u32::MAX`
    /// entries.
    fn validated_delete(
        num_entries:        &mut u32,
        headerless_entries: &mut Vec<u8>,
        key:                &[u8],
    ) -> Result<(), ()> {
        let incremented = num_entries.checked_add(1).ok_or(())?;

        let key_len = u32::try_from(key.len()).map_err(|_| ())?;

        headerless_entries.write_varint(key_len);
        headerless_entries.extend(key);
        headerless_entries.push(u8::from(EntryType::Deletion));
        *num_entries = incremented;

        Ok(())
    }

    #[must_use]
    fn persistent_encoding<'a>(
        &'a self,
        seq_num: SequenceNumber,
        buffer:  &'a mut [u8; 12],
    ) -> [&'a [u8]; 2] {
        buffer[..8].copy_from_slice(&seq_num.0.to_le_bytes());
        buffer[8..].copy_from_slice(&self.num_entries.to_le_bytes());
        [buffer, &self.headerless_entries]
    }
}

impl<'a> IntoIterator for &'a WriteBatch {
    type IntoIter = WriteBatchIter<'a>;
    type Item     = WriteEntry<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        WriteBatchIter::new(self)
    }
}

#[derive(Debug, Clone)]
pub struct UnvalidatedWriteBatch {
    pub num_entries:        u32,
    /// This field should consist of a flattened list  of length `self.num_entries` of encoded
    /// [`WriteEntry`] values.
    ///
    /// # Note on format
    ///
    /// Note that the field only _should_ contain data with the described format; the data must be
    /// validated. Additionally, note that the format is mostly compatible with the `WriteBatch`
    /// format persisted to disk and used in the original LevelDB implementation; the sole
    /// difference is that `headerless_entries` lacks a 12-byte header (containing a sequence
    /// number and `self.num_entries`) present in the persistent format.
    ///
    /// # Format
    ///
    /// Each encoded `WriteEntry` begins with:
    /// - `key_len`, which is a varint32,
    /// - `key`, which is `key_len`-many bytes,
    /// - the one-byte [`EntryType`] value corresponding to the `WriteEntry`.
    ///
    /// Additionally, in the [`WriteEntry::Deletion`] case, following the [`EntryType::Deletion`]
    /// byte are:
    /// - `value_len`, a varint32
    /// - `value`, which is `value_len`-many bytes.
    ///
    /// No data follows the [`EntryType::Value`] byte in the [`WriteEntry::Value`] case.
    pub headerless_entries: Vec<u8>,
}

impl UnvalidatedWriteBatch {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_buffer(Vec::new())
    }

    #[inline]
    #[must_use]
    pub fn new_with_buffer(buffer: Vec<u8>) -> Self {
        Self {
            num_entries:        0,
            headerless_entries: buffer,
        }
    }

    #[inline]
    #[must_use]
    pub fn from_validated(write_batch: WriteBatch) -> Self {
        Self {
            num_entries:        write_batch.num_entries,
            headerless_entries: write_batch.headerless_entries,
        }
    }

    pub fn validate(&self) -> Result<(), ()> {
        let mut byte_index: usize = 0;
        let mut entry_index: u32 = 0;

        // Loop through reading every `key_len` `key` `entry_type` [`value_len` `value`] entry.
        while byte_index < self.headerless_entries.len() {
            let current_entry = &self.headerless_entries[byte_index..];

            // Parse `key_len` and `key`.
            // The possible error: either `key_len` is invalid, or there weren't at least `key_len`
            // additional bytes to form `key` from.
            let length_prefixed_key = LengthPrefixedBytes::parse(current_entry)?;

            let after_key = &current_entry[byte_index + length_prefixed_key.len()..];

            // The possible error: missing entry type
            let &entry_type = after_key.first().ok_or(())?;

            // The possible error: invalid entry type
            let entry_type = EntryType::try_from(entry_type)?;

            match entry_type {
                EntryType::Deletion => {
                    // Nothing else to read for this entry.

                    // The possible error: `self.num_entries` did not equal the actual number
                    // of entries in the write batch
                    // (since `self.num_entries <= u32::MAX`, overflow necessitates this.)
                    entry_index = entry_index.checked_add(1).ok_or(())?;

                    // We read the `length_prefixed_key` data plus one byte for `entry_type`,
                    // and since we never read more than `self.headerless_entries.len()` bytes,
                    // this cannot overflow
                    byte_index += length_prefixed_key.len() + 1;
                },
                EntryType::Value => {
                    // First, read the value
                    let after_entry_type = &after_key[1..];

                    // Parse `value_len` and `value`.
                    // The possible error: either `value_len` is invalid, or there weren't at
                    // least `value_len` additional bytes to form `value` from.
                    let length_prefixed_value = LengthPrefixedBytes::parse(after_entry_type)?;

                    // The possible error: `self.num_entries` did not equal the actual number
                    // of entries in the write batch
                    // (since `self.num_entries <= u32::MAX`, overflow necessitates this.)
                    entry_index = entry_index.checked_add(1).ok_or(())?;

                    // We read the `length_prefixed_key` data, one byte for `entry_type`,
                    // and the `length_prefixed_value` data.
                    // We never read more than `self.headerless_entries.len()` bytes,
                    // so this cannot overflow.
                    byte_index += length_prefixed_key.len() + 1 + length_prefixed_value.len();
                }
            }
        }

        if entry_index == self.num_entries {
            Ok(())
        } else {
            // `self.num_entries` did not equal the actual number of entries in the write batch
            Err(())
        }
    }

    pub fn into_validated(self) -> Result<WriteBatch, (Self, ())> {
        match self.validate() {
            Ok(()) => Ok(WriteBatch {
                num_entries:        self.num_entries,
                headerless_entries: self.headerless_entries,
            }),
            Err(err) => Err((self, err)),
        }
    }

    /// # Errors
    /// Returns an error if `key.len()` or `value.len()` exceed `u32::MAX` or if there were
    /// already `u32::MAX` entries in the write batch.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        WriteBatch::validated_put(&mut self.num_entries, &mut self.headerless_entries, key, value)
    }

    /// # Errors
    /// Returns an error if `key.len()` exceeds `u32::MAX` or if there were already `u32::MAX`
    /// entries in the write batch.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), ()> {
        WriteBatch::validated_delete(&mut self.num_entries, &mut self.headerless_entries, key)
    }

    /// Appends an entire write batch to the end of this write batch.
    pub fn push_batch(&mut self, other: &Self) {
        self.num_entries += other.num_entries;
        self.headerless_entries.extend(&other.headerless_entries);
    }

    /// Appends an entire write batch to the end of this write batch.
    pub fn push_validated_batch(&mut self, other: &WriteBatch) {
        self.num_entries += other.num_entries;
        self.headerless_entries.extend(&other.headerless_entries);
    }

    #[inline]
    #[must_use]
    pub fn iter(&self) -> Result<WriteBatchIter<'_>, ()> {
        WriteBatchIter::from_unvalidated(self)
    }

    /// Reset the write batch to its initial empty state, keeping only buffer capacity.
    pub fn clear(&mut self) {
        self.num_entries = 0;
        self.headerless_entries.clear();
    }
}

impl From<WriteBatch> for UnvalidatedWriteBatch {
    #[inline]
    fn from(write_batch: WriteBatch) -> Self {
        Self::from_validated(write_batch)
    }
}

impl TryFrom<UnvalidatedWriteBatch> for WriteBatch {
    type Error = (UnvalidatedWriteBatch, ());

    fn try_from(write_batch: UnvalidatedWriteBatch) -> Result<Self, Self::Error> {
        write_batch.into_validated()
    }
}

#[derive(Debug, Clone)]
pub struct WriteBatchIter<'a> {
    validated_entries: &'a [u8],
    byte_index:        usize,
}

impl<'a> WriteBatchIter<'a> {
    #[inline]
    #[must_use]
    pub fn new(write_batch: &'a WriteBatch) -> Self {
        Self {
            validated_entries: &write_batch.headerless_entries,
            byte_index:        0,
        }
    }

    #[must_use]
    pub fn from_unvalidated(write_batch: &'a UnvalidatedWriteBatch) -> Result<Self, ()> {
        write_batch.validate()?;

        Ok(Self {
            validated_entries: &write_batch.headerless_entries,
            byte_index:        0,
        })
    }
}

impl<'a> Iterator for WriteBatchIter<'a> {
    type Item = WriteEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.byte_index >= self.validated_entries.len() {
            return None;
        }

        let current_entry = &self.validated_entries[self.byte_index..];

        // Get the key
        let key = LengthPrefixedBytes::parse(current_entry).unwrap();
        let after_key = &current_entry[key.len()..];

        // Get the entry type
        let &entry_type = after_key.first().unwrap();
        let entry_type = EntryType::try_from(entry_type).unwrap();

        Some(match entry_type {
            EntryType::Deletion => {
                WriteEntry::Deletion { key }
            }
            EntryType::Value => {
                // Get the value
                let after_entry_type = &after_key[1..];
                let value = LengthPrefixedBytes::parse(after_entry_type).unwrap();

                WriteEntry::Value { key, value }
            }
        })
    }
}

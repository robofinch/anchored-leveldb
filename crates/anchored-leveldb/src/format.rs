use std::{
    io::{Result as IoResult, Write as _},
    path::{Path, PathBuf},
};

use bijective_enum_map::injective_enum_map;
use integer_encoding::{VarInt as _, VarIntWriter as _};

use crate::public_format::{EntryType, LengthPrefixedBytes, WriteEntry};


// ================================================================
//  Config constants
// ================================================================

/// The maximum number of levels in the LevelDB database.
pub(crate) const NUM_LEVELS: u8 = 7;
/// The maximum number of levels in the LevelDB database, as a usize.
#[expect(clippy::as_conversions, reason = "`From` conversions do not yet work in const")]
pub(crate) const NUM_LEVELS_USIZE: usize = NUM_LEVELS as usize;
/// The maximum level which a level-0 file may be compacted to
pub(crate) const MAX_LEVEL_FOR_COMPACTION: u8 = 2;

/// Once there are [`L0_COMPACTION_TRIGGER`]-many level-0 files, size compactions may target
/// level 0.
pub(crate) const L0_COMPACTION_TRIGGER: u8 = 4;
/// Once there are [`L0_SOFT_FILE_LIMIT`]-many level-0 files, writes are slowed down
/// in order to let compactions catch up.
pub(crate) const L0_SOFT_FILE_LIMIT: u8 = 8;
/// Once there are [`L0_HARD_FILE_LIMIT`]-many level-0 files, writes are entirely stopped
/// in order to let compactions catch up.
pub(crate) const L0_HARD_FILE_LIMIT: u8 = 12;

// Note that the maximum size per file is configurable, but the maximum size per level is not.

/// Once level-1 files have a total file size exceeding [`MAX_BYTES_FOR_L1`], size compactions
/// may target level 1.
#[expect(clippy::as_conversions, reason = "`From` conversions do not yet work in const")]
pub(crate) const MAX_BYTES_FOR_L1: f64 = (1_u32 << 20_u8) as f64 * MAX_BYTES_MULTIPLIER;
/// Once level-(`n+1`) files have a total file size exceeding [`MAX_BYTES_MULTIPLIER`] times
/// the max bytes limit of level `n`, size compactions may target level `n+1`.
pub(crate) const MAX_BYTES_MULTIPLIER: f64 = 10.0;

/// For a given `max_file_size` setting, a file being built in a compaction from level `n` to
/// level `n+1` will stop being built if the file's overlapping grandparents in level `n+2`
/// reach a total size of <code>[GRANDPARENT_OVERLAP_SIZE_FACTOR] * max_file_size</code> bytes.
pub(crate) const GRANDPARENT_OVERLAP_SIZE_FACTOR: u64 = 10;
/// For a given `max_file_size` setting, a compaction from level `n` to level `n+1` will not be
/// expanded if the total file size of input files for the compaction, across both levels,
/// would exceed <code>[EXPANDED_COMPACTION_SIZE_FACTOR] * max_file_size</code> bytes
/// after expansion.
pub(crate) const EXPANDED_COMPACTION_SIZE_FACTOR: u64 = 25;

// TODO: make this configurable
// pub const READ_SAMPLE_PERIOD: u32 = 1 << 20;

/// The maximum value for the `max_file_size` setting. This number was chosen to ensure that
/// <code>[GRANDPARENT_OVERLAP_SIZE_FACTOR] * max_file_size</code> and
/// <code>[EXPANDED_COMPACTION_SIZE_FACTOR] * max_file_size</code> do not overflow.
pub(crate) const MAXIMUM_MAX_FILE_SIZE_OPTION: u64 = 1 << 59;

/// The block size for the log format used by `MANIFEST-_` files and write-ahead logs
/// (`_.log` files).
pub(crate) const WRITE_LOG_BLOCK_SIZE: usize = 1 << 15;

// ================================================================
//  Key and entry formats
// ================================================================

// Also see `crate::write_batch::WriteBatch`, which handles a persistent format.

/// A reference to a mostly-arbitrary byte slice of user key data.
///
/// When reading a `UserKey` from persistent storage, it should be assumed to be completely
/// arbitrary. When taking a new `UserKey` from the user, the length should be validated to be
/// at most `u32::MAX`.. minus 8.
///
// TODO: update the max key length to be `u32::MAX - 8` everywhere the length needs to be
// validated... after, of course, I finish working on anything that might further constrain
// that bound.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct UserKey<'a>(pub &'a [u8]);

/// A reference to a mostly-arbitrary byte slice of user key data.
///
/// When reading a `UserValue` from persistent storage, it should be assumed to be completely
/// arbitrary. When taking a new `UserKey` from the user, the length should be validated to be
/// at most `u32::MAX`.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct UserValue<'a>(pub &'a [u8]);

/// A possibly-valid encoding of an [`InternalKey`].
///
/// The referenced byte slice _should_ consist of user key data followed by 8 bytes.
/// Those 8 bytes should be a little-endian encoding of a 64 bit unsigned integer, where
/// the most significant 56 bits indicate the [`SequenceNumber`] and the least significant 8 bits
/// indicate the [`EntryType`].
///
/// This value must be validated; generally, methods taking an `EncodedInternalKey` should
/// return a result.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct EncodedInternalKey<'a>(pub &'a [u8]);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> EncodedInternalKey<'a> {
    pub fn user_key(self) -> Result<UserKey<'a>, ()> {
        let user_key_len = self.0.len()
            .checked_sub(8)
            .ok_or(())?;

        #[expect(clippy::indexing_slicing, reason = "`user_key_len < self.0.len()`")]
        Ok(UserKey(&self.0[..user_key_len]))
    }

    fn split(self) -> Result<(UserKey<'a>, u64), ()> {
        let user_key_len = self.0.len()
            .checked_sub(8)
            .ok_or(())?;

        let (user_key, last_eight_bytes) = self.0.split_at(user_key_len);
        #[expect(clippy::unwrap_used, reason = "`.try_into()` succeeds for a slice of length 8")]
        let last_eight_bytes: [u8; 8] = last_eight_bytes.try_into().unwrap();

        Ok((
            UserKey(user_key),
            u64::from_le_bytes(last_eight_bytes),
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalKey<'a> {
    pub user_key:        UserKey<'a>,
    pub sequence_number: SequenceNumber,
    pub entry_type:      EntryType,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> InternalKey<'a> {
    pub fn decode(key: EncodedInternalKey<'a>) -> Result<Self, ()> {
        let (user_key, tag) = key.split()?;

        let sequence_number = SequenceNumber(tag >> 8);
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "truncation is intentional",
        )]
        let entry_type = EntryType::try_from(tag as u8)?;

        Ok(Self {
            user_key,
            sequence_number,
            entry_type,
        })
    }

    #[inline]
    #[must_use]
    pub fn tag(&self) -> u64 {
        sequence_and_type_tag(self.sequence_number, self.entry_type)
    }

    #[inline]
    #[must_use]
    pub fn encoded_len(&self) -> u32 {
        u32::try_from(self.user_key.0.len() + 8).unwrap()
    }

    /// Extends the `output` buffer with the [`EncodedInternalKey`] slice corresponding to `self`.
    #[inline]
    pub fn append_encoded(&self, output: &mut Vec<u8>) {
        output.extend(self.user_key.0);
        output.extend(self.tag().to_le_bytes());
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InternalEntry<'a> {
    pub user_key:        UserKey<'a>,
    pub sequence_number: SequenceNumber,
    /// A `Some(_)` `value` corresponds to [`EntryType::Value`], and a `None` `value`
    /// corresponds to [`EntryType::Deletion`].
    pub value:           Option<UserValue<'a>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> InternalEntry<'a> {
    #[inline]
    #[must_use]
    pub const fn internal_key(self) -> InternalKey<'a> {
        let entry_type = if self.value.is_some() {
            EntryType::Value
        } else {
            EntryType::Deletion
        };
        InternalKey {
            user_key:        self.user_key,
            sequence_number: self.sequence_number,
            entry_type,
        }
    }
}

/// A valid [`EncodedMemtableEntry`] with an empty `value` slice, used to get values
/// from a memtable or version set.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct LookupKey<'a>(&'a [u8]);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> LookupKey<'a> {
    const INVALID_ENCODING: &'static str
        = "invalid LookupKey (this is a bug, not corruption)";

    /// `buffer` must be an empty buffer. The [`LookupKey`] slice is written to the buffer.
    ///
    /// # Correctness
    /// `sequence_number` must not exceed [`SequenceNumber::MAX_USABLE_SEQUENCE_NUMBER`].
    ///
    /// # Panics
    /// May panic if the user key has a length exceeding `u32::MAX - 8`.
    pub fn new(
        buffer:          &'a mut Vec<u8>,
        user_key:        UserKey<'a>,
        sequence_number: SequenceNumber,
    ) -> Self {
        let internal_key = InternalKey {
            user_key,
            sequence_number,
            entry_type:      EntryType::MAX_TYPE,
        };

        buffer.write_varint(internal_key.encoded_len()).expect("writing to a Vec does not fail");
        internal_key.append_encoded(buffer);
        buffer.write_varint(0_u32).expect("writing to a Vec does not fail");

        Self(buffer)
    }

    /// Returns an [`EncodedMemtableEntry`].
    #[inline]
    #[must_use]
    pub const fn encoded_memtable_entry(self) -> EncodedMemtableEntry<'a> {
        // `self.0` is:
        // - `internal_key_len`, a varint32,
        // - `internal_key`, containing:
        //   - `user_key`, a byte slice of user key data of length `internal_key_len - 8`,
        //   - `seq_and_type_tag`, 8 bytes encoding a `SequenceNumber` and `EntryType`,
        // - `value_len`, a varint32 encoding of 0,
        // - `value`, a byte slice of length 0.
        // Thus, this call preserves the invariant of `EncodedMemtableEntry`.
        EncodedMemtableEntry::new_unchecked(self.0)
    }

    #[must_use]
    pub fn memtable_entry(self) -> MemtableEntry<'a> {
        MemtableEntry::decode(self.encoded_memtable_entry())
    }

    /// Returns a valid [`EncodedInternalKey`].
    #[inline]
    #[must_use]
    pub fn encoded_internal_key(self) -> EncodedInternalKey<'a> {
        // We know that `self.0` starts with a valid varint, followed by
        // an encoded internal key, followed by a varint 0 (which is a single zero byte).
        let prefix_len = u32::decode_var(self.0).unwrap().1;
        // We know that `self.0.len() >= 1`, since we push a varint 0 to the end.
        EncodedInternalKey(&self.0[prefix_len..self.0.len() - 1])
    }

    #[must_use]
    pub fn internal_key(self) -> InternalKey<'a> {
        InternalKey::decode(self.encoded_internal_key()).expect(Self::INVALID_ENCODING)
    }

    /// Returns the [`UserKey`] which was used to make this lookup key.
    #[inline]
    #[must_use]
    pub fn user_key(self) -> UserKey<'a> {
        // We know that `self.0` starts with a valid varint, followed by
        // an encoded internal key, followed by a varint 0 (which is a single zero byte).
        let prefix_len = u32::decode_var(self.0).unwrap().1;
        // After the prefix is an encoded internal key followed by a zero byte.
        // The last eight bytes of the encoded internal key are the tag bytes, and the remaining
        // bytes are the user key.
        UserKey(&self.0[prefix_len..self.0.len() - 9])
    }
}

/// A valid encoding of a [`MemtableEntry`].
///
/// The referenced byte slice must be the concatenation of the following:
/// - the backing byte slice of a [`LengthPrefixedBytes`] value wrapping the backing byte slice of
///   a valid [`EncodedInternalKey`] value,
/// - the backing byte slice of a [`LengthPrefixedBytes`] value wrapping an arbitrary byte slice
///   of a user value.
///
/// Fully expanded, the referenced slice consists of the following:
/// - `internal_key_len`, a varint32,
/// - `user_key`, a byte slice of user key data of length `internal_key_len - 8`,
/// - `seq_and_type_tag`, 8 bytes encoding a [`SequenceNumber`] and [`EntryType`],
/// - `value_len`, a varint32,
/// - `value`, a byte slice of length `value_len`.
///
/// Note that if `seq_and_type_tag` indicates [`EntryType::Deletion`], then `value` is likely the
/// empty slice, but is not strictly guaranteed to be empty. In such an event, its value should not
/// semantically matter; its value may be preserved or discarded.
///
/// # Safety
/// Unsafe code may not rely on the invariant of this type, but functions may panic if the invariant
/// is violated.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct EncodedMemtableEntry<'a>(&'a [u8]);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> EncodedMemtableEntry<'a> {
    const INVALID_ENCODING: &'static str
        = "invalid EncodedMemtableEntry (this is a bug, not corruption)";

    /// Must only be used if `memtable_entry` is known to be a valid encoding of a
    /// [`MemtableEntry`], as described in the type-level documentation of [`EncodedMemtableEntry`].
    #[inline]
    #[must_use]
    pub const fn new_unchecked(memtable_entry: &'a [u8]) -> Self {
        Self(memtable_entry)
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> &'a [u8] {
        self.0
    }

    #[must_use]
    pub fn encoded_internal_key(self) -> EncodedInternalKey<'a> {
        #![expect(
            clippy::expect_used,
            clippy::missing_panics_doc,
            reason = "invariant of type: begins with a valid `LengthPrefixedBytes`",
        )]
        let (prefixed_internal_key, _) = LengthPrefixedBytes::parse(self.0)
            .expect(Self::INVALID_ENCODING);
        EncodedInternalKey(prefixed_internal_key.data())
    }

    #[must_use]
    pub fn internal_key(self) -> InternalKey<'a> {
        #![expect(
            clippy::expect_used,
            clippy::missing_panics_doc,
            reason = "invariant of type: the encoded internal key is valid",
        )]
        InternalKey::decode(self.encoded_internal_key()).expect(Self::INVALID_ENCODING)
    }

    #[must_use]
    pub fn user_key(self) -> UserKey<'a> {
        #![expect(
            clippy::expect_used,
            clippy::missing_panics_doc,
            reason = "invariant of type: the encoded internal key is valid",
        )]
        self.encoded_internal_key().user_key().expect(Self::INVALID_ENCODING)
    }

    /// Note that if the [`EncodedInternalKey`] has an [`EntryType::Deletion`] tag, the returned
    /// [`LengthPrefixedBytes`] is likely empty, but is not strictly guaranteed to be empty.
    pub fn key_and_value(self) -> (EncodedInternalKey<'a>, LengthPrefixedBytes<'a>) {
        /// This function is essentially used as a `try` block.
        fn try_scope(
            memtable_entry: EncodedMemtableEntry<'_>,
        ) -> Option<(EncodedInternalKey<'_>, LengthPrefixedBytes<'_>)> {
            let (
                prefixed_internal_key,
                after_key,
            ) = LengthPrefixedBytes::parse(memtable_entry.0).ok()?;
            let internal_key = EncodedInternalKey(prefixed_internal_key.data());

            let (prefixed_value, after_value) = LengthPrefixedBytes::parse(after_key).ok()?;

            if after_value.is_empty() {
                Some((internal_key, prefixed_value))
            } else {
                None
            }
        }

        try_scope(self).expect(Self::INVALID_ENCODING)
    }

    /// Note that if the [`EncodedInternalKey`] has an [`EntryType::Deletion`] tag,
    /// the returned [`UserValue`] is likely empty, but is not strictly guaranteed to be empty.
    #[must_use]
    pub fn key_and_user_value(self) -> (EncodedInternalKey<'a>, UserValue<'a>) {
        let (internal_key, prefixed_value) = self.key_and_value();
        (internal_key, UserValue(prefixed_value.data()))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MemtableEntryEncoder<'a> {
    internal_key_len: u32,
    user_key:         UserKey<'a>,
    tag:              u64,
    value:            Option<LengthPrefixedBytes<'a>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> MemtableEntryEncoder<'a> {
    /// Prepare to encode a valid [`EncodedMemtableEntry`] formed from the provided
    /// [`WriteEntry`] and [`SequenceNumber`].
    ///
    /// The length of the [`EncodedMemtableEntry`] slice is returned along with a
    /// [`MemtableEntryEncoder`]. The encoder should be provided with a mutable slice of exactly
    /// the correct length, and can write the [`EncodedMemtableEntry`] to such a slice.
    ///
    /// # Panics
    /// This function may panic if the user key in `write_entry` has a length exceeding
    /// `u32::MAX - 8`.
    #[must_use]
    pub fn start_encode(
        write_entry:     WriteEntry<'a>,
        sequence_number: SequenceNumber,
    ) -> (usize, Self) {
        let (key, value) = match write_entry {
            WriteEntry::Deletion { key }     => (key, None),
            WriteEntry::Value { key, value } => (key, Some(value)),
        };

        let user_key = key.data();
        let internal_key = InternalKey {
            user_key:        UserKey(user_key),
            sequence_number,
            entry_type:      write_entry.entry_type(),
        };

        let internal_key_len_u32 = internal_key.encoded_len();
        let internal_key_len = usize::try_from(internal_key_len_u32).unwrap();
        let prefixed_internal_key_len = internal_key_len
            + u32::required_space(internal_key_len_u32);

        let prefixed_value_len = if let Some(prefixed_value) = value {
            prefixed_value.prefixed_data().len()
        } else {
            u32::required_space(0)
        };

        let encoded_memtable_entry_len = prefixed_internal_key_len + prefixed_value_len;

        let encoder = MemtableEntryEncoder {
            internal_key_len: internal_key_len_u32,
            user_key:         internal_key.user_key,
            tag:              internal_key.tag(),
            value,
        };

        (encoded_memtable_entry_len, encoder)
    }

    /// Write a valid [`EncodedMemtableEntry`] to the `output` slice.
    ///
    /// # Panics
    /// This encoder was constructed from [`Self::start_encode`], alongside
    /// a usize length.
    ///
    /// This function may panic if `output` does not have exactly that length.
    #[inline]
    pub fn encode_to(&self, output: &mut [u8]) {
        self.try_encode_to(output).expect("`output` was not of the correct length")
    }

    /// This function is essentially used as a `try` block.
    ///
    /// # Error
    /// This encoder was constructed from [`Self::start_encode`], alongside
    /// a usize length.
    ///
    /// This function may return an error if `output` does not have exactly that length.
    #[inline]
    fn try_encode_to(&self, mut output: &mut [u8]) -> IoResult<()> {
        // Note that the function actually only errors if `output` is not long enough;
        // it could be too long. However, the "may" phrasing means that this behavior
        // is within the contract.
        output.write_varint(self.internal_key_len)?;
        output.write_all(self.user_key.0)?;
        output.write_all(&self.tag.to_le_bytes())?;
        if let Some(prefixed_value) = self.value {
            output.write_all(prefixed_value.prefixed_data())?;
        } else {
            output.write_varint(0_u32)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MemtableEntry<'a> {
    pub user_key:        UserKey<'a>,
    pub sequence_number: SequenceNumber,
    /// A `Some(_)` `value` corresponds to [`EntryType::Value`], and a `None` `value`
    /// corresponds to [`EntryType::Deletion`].
    pub value:           Option<LengthPrefixedBytes<'a>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> MemtableEntry<'a> {
    #[must_use]
    pub fn new(write_entry: WriteEntry<'a>, sequence_number: SequenceNumber) -> Self {
        let (key, value) = match write_entry {
            WriteEntry::Deletion { key }     => (key, None),
            WriteEntry::Value { key, value } => (key, Some(value)),
        };

        Self {
            user_key:        UserKey(key.data()),
            sequence_number,
            value,
        }
    }

    #[must_use]
    pub fn decode(memtable_entry: EncodedMemtableEntry<'a>) -> Self {
        let (internal_key, prefixed_value) = memtable_entry.key_and_value();
        let internal_key = InternalKey::decode(internal_key)
            .expect(EncodedMemtableEntry::INVALID_ENCODING);

        let value = match internal_key.entry_type {
            EntryType::Deletion => {
                // The "value" associated with an encoded deletion entry should probably be
                // the empty slice, but that isn't explicitly stated in a spec, so we leniently
                // accept any value.
                None
            }
            EntryType::Value => {
                // Any possible value is valid in this case
                Some(prefixed_value)
            }
        };

        Self {
            user_key:        internal_key.user_key,
            sequence_number: internal_key.sequence_number,
            value,
        }
    }

    #[inline]
    #[must_use]
    pub fn internal_key(&self) -> InternalKey<'a> {
        let entry_type = if self.value.is_some() {
            EntryType::Value
        } else {
            EntryType::Deletion
        };
        InternalKey {
            user_key:        self.user_key,
            sequence_number: self.sequence_number,
            entry_type,
        }
    }

    #[inline]
    #[must_use]
    pub fn internal_entry(&self) -> InternalEntry<'a> {
        InternalEntry {
            user_key:        self.user_key,
            sequence_number: self.sequence_number,
            value:           self.value.map(|prefixed_value| UserValue(prefixed_value.data())),
        }
    }
}

#[inline]
#[must_use]
pub(crate) fn sequence_and_type_tag(sequence_number: SequenceNumber, entry_type: EntryType) -> u64 {
    (sequence_number.0 << 8) | u64::from(u8::from(entry_type))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub(crate) struct SequenceNumber(u64);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl SequenceNumber {
    pub const ZERO: Self = Self(0);
    pub const MAX_USABLE_SEQUENCE_NUMBER: Self = Self((1 << 56) - 2);
    pub const MAX_SEQUENCE_NUMBER: Self = Self((1 << 56) - 1);

    /// Returns `SequenceNumber(sequence_number)` if the result would be a valid sequence number
    /// which could be used normally.
    #[inline]
    #[must_use]
    pub const fn new_usable(sequence_number: u64) -> Option<Self> {
        if sequence_number <= Self::MAX_USABLE_SEQUENCE_NUMBER.0 {
            Some(Self(sequence_number))
        } else {
            None
        }
    }

    /// `sequence_number` must be at most <code>[Self::MAX_SEQUENCE_NUMBER].inner()</code>
    /// to be valid, and at most <code>[Self::MAX_USABLE_SEQUENCE_NUMBER].inner()</code> to be
    /// usable as a normal sequence number.
    #[inline]
    #[must_use]
    pub const fn new_unchecked(sequence_number: u64) -> Self {
        Self(sequence_number)
    }

    #[inline]
    #[must_use]
    pub const fn inner(self) -> u64 {
        self.0
    }

    /// Attempts to return `SequenceNumber(last_sequence.0 + additional)`, checking that
    /// overflow does not occur and that the result is a valid and usable sequence number.
    ///
    /// If this returns `Some`, then every sequence number from `last_sequence` up to
    /// the returned sequence number, inclusive, are guaranteed to be valid and usable sequence
    /// numbers.
    #[inline]
    #[must_use]
    pub fn checked_add(self, additional: u64) -> Option<Self> {
        let new_sequence_number = self.0.checked_add(additional)?;

        if new_sequence_number <= Self::MAX_USABLE_SEQUENCE_NUMBER.0 {
            Some(Self(new_sequence_number))
        } else {
            None
        }
    }

    /// Attempts to return `SequenceNumber(last_sequence.0 + u64::from(additional))`, checking that
    /// overflow does not occur and that the result is a valid and usable sequence number.
    ///
    /// If this returns `Some`, then every sequence number from `last_sequence` up to
    /// the returned sequence number, inclusive, are guaranteed to be valid and usable sequence
    /// numbers.
    #[inline]
    #[must_use]
    pub fn checked_add_u32(self, additional: u32) -> Option<Self> {
        self.checked_add(u64::from(additional))
    }
}

// ================================================================
//  Version Edit types
// ================================================================

// Also see `crate::version::version_edit::VersionEdit`, which handles a persistent format.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub(crate) struct FileNumber(pub u64);

#[derive(Debug, Clone, Copy)]
pub(crate) enum VersionEditTag {
    Comparator,
    LogNumber,
    NextFileNumber,
    LastSequence,
    CompactPointer,
    DeletedFile,
    NewFile,
    /// No longer used, but still tracked in case we read a database made by an old version
    /// of LevelDB.
    PrevLogNumber,
}

injective_enum_map! {
    VersionEditTag, u32,
    Comparator     <=> 1,
    LogNumber      <=> 2,
    NextFileNumber <=> 3,
    LastSequence   <=> 4,
    CompactPointer <=> 5,
    DeletedFile    <=> 6,
    NewFile        <=> 7,
    // Skipping 8 is intentional
    PrevLogNumber  <=> 9,
}

// ================================================================
//  Write log format
// ================================================================

// Also see `crate::write_log::{WriteLogReader, WriteLogWriter}`, which handle a persistent format.

#[derive(Debug, Clone, Copy)]
pub(crate) enum WriteLogRecordType {
    Zero,
    Full,
    First,
    Middle,
    Last,
}

impl WriteLogRecordType {
    pub(crate) const ALL_TYPES: [Self; 5] = [
        Self::Zero, Self::Full, Self::First, Self::Middle, Self::Last,
    ];
}

injective_enum_map! {
    WriteLogRecordType, u8,
    Zero   <=> 0,
    Full   <=> 1,
    First  <=> 2,
    Middle <=> 3,
    Last   <=> 4,
}

pub(crate) trait IndexRecordTypes<T> {
    #[must_use]
    fn infallible_index(&self, record_type: WriteLogRecordType) -> &T;
}

impl<T> IndexRecordTypes<T> for [T; WriteLogRecordType::ALL_TYPES.len()] {
    fn infallible_index(&self, record_type: WriteLogRecordType) -> &T {
        // We need to ensure that `0 <= usize::from(u8::from(record_type)) < self.len()`.
        // This holds, since `self.len() == WriteLogRecordType::ALL_TYPES.len() == 5`,
        // and `0 <= usize::from(u8::from(record_type)) < 5`.
        #[expect(clippy::unwrap_used, reason = "See above. Not pressing enough to use `unsafe`")]
        self.get(usize::from(u8::from(record_type))).unwrap()
    }
}

const CHECKSUM_MASK_DELTA: u32 = 0x_a282_ead8;

#[inline]
#[must_use]
pub(crate) const fn mask_checksum(unmasked: u32) -> u32 {
    unmasked.rotate_right(15).wrapping_add(CHECKSUM_MASK_DELTA)
}

#[inline]
#[must_use]
pub(crate) const fn unmask_checksum(masked: u32) -> u32 {
    masked.wrapping_sub(CHECKSUM_MASK_DELTA).rotate_left(15)
}

// ================================================================
//  File names
// ================================================================

#[derive(Debug, Clone, Copy)]
pub(crate) enum LevelDBFileName {
    Log {
        file_number: FileNumber,
    },
    Lockfile,
    Table {
        file_number: FileNumber,
    },
    TableLegacyExtension {
        file_number: FileNumber,
    },
    Manifest {
        file_number: FileNumber,
    },
    Current,
    Temp {
        file_number: FileNumber,
    },
    InfoLog,
    OldInfoLog,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl LevelDBFileName {
    #[must_use]
    pub fn parse(file_name: &Path) -> Option<Self> {
        // Currently, all valid file names for LevelDB files are valid 7-bit ASCII and thus
        // valid UTF-8.
        let file_name = file_name.to_str()?;

        // Note that all the valid file names are nonempty
        let &first_byte = file_name.as_bytes().first()?;
        // `from_str_radix` permits a leading sign, including `+`. We need to reject this case.
        if first_byte == b'+' {
            return None;
        }

        if let Some(file_number) = file_name.strip_suffix(".ldb") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Table { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".log") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Log { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".sst") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::TableLegacyExtension { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".dbtmp") {
            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Temp { file_number })

        } else if let Some(file_number) = file_name.strip_prefix("MANIFEST-") {
            // Any file number, even 0, would make it nonempty.
            let &first_num_byte = file_number.as_bytes().first()?;
            // `from_str_radix` permits a leading sign, including `+`. We need to reject this case.
            if first_num_byte == b'+' {
                return None;
            }

            let file_number = FileNumber(u64::from_str_radix(file_number, 10).ok()?);
            Some(Self::Manifest { file_number })

        } else {
            Some(match file_name {
                "LOCK"    => Self::Lockfile,
                "CURRENT" => Self::Current,
                "LOG"     => Self::InfoLog,
                "LOG.old" => Self::OldInfoLog,
                _         => return None,
            })
        }
    }

    #[must_use]
    pub fn file_name(self) -> PathBuf {
        match self {
            Self::Log { file_number }      => format!("{:06}.log", file_number.0).into(),
            Self::Lockfile                 => Path::new("LOCK").to_owned(),
            Self::Table { file_number }    => format!("{:06}.ldb", file_number.0).into(),
            Self::TableLegacyExtension { file_number } => format!("{:06}.sst", file_number.0).into(),
            Self::Manifest { file_number } => format!("MANIFEST-{:06}", file_number.0).into(),
            Self::Current                  => Path::new("CURRENT").to_owned(),
            Self::Temp { file_number }     => format!("{:06}.dbtmp", file_number.0).into(),
            Self::InfoLog                  => Path::new("LOG").to_owned(),
            Self::OldInfoLog               => Path::new("LOG.old").to_owned(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;


    /// Tests that the filenames do not have directory components.
    #[test]
    fn file_name_has_no_slash() {
        for file_number in 0..10 {
            for file_name in [
                LevelDBFileName::Log { file_number },
                LevelDBFileName::Table { file_number },
                LevelDBFileName::TableLegacyExtension { file_number },
                LevelDBFileName::Manifest { file_number },
                LevelDBFileName::Temp { file_number },
            ].map(LevelDBFileName::file_name) {
                assert_eq!(file_name.file_name(), Some(file_name.as_os_str()));
            }
        }

        for file_name in [
            LevelDBFileName::Lockfile,
            LevelDBFileName::Current,
            LevelDBFileName::InfoLog,
            LevelDBFileName::OldInfoLog,
        ].map(LevelDBFileName::file_name) {
            assert_eq!(file_name.file_name(), Some(file_name.as_os_str()));
        }
    }
}

#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style lifetime extension"),
)]

#[cfg(not(feature = "polonius"))]
use std::slice;
use std::io::{Result as IoResult, Write as _};

use bijective_enum_map::injective_enum_map;
use integer_encoding::{VarInt as _, VarIntWriter as _};
use thiserror::Error;

use crate::public_format::{EntryType, LengthPrefixedBytes, WriteEntry};


// ================================================================
//  Key and entry formats
// ================================================================

// Also see `crate::write_batch::WriteBatch`, which handles a persistent format.

/// A reference to a mostly-arbitrary byte slice of user key data.
///
/// When reading a `UserKey` from persistent storage, it should be assumed to be completely
/// arbitrary. When taking a new `UserKey` from the user, the length should be validated to be
/// at most `min(u32::MAX, usize::MAX)`.. minus 8.
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
/// at most `u32::MAX`. (It must be at most `usize::MAX`.)
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
    pub const fn encoded_len(&self) -> usize {
        self.user_key.0.len() + 8
    }

    #[inline]
    #[must_use]
    pub fn encoded_len_u32(&self) -> u32 {
        // `InternalKey` requires that this does not exceed `u32::MAX`.
        u32::try_from(self.encoded_len()).unwrap()
    }

    /// Extends the `output` buffer with the [`EncodedInternalKey`] slice corresponding to `self`.
    #[inline]
    pub fn append_encoded(&self, output: &mut Vec<u8>) {
        output.extend(self.user_key.0);
        output.extend(self.tag().to_le_bytes());
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EncodedInternalEntry<'a>(EncodedInternalKey<'a>, &'a [u8]);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> EncodedInternalEntry<'a> {
    #[inline]
    #[must_use]
    pub fn new(valid_internal_key: EncodedInternalKey<'a>, value: &'a [u8]) -> Self {
        Self(valid_internal_key, value)
    }

    #[inline]
    #[must_use]
    pub const fn encoded_internal_key(self) -> EncodedInternalKey<'a> {
        self.0
    }

    #[inline]
    #[must_use]
    pub const fn value_bytes(self) -> &'a [u8] {
        self.1
    }

    #[must_use]
    pub fn decode(self) -> InternalEntry<'a> {
        let InternalKey {
            user_key,
            sequence_number,
            entry_type,
        } = InternalKey::decode(self.0)
            .expect("the internal key of an `EncodedInternalEntry` must be valid");

        let value = match entry_type {
            EntryType::Deletion => None,
            EntryType::Value    => Some(UserValue(self.1)),
        };

        InternalEntry { user_key, sequence_number, value }
    }

    /// # Safety
    /// The `valid_internal_key` and `value` slices provided to `Self::new` must be valid for at
    /// least `'b`. That is, those slices must not have an exclusive (mutable) reference for at
    /// least `'b` in order to satisfy Rust's aliasing rules, and their backing storage must not
    /// be dropped or otherwise invalidated for at least `'b`.
    ///
    /// This method is primarily intended to be used as stable support for a nightly Polonius
    /// early return of a borrow, so that Polonius can prove that the aliasing and ownership rules
    /// are satisfied.
    #[cfg(not(feature = "polonius"))]
    pub const unsafe fn extend_lifetime<'b>(self) -> EncodedInternalEntry<'b> {
        let key = self.0.0;
        let value = self.1;

        // SAFETY: `key.as_ptr()` is non-null, properly aligned, valid for reads of
        // `key.len()` bytes, points to `key.len()`-many valid bytes, and doesn't
        // have too long of a length, since it came from a valid slice.
        // The sole remaining constraint is the lifetime. The caller asserts that the slices
        // are valid for 'b.
        let key: &'b [u8] = unsafe { slice::from_raw_parts(key.as_ptr(), key.len()) };
        // SAFETY: same as above
        let value: &'b [u8] = unsafe { slice::from_raw_parts(value.as_ptr(), value.len()) };

        EncodedInternalEntry(EncodedInternalKey(key), value)
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

/// A valid [`EncodedInternalKey`], used to get values from a memtable or version set.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct LookupKey<'a>(EncodedInternalKey<'a>);

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
        InternalKey {
            user_key,
            sequence_number,
            entry_type:      EntryType::MAX_TYPE,
        }.append_encoded(buffer);

        Self(EncodedInternalKey(buffer))
    }

    #[inline]
    #[must_use]
    pub fn new_unchecked(bytes: &'a [u8]) -> Self {
        Self(EncodedInternalKey(bytes))
    }

    /// Returns a valid [`EncodedInternalKey`].
    #[inline]
    #[must_use]
    pub const fn encoded_internal_key(self) -> EncodedInternalKey<'a> {
        self.0
    }

    #[must_use]
    pub fn internal_key(self) -> InternalKey<'a> {
        InternalKey::decode(self.encoded_internal_key()).expect(Self::INVALID_ENCODING)
    }

    /// Returns the [`UserKey`] which was used to make this lookup key.
    #[inline]
    #[must_use]
    pub fn user_key(self) -> UserKey<'a> {
        self.0.user_key().expect("a LookupKey is a valid EncodedInternalKey")
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

        let internal_key_len_u32 = internal_key.encoded_len_u32();
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
        self.try_encode_to(output).expect("`output` was not of the correct length");
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
    pub const fn internal_key(&self) -> InternalKey<'a> {
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
    /// If this returns `Ok`, then every sequence number from `last_sequence` up to
    /// the returned sequence number, inclusive, are guaranteed to be valid and usable sequence
    /// numbers.
    #[inline]
    pub fn checked_add(self, additional: u64) -> Result<Self, OutOfSequenceNumbers> {
        let new_sequence_number = self.0.checked_add(additional).ok_or(OutOfSequenceNumbers)?;

        if new_sequence_number <= Self::MAX_USABLE_SEQUENCE_NUMBER.0 {
            Ok(Self(new_sequence_number))
        } else {
            Err(OutOfSequenceNumbers)
        }
    }

    /// Attempts to return `SequenceNumber(last_sequence.0 + u64::from(additional))`, checking that
    /// overflow does not occur and that the result is a valid and usable sequence number.
    ///
    /// If this returns `Ok`, then every sequence number from `last_sequence` up to
    /// the returned sequence number, inclusive, are guaranteed to be valid and usable sequence
    /// numbers.
    #[inline]
    pub fn checked_add_u32(self, additional: u32) -> Result<Self, OutOfSequenceNumbers> {
        self.checked_add(u64::from(additional))
    }
}

#[derive(Error, Debug, Clone, Copy)]
#[error("somehow, the maximum sequence number - which is over 72 quadrillion - was reached")]
pub(crate) struct OutOfSequenceNumbers;

// ================================================================
//  Version Edit types
// ================================================================

// Also see `crate::version::version_edit::VersionEdit`, which handles a persistent format.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub(crate) struct FileNumber(pub u64);

impl FileNumber {
    #[inline]
    pub fn next(self) -> Result<Self, OutOfFileNumbers> {
        self.0.checked_add(1).map(Self).ok_or(OutOfFileNumbers)
    }
}

#[derive(Error, Debug, Clone, Copy)]
#[error("somehow, the maximum file number - which is over 18 quintillion - was reached")]
pub(crate) struct OutOfFileNumbers;

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
        #[expect(
            clippy::indexing_slicing,
            reason = "See above. Not pressing enough to use `unsafe`",
        )]
        &self[usize::from(u8::from(record_type))]
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

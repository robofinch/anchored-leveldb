use std::path::{Path, PathBuf};

use integer_encoding::{VarInt as _, VarIntWriter as _};

use crate::public_format::{EntryType, LengthPrefixedBytes, WriteEntry};


// ================================================================
//  Config constants
// ================================================================

// TODO: make all of these configurable, there's no real reason for them to be constants
// IMO.

/// The maximum number of levels in the LevelDB database.
pub const NUM_LEVELS: u8 = 7;

/// Once there are [`L0_COMPACTION_TRIGGER`]-many level 0 files, compaction begins.
pub const L0_COMPACTION_TRIGGER: u8 = 4;
/// Once there are [`L0_SOFT_FILE_LIMIT`]-many level 0 files, writes are slowed down
/// in order to let compaction catch up.
pub const L0_SOFT_FILE_LIMIT: u8 = 8;
/// Once there are [`L0_HARD_FILE_LIMIT`]-many level 0 files, writes are entirely stopped
/// in order to let compaction catch up.
pub const L0_HARD_FILE_LIMIT: u8 = 12;

pub const MAX_LEVEL_FOR_COMPACTION: u8 = 2;

pub const READ_SAMPLE_PERIOD: u32 = 2 << 20;


// ================================================================
//  Key and entry formats
// ================================================================

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
pub struct UserKey<'a>(pub &'a [u8]);

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
pub struct EncodedInternalKey<'a>(pub &'a [u8]);

impl<'a> EncodedInternalKey<'a> {
    pub fn user_key(self) -> Result<UserKey<'a>, ()> {
        let user_key_len = self.0.len()
            .checked_sub(8)
            .ok_or(())?;

        Ok(UserKey(&self.0[..user_key_len]))
    }

    fn split(self) -> Result<(UserKey<'a>, u64), ()> {
        let user_key_len = self.0.len()
            .checked_sub(8)
            .ok_or(())?;

        let (user_key, last_eight_bytes) = self.0.split_at(user_key_len);
        let last_eight_bytes: [u8; 8] = last_eight_bytes.try_into().unwrap();

        Ok((
            UserKey(user_key),
            u64::from_le_bytes(last_eight_bytes),
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InternalKey<'a> {
    pub user_key:        UserKey<'a>,
    pub sequence_number: SequenceNumber,
    pub entry_type:      EntryType,
}

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

    /// Returns the length of the [`EncodedInternalKey`] slice corresponding to `self`.
    #[inline]
    #[must_use]
    pub fn encoded_length(&self) -> usize {
        self.user_key.0.len() + 8
    }

    /// Returns the length of the [`EncodedInternalKey`] slice corresponding to `self`.
    ///
    /// # Panics
    /// Panics if `self.user_key` was not validated to have length at most `u32::MAX - 8`.
    #[inline]
    #[must_use]
    pub fn encoded_length_u32(&self) -> u32 {
        #[expect(clippy::unwrap_used, reason = "Intentional; panic is declared")]
        u32::try_from(self.encoded_length()).unwrap()
    }

    #[inline]
    pub fn append_encoded(&self, output: &mut Vec<u8>) {
        output.extend(self.user_key.0);
        output.extend(self.tag().to_le_bytes());
    }
}

/// A possibly-valid encoding of a [`MemtableEntry`].
///
/// The referenced byte slice should be the concatenation of the following:
/// - the backing byte slice of a [`LengthPrefixedBytes`] value wrapping the backing byte slice of
///   an [`EncodedInternalKey`] value,
/// - the backing byte slice of a [`LengthPrefixedBytes`] value wrapping an arbitrary byte slice
///   of a user value.
///
/// Fully expanded, the referenced slice should consist of the following:
/// - `internal_key_len`, a varint32,
/// - `user_key`, a byte slice of user key data of length `internal_key_len - 8`,
/// - `seq_and_type_tag`, 8 bytes encoding a [`SequenceNumber`] and [`EntryType`],
/// - `value_len`, a varint32,
/// - `value`, a byte slice of length `value_len`.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct EncodedMemtableEntry<'a>(pub &'a [u8]);

impl<'a> EncodedMemtableEntry<'a> {
    /// Extends the `output` buffer with an `EncodedMemtableEntry` formed from the provided
    /// [`WriteEntry`] and [`SequenceNumber`].
    ///
    /// The [`WriteEntry`] determines the values of `internal_key_len`, `user_key`,
    /// `value_len`, and `value`. Note that the [`WriteEntry::Deletion`] case uses an empty
    /// slice for the `value`.
    ///
    /// The provided [`SequenceNumber`] and the [`EntryType`] corresponding to the [`WriteEntry`]
    /// together determine the value of `seq_and_type_tag`.
    pub fn write(
        write_entry:     WriteEntry<'a>,
        sequence_number: SequenceNumber,
        output:          &'a mut Vec<u8>,
    ) -> Self {
        let start_idx = output.len();

        let (key, value) = match write_entry {
            WriteEntry::Deletion { key }     => (key, None),
            WriteEntry::Value { key, value } => (key, Some(value)),
        };

        let internal_key = InternalKey {
            user_key:        UserKey(key.data()),
            sequence_number,
            entry_type:      write_entry.entry_type(),
        };

        #[expect(
            clippy::unwrap_used,
            reason = "WriteEntry is validated on construction; the length is at most `u32::MAX-8`",
        )]
        let user_key_len = u32::try_from(key.data().len()).unwrap();

        output.write_varint(user_key_len).expect("writing to a Vec does not fail");
        internal_key.append_encoded(output);

        if let Some(value) = value {
            // Write the already-prefixed data as `value_len` and `value`.
            output.extend(value.prefixed_data());
        } else {
            // Use an empty `value`; we need only write `0` for `value_len`.
            output.write_varint(0_u32).expect("writing to a Vec does not fail");
        }

        EncodedMemtableEntry(&output[start_idx..])
    }

    pub fn user_key(self) -> Result<UserKey<'a>, ()> {
        let (prefixed_internal_key, _) = LengthPrefixedBytes::parse(self.0)?;
        let internal_key = EncodedInternalKey(prefixed_internal_key.data());
        internal_key.user_key()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MemtableEntry<'a> {
    pub internal_key: InternalKey<'a>,
    pub value:        Option<LengthPrefixedBytes<'a>>,
}

impl<'a> MemtableEntry<'a> {
    #[must_use]
    pub fn new(write_entry: WriteEntry<'a>, sequence_number: SequenceNumber) -> Self {
        let (key, value) = match write_entry {
            WriteEntry::Deletion { key }     => (key, None),
            WriteEntry::Value { key, value } => (key, Some(value)),
        };

        let internal_key = InternalKey {
            user_key:        UserKey(key.data()),
            sequence_number,
            entry_type:      write_entry.entry_type(),
        };

        Self { internal_key, value }
    }

    pub fn decode(memtable_entry: EncodedMemtableEntry<'a>) -> Result<Self, ()> {
        let (prefixed_internal_key, after_key) = LengthPrefixedBytes::parse(memtable_entry.0)?;
        let internal_key = EncodedInternalKey(prefixed_internal_key.data());
        let internal_key = InternalKey::decode(internal_key)?;

        let (prefixed_value, after_value) = LengthPrefixedBytes::parse(after_key)?;

        // We should have parsed the entire entry
        if !after_value.is_empty() {
            return Err(());
        }

        let value = match internal_key.entry_type {
            EntryType::Deletion => {
                // The "value" associated with a deletion `WriteEntry` should be the empty slice.
                if !prefixed_value.data().is_empty() {
                    return Err(());
                }

                None
            }
            EntryType::Value => {
                // Any possible value is valid in this case
                Some(prefixed_value)
            }
        };

        Ok(Self { internal_key, value })
    }

    /// Returns the length of the [`EncodedMemtableEntry`] slice corresponding to `self`.
    #[must_use]
    pub fn encoded_length(&self) -> usize {
        // Number of bytes required for the `internal_key_len` varint32 used as the length prefix
        // for `self.internal_key`.
        // Note that `WriteEntry` validates that the key it received has length at most
        // `u32::MAX - 8`, so this does not panic.
        let len_of_internal_key_len = u32::required_space(self.internal_key.encoded_length_u32());

        let len_of_internal_key = self.internal_key.encoded_length();

        let prefixed_value_len = if let Some(prefixed_value) = self.value {
            prefixed_value.prefixed_data().len()
        } else {
            u32::required_space(0)
        };

        // This covers all 5 components of the `EncodedMemtableEntry` slice, noting that
        // `len_of_internal_key` and `prefixed_value_len` are each the length of two components.
        len_of_internal_key_len + len_of_internal_key + prefixed_value_len
    }
}

#[inline]
#[must_use]
pub fn sequence_and_type_tag(sequence_number: SequenceNumber, entry_type: EntryType) -> u64 {
    (sequence_number.0 << 8) | u64::from(u8::from(entry_type))
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct SequenceNumber(pub u64);

impl SequenceNumber {
    pub const MAX_SEQUENCE_NUMBER: Self = Self((1 << 56) - 1);
}

// ================================================================
//  File names
// ================================================================

#[derive(Debug, Clone, Copy)]
pub enum LevelDBFileName {
    Log {
        file_number: u64,
    },
    Lockfile,
    Table {
        file_number: u64,
    },
    TableLegacyExtension {
        file_number: u64,
    },
    Manifest {
        file_number: u64,
    },
    Current,
    Temp {
        file_number: u64,
    },
    InfoLog,
    OldInfoLog,
}

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
            let file_number = u64::from_str_radix(file_number, 10).ok()?;
            Some(Self::Table { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".log") {
            let file_number = u64::from_str_radix(file_number, 10).ok()?;
            Some(Self::Log { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".sst") {
            let file_number = u64::from_str_radix(file_number, 10).ok()?;
            Some(Self::TableLegacyExtension { file_number })

        } else if let Some(file_number) = file_name.strip_suffix(".dbtmp") {
            let file_number = u64::from_str_radix(file_number, 10).ok()?;
            Some(Self::Temp { file_number })

        } else if let Some(file_number) = file_name.strip_prefix("MANIFEST-") {
            // Any file number, even 0, would make it nonempty.
            let &first_num_byte = file_number.as_bytes().first()?;
            // `from_str_radix` permits a leading sign, including `+`. We need to reject this case.
            if first_num_byte == b'+' {
                return None;
            }

            let file_number = u64::from_str_radix(file_number, 10).ok()?;
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
            Self::Log { file_number }      => format!("{file_number:06}.log").into(),
            Self::Lockfile                 => Path::new("LOCK").to_owned(),
            Self::Table { file_number }    => format!("{file_number:06}.ldb").into(),
            Self::TableLegacyExtension { file_number } => format!("{file_number:06}.sst").into(),
            Self::Manifest { file_number } => format!("MANIFEST-{file_number:06}").into(),
            Self::Current                  => Path::new("CURRENT").to_owned(),
            Self::Temp { file_number }     => format!("{file_number:06}.dbtmp").into(),
            Self::InfoLog                  => Path::new("LOG").to_owned(),
            Self::OldInfoLog               => Path::new("LOG.old").to_owned(),
        }
    }
}

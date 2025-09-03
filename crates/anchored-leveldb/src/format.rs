use std::ops::Deref;
use std::path::{Path, PathBuf};

use integer_encoding::VarInt;


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

/// An arbitrary byte slice, used as a key by the LevelDB user.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct UserKey<'a>(pub &'a [u8]);

/// A possibly-valid encoding of an [`InternalKey`].
///
/// The byte slice _should_ consist of [`UserKey`] data followed by 8 bytes.
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
        let entry_type      = EntryType::try_from(tag as u8)?;

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
    pub fn append_encoded(&self, output: &mut Vec<u8>) {
        output.extend(self.user_key.0);
        output.extend(self.tag().to_le_bytes());
    }
}

#[derive(Debug, Clone, Copy)]
pub enum WriteEntry<'a> {
    Value {
        key:   LengthPrefixedBytes<'a>,
        value: LengthPrefixedBytes<'a>,
    },
    Deletion {
        key:   LengthPrefixedBytes<'a>,
    }
}

impl WriteEntry<'_> {
    #[inline]
    #[must_use]
    pub fn entry_type(&self) -> EntryType {
        match self {
            Self::Value { .. }    => EntryType::Value,
            Self::Deletion { .. } => EntryType::Deletion,
        }
    }
}

// TODO: MemtableEntry, which can be obtained from WriteEntry + SequenceNumber

// ================================================================
//  Types and functions used in key and entry formats
// ================================================================

/// A `LengthPrefixedBytes` value is a byte slice formed from the concatenation of:
/// - a varint32 length prefix
/// - a byte slice of the length indicated by the varint32
///
/// Values are verified on construction, so consumers of `LengthPrefixedBytes` values can
/// assume that they are valid.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct LengthPrefixedBytes<'a>(&'a [u8]);

impl<'a> LengthPrefixedBytes<'a> {
    /// If possible, a varint32 `len` is parsed from the start of `src`, and a `LengthPrefixedBytes`
    /// wrapping `len` and the following `len` bytes of `src` is returned.
    ///
    /// This may fail if `src` does not begin with a valid varint32, or if `src` is not long enough
    /// to have `len` bytes following the parsed varint32 `len`.
    pub fn parse(src: &'a [u8]) -> Result<Self, ()> {
        // TODO: do not rely on integer_encoding, I don't like how it ignores some errors
        // and necessitates an extra check to see whether what it tells me is true.
        let (bytes_len, varint_len) = u32::decode_var(src).ok_or(())?;

        let bytes_len_usize = usize::try_from(bytes_len).map_err(|_| ())?;
        let output_len = varint_len.checked_add(bytes_len_usize).ok_or(())?;

        if output_len <= src.len() {
            Ok(Self(&src[..output_len]))
        } else {
            Err(())
        }
    }

    pub fn without_prefix(&self) -> &[u8] {
        let prefix_len = u32::decode_var(self.0).unwrap().1;
        &self[prefix_len..]
    }
}

impl Deref for LengthPrefixedBytes<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0
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

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EntryType {
    Deletion = 0,
    Value    = 1,
}

impl EntryType {
    pub const MIN_TYPE: Self = Self::Deletion;
    pub const MAX_TYPE: Self = Self::Value;
}

impl From<EntryType> for u8 {
    #[inline]
    fn from(entry_type: EntryType) -> Self {
        entry_type as u8
    }
}

impl TryFrom<u8> for EntryType {
    type Error = ();

    #[inline]
    fn try_from(entry_type: u8) -> Result<Self, Self::Error> {
        match entry_type {
            0 => Ok(Self::Deletion),
            1 => Ok(Self::Value),
            _ => Err(()),
        }
    }
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
            return Some(Self::Table { file_number });

        } else if let Some(file_number) = file_name.strip_suffix(".log") {
            let file_number = u64::from_str_radix(file_number, 10).ok()?;
            return Some(Self::Log { file_number });

        } else if let Some(file_number) = file_name.strip_suffix(".sst") {
            let file_number = u64::from_str_radix(file_number, 10).ok()?;
            return Some(Self::TableLegacyExtension { file_number });

        } else if let Some(file_number) = file_name.strip_suffix(".dbtmp") {
            let file_number = u64::from_str_radix(file_number, 10).ok()?;
            return Some(Self::Temp { file_number });

        } else if let Some(file_number) = file_name.strip_prefix("MANIFEST-") {
            // Any file number, even 0, would make it nonempty.
            let &first_byte = file_number.as_bytes().first()?;
            // `from_str_radix` permits a leading sign, including `+`. We need to reject this case.
            if first_byte == b'+' {
                return None;
            }

            let file_number = u64::from_str_radix(file_number, 10).ok()?;
            return Some(Self::Manifest { file_number });
        }

        Some(match file_name {
            "LOCK"    => Self::Lockfile,
            "CURRENT" => Self::Current,
            "LOG"     => Self::InfoLog,
            "LOG.old" => Self::OldInfoLog,
            _         => return None,
        })
    }

    pub fn file_name(self) -> PathBuf {
        match self {
            Self::Log { file_number }      => format!("{:06}.log", file_number).into(),
            Self::Lockfile                 => Path::new("LOCK").to_owned(),
            Self::Table { file_number }    => format!("{:06}.ldb", file_number).into(),
            Self::TableLegacyExtension { file_number } => format!("{:06}.sst", file_number).into(),
            Self::Manifest { file_number } => format!("MANIFEST-{:06}", file_number).into(),
            Self::Current                  => Path::new("CURRENT").to_owned(),
            Self::Temp { file_number }     => format!("{:06}.dbtmp", file_number).into(),
            Self::InfoLog                  => Path::new("LOG").to_owned(),
            Self::OldInfoLog               => Path::new("LOG.old").to_owned(),
        }
    }
}

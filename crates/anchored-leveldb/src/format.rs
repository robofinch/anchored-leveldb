use std::path::{Path, PathBuf};

use crate::public_format::EntryType;


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

/// A reference to an arbitrary byte slice of user key data.
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

use std::collections::HashSet;
use std::num::NonZeroU8;
use std::str;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use crate::pub_traits::compression::CompressorId;
use super::types;




// fn is_fsync_error, fn is_compaction_error, fn is_closed_error
// fn merge_worst_error, fn replace_with_writes_closed


// ================================================================
//  Debug implementations that avoid showing too much data
// ================================================================

#[derive(Clone, Copy)]
struct CompressedData<'a>(&'a [u8]);

impl Debug for CompressedData<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "<{} bytes of compressed data>", self.0.len())
    }
}

#[derive(Clone, Copy)]
struct UncompressedData<'a>(&'a [u8]);

impl Debug for UncompressedData<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "<{} bytes of uncompressed data>", self.0.len())
    }
}

struct UserKey(usize);

impl Debug for UserKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "<{} bytes of user key data>", self.0)
    }
}

struct CurrentWithoutNewline<'a>(&'a [u8]);

impl Debug for CurrentWithoutNewline<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let non_whitespace_length = self.0
            .iter()
            .take_while(|ch| ch.is_ascii_alphanumeric() || **ch == b'-')
            .count();
        let whitespace_len = self.0.len() - non_whitespace_length;


        if whitespace_len <= 16 {
            write!(f, "{:?}", self.0)
        } else {
            #[expect(clippy::expect_used, clippy::indexing_slicing, reason = "trivial to verify")]
            let ascii_prefix = str::from_utf8(&self.0[..non_whitespace_length])
                .expect("the first `non_whitespace_length` characters are ASCII");

            write!(f, "{ascii_prefix}<{whitespace_len} bytes of whitespace>")
        }
    }
}

struct CorruptedCurrent<'a>(&'a [u8]);

impl Debug for CorruptedCurrent<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if self.0.len() <= 64 {
            write!(f, "{:?}", self.0)
        } else {
            write!(f, "<{} bytes of data>", self.0.len())
        }
    }
}

// ================================================================
//  Error equivalents to apply `derive(Debug)` to
// ================================================================

#[derive(Debug)]
#[expect(dead_code, reason = "only used in Debug")]
enum WriteError<'a, Fs, Compression, Decompression> {
    BufferAllocErr,
    ManuallyClosed,
    WritesClosedByError,
    WritesClosedByCorruptionError,
    OutOfFileNumbers,
    OutOfSequenceNumbers,
    TableFileUnusable(&'a u64, &'a types::CorruptedTableError<Decompression>),
    Compression(CompressorId, UncompressedData<'a>, &'a Compression),
    Filesystem(&'a types::FilesystemError<Fs>, &'a types::WriteFsError),
}

#[derive(Debug)]
#[expect(dead_code, reason = "only used in Debug")]
enum CorruptionError<'a, InvalidKey, Decompression> {
    MissingCurrent,
    CurrentWithoutNewline(CurrentWithoutNewline<'a>),
    CorruptedCurrent(CorruptedCurrent<'a>),
    MissingManifest(&'a u64),
    CorruptedManifest(&'a u64, &'a types::CorruptedManifestError<InvalidKey>),
    MissingTableFiles(&'a HashSet<u64>),
    CorruptedLog(&'a u64, &'a types::CorruptedLogError),
    MissingTableFile(&'a u64),
    CorruptedTable(&'a u64, &'a types::CorruptedTableError<Decompression>),
    CorruptedVersion(&'a types::CorruptedVersionError<InvalidKey>),
}

#[derive(Debug)]
#[expect(dead_code, reason = "only used in Debug")]
enum CorruptedVersionError<'a, InvalidKey> {
    TableFileNumberTooLarge(&'a u64, &'a u64),
    FileInMultipleLevels(&'a u64, u8, u8),
    OverlappingFileKeyRanges(&'a u64, &'a u64, NonZeroU8),
    InvalidKeyError(&'a u64, UserKey, &'a InvalidKey),
}

#[derive(Debug)]
#[expect(dead_code, reason = "only used in Debug")]
enum CorruptedBlockError<'a, Decompression> {
    ChecksumMismatch(u32, u32),
    Decompression(CompressorId, CompressedData<'a>, &'a Decompression),
}

// ================================================================
//  Debug implementations for error types
// ================================================================

impl Debug for types::SettingsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        #[derive(Debug)]
        #[expect(dead_code, reason = "only used in Debug")]
        enum ByteString<'a> {
            Utf8(&'a str),
            NonUtf8(&'a [u8]),
        }

        impl<'a> ByteString<'a> {
            const fn new(byte_string: &'a [u8]) -> Self {
                if let Ok(utf8) = str::from_utf8(byte_string) {
                    Self::Utf8(utf8)
                } else {
                    Self::NonUtf8(byte_string)
                }
            }
        }

        match self {
            Self::MismatchedComparator { chosen, recorded } => {
                f.debug_struct("SettingsError")
                    .field("chosen",   &ByteString::new(chosen))
                    .field("recorded", &ByteString::new(recorded))
                    .finish()
            }
        }
    }
}

impl<Fs: Debug, Compression: Debug, Decompression: Debug> Debug
for types::WriteError<Fs, Compression, Decompression>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let this = match self {
            Self::BufferAllocErr                => WriteError::BufferAllocErr,
            Self::ManuallyClosed                => WriteError::ManuallyClosed,
            Self::WritesClosedByError           => WriteError::WritesClosedByError,
            Self::WritesClosedByCorruptionError => WriteError::WritesClosedByCorruptionError,
            Self::OutOfFileNumbers              => WriteError::OutOfFileNumbers,
            Self::OutOfSequenceNumbers          => WriteError::OutOfSequenceNumbers,
            Self::TableFileUnusable(table, err)
                => WriteError::TableFileUnusable(table, err),
            Self::Compression(id, data, err)
                => WriteError::Compression(*id, UncompressedData(data), err),
            Self::Filesystem(fs_err, write_err)
                => WriteError::Filesystem(fs_err, write_err),
        };

        Debug::fmt(&this, f)
    }
}

impl<InvalidKey: Debug, Decompression: Debug> Debug
for types::CorruptionError<InvalidKey, Decompression>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let this = match self {
            Self::MissingCurrent
                => CorruptionError::MissingCurrent,
            Self::CurrentWithoutNewline(current)
                => CorruptionError::CurrentWithoutNewline(CurrentWithoutNewline(current)),
            Self::CorruptedCurrent(current)
                => CorruptionError::CorruptedCurrent(CorruptedCurrent(current)),
            Self::MissingManifest(manifest)
                => CorruptionError::MissingManifest(manifest),
            Self::CorruptedManifest(manifest, err)
                => CorruptionError::CorruptedManifest(manifest, err),
            Self::MissingTableFiles(tables)
                => CorruptionError::MissingTableFiles(tables),
            Self::CorruptedLog(log, err)
                => CorruptionError::CorruptedLog(log, err),
            Self::MissingTableFile(file)
                => CorruptionError::MissingTableFile(file),
            Self::CorruptedTable(table, err)
                => CorruptionError::CorruptedTable(table, err),
            Self::CorruptedVersion(version)
                => CorruptionError::CorruptedVersion(version),
        };

        Debug::fmt(&this, f)
    }
}

impl<InvalidKey: Debug> Debug for types::CorruptedVersionError<InvalidKey> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let this = match self {
            Self::TableFileNumberTooLarge(table, next_file_number)
                => CorruptedVersionError::TableFileNumberTooLarge(table, next_file_number),
            Self::FileInMultipleLevels(table, level, other_level)
                => CorruptedVersionError::FileInMultipleLevels(table, *level, *other_level),
            Self::OverlappingFileKeyRanges(table, other_table, level)
                => CorruptedVersionError::OverlappingFileKeyRanges(table, other_table, *level),
            Self::InvalidKeyError(table, user_key, err)
                => CorruptedVersionError::InvalidKeyError(table, UserKey(user_key.len()), err),
        };

        Debug::fmt(&this, f)
    }
}

impl<Decompression: Debug> Debug for types::CorruptedBlockError<Decompression> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let this = match self {
            Self::ChecksumMismatch(in_header, computed)
                => CorruptedBlockError::ChecksumMismatch(*in_header, *computed),
            Self::Decompression(id, compressed, err)
                => CorruptedBlockError::Decompression(*id, CompressedData(compressed), err),
        };

        Debug::fmt(&this, f)
    }
}

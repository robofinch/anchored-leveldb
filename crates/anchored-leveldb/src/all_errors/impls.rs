use std::{mem, str};
use std::{collections::HashSet, error::Error};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use crate::pub_traits::compression::CompressorId;
use crate::pub_typed_bytes::{FileNumber, Level, NonZeroLevel};
use super::types;


#[expect(clippy::unimplemented, clippy::disallowed_macros, reason = "TODO: fill out error stubs")]
impl<Fs, InvalidKey, Compression, Decompression>
    types::RecoveryError<Fs, InvalidKey, Compression, Decompression>
{
    #[must_use]
    pub fn is_fsync_error(&self) -> bool {
        unimplemented!()
    }

    #[inline]
    #[must_use]
    pub const fn is_corruption_error(&self) -> bool {
        matches!(self.kind, types::RecoveryErrorKind::Corruption(_))
    }
}

impl<Fs: Display, InvalidKey: Display, Compression: Display, Decompression: Display> Display
for types::RecoveryError<Fs, InvalidKey, Compression, Decompression>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // TODO: fill out error stubs
        f.debug_struct("RecoveryError").finish_non_exhaustive()
    }
}

impl<Fs: Error, InvalidKey: Error, Compression: Error, Decompression: Error> Error
for types::RecoveryError<Fs, InvalidKey, Compression, Decompression>
{}

#[expect(clippy::unimplemented, clippy::disallowed_macros, reason = "TODO: fill out error stubs")]
impl<Fs, InvalidKey, Compression, Decompression>
    types::RwError<Fs, InvalidKey, Compression, Decompression>
{
    #[must_use]
    pub fn is_fsync_error(&self) -> bool {
        unimplemented!()
    }

    #[inline]
    #[must_use]
    pub const fn is_corruption_error(&self) -> bool {
        matches!(self.kind, types::RwErrorKind::Corruption(_))
    }

    #[must_use]
    pub fn is_closed_error(&self) -> bool {
        unimplemented!()
    }

    #[inline]
    pub fn merge_worst_error(&mut self, other: Self) {
        #[allow(clippy::no_effect_underscore_binding, reason = "temporary code")]
        let _ignore = other;
        unimplemented!()
    }

    #[must_use]
    pub fn replace_with_writes_closed(&mut self) -> Self {
        let replacement = if self.is_corruption_error() {
            types::WriteError::WritesClosedByCorruptionError
        } else {
            types::WriteError::WritesClosedByError
        };

        let replacement = Self {
            db_directory: self.db_directory.clone(),
            kind:         types::RwErrorKind::Write(replacement),
        };

        mem::replace(self, replacement)
    }
}

impl<Fs: Display, InvalidKey: Display, Compression: Display, Decompression: Display> Display
for types::RwError<Fs, InvalidKey, Compression, Decompression>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // TODO: fill out error stubs
        f.debug_struct("RwError").finish_non_exhaustive()
    }
}

impl<Fs: Error, InvalidKey: Error, Compression: Error, Decompression: Error> Error
for types::RwError<Fs, InvalidKey, Compression, Decompression>
{}

impl<Fs: Display> Display for types::DestroyError<Fs> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // TODO: fill out error stubs
        f.debug_struct("DestroyError").finish_non_exhaustive()
    }
}

impl<Fs: Error> Error for types::DestroyError<Fs> {}

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
enum WriteError<'a, Fs, InvalidKey, Compression, Decompression> {
    BufferAllocErr,
    ManuallyClosed,
    WritesClosedByError,
    WritesClosedByCorruptionError,
    OutOfFileNumbers,
    OutOfSequenceNumbers,
    TableFileUnusable(&'a FileNumber, &'a types::CorruptedTableError<InvalidKey, Decompression>),
    Compression(CompressorId, UncompressedData<'a>, &'a Compression),
    Filesystem(&'a types::FilesystemError<Fs>, &'a FileNumber, &'a types::WriteFsError),
}

#[derive(Debug)]
#[expect(dead_code, reason = "only used in Debug")]
enum CorruptionError<'a, InvalidKey, Decompression> {
    MissingCurrent,
    CurrentWithoutNewline(CurrentWithoutNewline<'a>),
    CorruptedCurrent(CorruptedCurrent<'a>),
    MissingManifest(&'a FileNumber),
    CorruptedManifest(&'a FileNumber, &'a types::CorruptedManifestError<InvalidKey>),
    MissingTableFiles(&'a HashSet<FileNumber>),
    CorruptedLog(&'a FileNumber, &'a types::CorruptedLogError),
    MissingTableFile(&'a FileNumber),
    CorruptedTableMetadata(&'a FileNumber, &'a types::CorruptedTableMetadataError<InvalidKey>),
    CorruptedTable(&'a FileNumber, &'a types::CorruptedTableError<InvalidKey, Decompression>),
    CorruptedVersion(&'a types::CorruptedVersionError<InvalidKey>),
    HandlerReportedError,
}

#[derive(Debug)]
#[expect(dead_code, reason = "only used in Debug")]
enum CorruptedVersionError<'a, InvalidKey> {
    TableFileNumberTooLarge(&'a FileNumber, &'a FileNumber),
    FileInMultipleLevels(&'a FileNumber, Level, Level),
    OverlappingFileKeyRanges(&'a FileNumber, &'a FileNumber, NonZeroLevel),
    InvalidUserKey(&'a FileNumber, UserKey, &'a InvalidKey),
    FileSizeTooSmall(&'a FileNumber),
}

#[derive(Debug)]
#[expect(dead_code, reason = "only used in Debug")]
enum CompressedBlockError<'a, Decompression> {
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

impl<Fs: Debug, InvalidKey: Debug, Compression: Debug, Decompression: Debug> Debug
for types::WriteError<Fs, InvalidKey, Compression, Decompression>
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
            Self::Filesystem(fs_err, file, write_err)
                => WriteError::Filesystem(fs_err, file, write_err),
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
            Self::CorruptedTableMetadata(file, err)
                => CorruptionError::CorruptedTableMetadata(file, err),
            Self::CorruptedTable(table, err)
                => CorruptionError::CorruptedTable(table, err),
            Self::CorruptedVersion(version)
                => CorruptionError::CorruptedVersion(version),
            Self::HandlerReportedError
                => CorruptionError::HandlerReportedError,
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
            Self::InvalidUserKey(table, user_key, err)
                => CorruptedVersionError::InvalidUserKey(table, UserKey(user_key.len()), err),
            Self::FileSizeTooSmall(size)
                => CorruptedVersionError::FileSizeTooSmall(size),
        };

        Debug::fmt(&this, f)
    }
}

impl<Decompression: Debug> Debug for types::CompressedBlockError<Decompression> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let this = match self {
            Self::ChecksumMismatch(in_header, computed)
                => CompressedBlockError::ChecksumMismatch(*in_header, *computed),
            Self::Decompression(id, data, err)
                => CompressedBlockError::Decompression(*id, CompressedData(data), err),
        };

        Debug::fmt(&this, f)
    }
}

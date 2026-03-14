use std::{collections::HashSet, io::Error as IoError, path::PathBuf};

use crate::pub_traits::compression::CompressorId;
use crate::pub_typed_bytes::{
    BlockHandle, FileNumber, FileOffset, FileSize, Level, LogicalRecordOffset, NonZeroLevel,
    SequenceNumber, TableBlockOffset,
};


// ================================================================
//  The high-level errors returned by DB methods (and their kinds)
// ================================================================

#[derive(Debug)]
pub struct RecoveryError<Fs, InvalidKey, Compression, Decompression> {
    pub db_directory: PathBuf,
    pub kind:         RecoveryErrorKind<Fs, InvalidKey, Compression, Decompression>,
}

#[derive(Debug)]
pub enum RecoveryErrorKind<Fs, InvalidKey, Compression, Decompression> {
    Options(OptionsError),
    Settings(SettingsError),
    Open(OpenError<Fs>),
    Read(ReadError<Fs>),
    Write(WriteError<Fs, InvalidKey, Compression, Decompression>),
    Corruption(CorruptionError<InvalidKey, Decompression>),
}

#[derive(Debug)]
pub struct RwError<Fs, InvalidKey, Compression, Decompression> {
    pub db_directory: PathBuf,
    pub kind:         RwErrorKind<Fs, InvalidKey, Compression, Decompression>,
}

#[derive(Debug)]
pub enum RwErrorKind<Fs, InvalidKey, Compression, Decompression> {
    Options(OptionsError),
    Settings(SettingsError),
    Read(ReadError<Fs>),
    Write(WriteError<Fs, InvalidKey, Compression, Decompression>),
    Corruption(CorruptionError<InvalidKey, Decompression>),
}

#[derive(Debug)]
pub struct DestroyError<Fs> {
    pub db_directory: PathBuf,
    pub kind:         DestroyErrorKind<Fs>,
}

#[derive(Debug)]
pub enum DestroyErrorKind<Fs> {
    DatabaseLocked,
    OpenDatabaseDirectory(Fs),
    ReadDatabaseDirectory(Fs),
    LockError(Fs),
    RemoveFileErrors(Vec<(Fs, RemoveError)>),
}

// ================================================================
//  The many error types used by the above errors.
// ================================================================

#[derive(Debug)]
pub enum FilesystemError<Fs> {
    Io(IoError),
    FsError(Fs),
}

#[derive(Debug)]
pub enum RemoveError {
    ReadDatabaseDirectory,
    RemoveFileError(PathBuf),
}

/// One or more database options were not valid.
///
/// "Options" are relevant for performance and other configuration that isn't absolutely critical
/// for the persistent database format.
///
/// However, within options, "format options" still affect the persistent database format, and poor
/// choices of format options may *substantially* impact the performance of the database.
/// Choosing different compressors or block sizes can affect the rate of compression, and therefore
/// the memory used by the database. Opening the same LevelDB database with different options
/// for the maximum sizes of each level of the database can trigger a wave of compactions.
#[derive(Debug, Clone, Copy)]
pub enum OptionsError {
    /// Set [`create_if_missing`] to `false` and [`error_if_exists`] to `true`, meaning that the
    /// database could not possibly be opened successfully.
    ErrorIfMissingOrExists,
    /// A compressor for flushing write buffers (that is, memtables) was selected, but is not
    /// supported by the chosen set of compression codecs.
    ///
    /// # Data
    /// The unwrapped [`fast_compressor`] option.
    UnsupportedFastCompressor(CompressorId),
    /// The chosen compressor for writing data is not supported by the chosen set of compression
    /// codecs.
    ///
    /// # Data
    /// The [`compressor`] option.
    UnsupportedCompressor(CompressorId),
}

/// One or more database settings were not valid.
///
/// "Settings" significantly impact the persistent database format, such that opening the same
/// LevelDB database with different settings might fail.
pub enum SettingsError {
    /// The chosen comparator's name does not match the comparator name recorded in the persistent
    /// database files.
    ///
    /// This could, conceivably, be an extremely rare corruption error rather than user error,
    /// but it's highly probable that the database is intact and the fault lies in choosing the
    /// wrong comparator in settings.
    MismatchedComparator {
        chosen:   Vec<u8>,
        recorded: Vec<u8>,
    },
}

/// Errors exclusive to the process of opening a database.
#[derive(Debug)]
pub enum OpenError<Fs> {
    /// Attempted to open a database which appears to not exist, and the [`create_if_missing`]
    /// option was false.
    DatabaseDoesNotExist,
    /// Attempted to open a database which seems unlikely to exist.
    ///
    /// The database could conceivably be in a corrupted state where returning [`MissingCurrent`]
    /// would be appropriate. Checks to determine whether the database exists but is corrupted
    /// were not entirely conclusive due to a filesystem error.
    ///
    /// This error is returned even if [`create_if_missing`] is `true`, since proceeding under the
    /// assumption that the database does not exist would *severely* exacerbate any corruption
    /// present, even if that scenario is unlikely.
    ///
    /// # Data
    /// The filesystem error that occurred while checking whether the database exists but is
    /// corrupted.
    ///
    /// [`MissingCurrent`]: CorruptionError::MissingCurrent
    DatabaseProbablyDoesNotExist(FilesystemError<Fs>),
    /// Attempted to open an database which appears to already exist, and the [`error_if_exists`]
    /// option was `true`.
    ///
    /// (Note that the slightly-more-precise `DatabaseLocked` error may be returned if
    /// the database exists *and* is locked, even if [`error_if_exists`] is `true`.)
    DatabaseExists,
    /// The database's `LOCK` file was already locked, likely indicating that some other LevelDB
    /// client is using the database.
    ///
    /// This error implies that the database presumably exists but could not be opened at
    /// present time.
    DatabaseLocked,
    /// Attempted to open a database whose `CURRENT` file is invalid. The file might be encrypted
    /// or corrupted.
    ///
    /// Note that if the `CURRENT` file's contents begin with `MANIFEST-`, then the file is assumed
    /// to not be encrypted and a corruption error is returned.
    ///
    /// # Data
    /// The contents of the `CURRENT` file.
    EncryptedDatabaseOrCorruptedCurrent(Vec<u8>),
    /// An error occurred due to a filesystem error while attempting to open the database.
    ///
    /// (This excludes [`Self::DatabaseProbablyDoesNotExist`], as (setting aside
    /// [`create_if_missing`]) the filesystem error is very likely to have simply converted one
    /// error to another by preventing [`Self::DatabaseDoesNotExist`] from being returned.)
    Filesystem(FilesystemError<Fs>, OpenFsError),
}

/// A kind of error that occurred due to a filesystem error while attempting to open the database.
#[derive(Debug, Clone, Copy)]
pub enum OpenFsError {
    /// Attempting to acquire a lockfile, creating it if it does not exist, failed for an
    /// uncategorized reason (i.e. not because the file is already locked).
    ///
    /// In particular, this error is returned only if:
    /// 1. An initial attempt to acquire the `LOCK` file in the database directory failed for some
    ///    reason other than it already being locked. (For instance, the lockfile might not exist.)
    /// 2. A `CURRENT` file was confirmed to exist in the database directory.
    /// 3. A second attempt to acquire the `LOCK` file is made, creating the file if necessary,
    ///    and it failed for some reason other than it already being locked.
    AcquireLockfile,
    /// Attempting to create and acquire a lockfile, additionally creating any missing parent
    /// directories, failed.
    ///
    /// In particular, this operation is run only when the lockfile is thought to not exist,
    /// though an existing lockfile could become present in a time-of-check to time-of-use scenario.
    CreateLockfile,
    /// It is unknown whether the database exists or not.
    ///
    /// In particular, checking whether the `CURRENT` file of the database exists was inconclusive,
    /// perhaps due to lacking sufficient permissions over the database directory.
    UnknownExistence,
    /// Writing a new, empty database into the database directory failed.
    ///
    /// In this situation, the database directory itself has already been created (if necessary)
    /// and the `LOCK` file has been created (if necessary) and acquired.
    InitEmptyDatabase(InitEmptyDatabaseError),
    /// The `CURRENT` file of the database could not be opened, perhaps due to a permissions error.
    ///
    /// Note that this error is returned when the `CURRENT` file is expected to exist, though
    /// it is not strictly guaranteed that it still does (due to time-of-check to time-of-use
    /// scenarios). More precise errors may be returned if that earlier existence check failed.
    OpenCurrent,
    /// The database's `CURRENT` file was opened, but its contents were not successfully read.
    ReadCurrent,
    /// A `MANIFEST` file could not be opened for some reason other than it not existing.
    ///
    /// # Data
    /// The `MANIFEST`'s file number.
    OpenManifest(FileNumber),
    /// Reading the contents of a `MANIFEST` file failed due to a filesystem error.
    ///
    /// # Data
    /// The `MANIFEST`'s file number.
    ReadManifest(FileNumber),
    /// Iterating through the relative paths of files in the database directory failed due to a
    /// filesystem error.
    ///
    /// Either the iterator failed to be created in the first place, or getting the next filename
    /// failed.
    ReadDatabaseDirectory,
    /// A `.log` write-ahead log file could not be opened for some reason.
    ///
    /// # Data
    /// The file number of the `.log` file.
    OpenLog(FileNumber),
}

/// Writing a new, empty database into the database directory failed.
///
/// This process does not include creating the database directory or acquiring the `LOCK` file.
#[derive(Debug, Clone, Copy)]
pub enum InitEmptyDatabaseError {
    /// Opening the `MANIFEST-000001` file in the database directory failed.
    ///
    /// Note that `MANIFEST-000001` is the first `MANIFEST` file of a new, empty database.
    OpenManifest,
    /// Writing the `MANIFEST-000001` file in the database directory failed.
    ///
    /// Note that `MANIFEST-000001` is the first `MANIFEST` file of a new, empty database.
    WriteManifest,
    /// Syncing the contents of the `MANIFEST-000001` file in the database directory
    /// (that is, flushing its contents to persistent storage, if supported) failed.
    ///
    /// Note that `MANIFEST-000001` is the first `MANIFEST` file of a new, empty database.
    SyncManifest,
    /// Creating a `CURRENT` file that points to `MANIFEST-000001` failed.
    ///
    /// Note that `MANIFEST-000001` is the first `MANIFEST` file of a new, empty database.
    SetCurrent(SetCurrentError),
}

/// Changing the database's `CURRENT` file to point to some new `MANIFEST` failed.
#[derive(Debug, Clone, Copy)]
pub enum SetCurrentError {
    /// Opening a temporary file in the database directory failed.
    OpenTemp,
    /// Writing to a temporary file in the database directory failed.
    WriteTemp,
    /// Syncing the contents of a temporary file in the database directory (that is, flushing
    /// its contents to persistent storage, if supported) failed.
    SyncTemp,
    /// Renaming a temporary file to atomically replace the database's `CURRENT` file failed.
    ///
    /// On Unix systems, this includes an attempt to sync the database directory after executing
    /// the rename.
    ///
    /// Note that it appears that some filesystems and operating systems do not guarantee that
    /// the rename is atomic if the system crashes while the operation is performed; that is, a
    /// future attempt to reopen the database might see that the `CURRENT` file does not exist.
    /// So long as the whole system does not crash, though, (and even if the *program* crashes,)
    /// there is no moment in time during which a program could see the `CURRENT` file missing.
    /// (This is the sense in which the rename is atomic.)
    ///
    /// In the situation where the system crashes at the most inopportune moment, a future attempt
    /// to open that database with `anchored-leveldb` would return a [`MissingCurrent`] corruption
    /// error, and manual recovery should be attempted. Attempting to open that database with
    /// Google's `leveldb` might do Very Bad Things, but this atomic rename strategy is what
    /// `leveldb` does, so that is no worse than the status quo. This situation *can* be fixed
    /// by extending the persistent database format, but doing so in a way backwards-compatible
    /// with `leveldb` would be excessively complicated in the eyes of this author. Instead,
    /// `anchored-leveldb` should provide utilities for recovering corrupted databases, including
    /// this relatively easy-to-fix case.
    ///
    /// [`MissingCurrent`]: CorruptionError::MissingCurrent
    RenameTempToCurrent,
}

/// An error occurred while attempting to read a database, for some reason not covered by other
/// cases.
#[derive(Debug)]
pub enum ReadError<Fs> {
    BufferAllocErr,
    /// The database has been (or is currently being) manually closed with [`try_close_nonblocking`]
    /// or [`force_close_all_nonblocking`].
    ManuallyClosed,
    /// A table file whose file size exceeds `usize::MAX` contains a block whose length
    /// exceeds `usize::MAX - 5`, resulting in reading that block being impossible on this
    /// computer due to `usize` overflow.
    ///
    /// # Data
    /// The file number of the table file followed by the handle of the block which could not
    /// be read.
    BlockUsizeOverflow(FileNumber, BlockHandle),
    Filesystem(FilesystemError<Fs>, ReadFsError),
}

#[derive(Debug, Clone, Copy)]
pub enum ReadFsError {
    /// A table file could not be opened for some reason other than it not existing.
    ///
    /// # Data
    /// The file number of the table file.
    OpenTableFile(FileNumber),
    /// Reading the contents of a table file failed due to a filesystem error.
    ///
    /// # Data
    /// The file number of the table file.
    ReadTableFile(FileNumber),
}

/// An error occurred while attempting to write a database, for some reason not covered by other
/// cases.
pub enum WriteError<Fs, InvalidKey, Compression, Decompression> {
    BufferAllocErr,
    /// The database has been (or is currently being) manually closed with [`try_close_nonblocking`]
    /// or [`force_close_all_nonblocking`].
    ManuallyClosed,
    /// An error (of any kind) occurred while attempting to write to the database, so all
    /// writes are closed.
    WritesClosedByError,
    /// A [`CorruptionError`] occurred while accessing the database (in any way), so all
    /// writes are closed.
    ///
    /// This error takes priority over [`Self::WritesClosedByError`].
    WritesClosedByCorruptionError,
    OutOfFileNumbers,
    OutOfSequenceNumbers,
    /// A just-written table file is corrupted, and will therefore be discarded.
    ///
    /// Either the filesystem genuinely did fail to save that table file in some way, or the
    /// compressors and/or decompressors provided in the database settings are implemented
    /// incorrectly.
    ///
    /// # Data
    /// The file number of the table file, followed by the type of corruption that occurred.
    TableFileUnusable(FileNumber, CorruptedTableError<InvalidKey, Decompression>),
    /// Attempting to compress data failed (for some reason other than not supporting the indicated
    /// type of compression or failing to allocate a buffer).
    ///
    /// # Data
    /// The fields indicate the selected compressor, the uncompressed data, and the resulting
    /// compression error, respectively.
    Compression(CompressorId, Vec<u8>, Compression),
    /// An error occurred due to a filesystem error while attempting to write to part of the
    /// database.
    Filesystem(FilesystemError<Fs>, FileNumber, WriteFsError),
}

#[derive(Debug, Clone, Copy)]
pub enum WriteFsError {
    OpenWritableTableFile,
    WriteTableFile,
    SyncTableFile,
    TableFileUnusable,
    OpenWritableLog,
    OpenAppendableLog,
    WriteLog,
    SyncLog,
    OpenWritableManifest,
    OpenAppendableManifest,
    WriteManifest,
    SyncManifest,
    /// Changing the `CURRENT` file of the database to point to a newly-written `MANIFEST` file
    /// failed.
    ///
    /// # Data
    /// The type of error that occurred when changing `CURRENT`. Note that the associated file
    /// number (in the [`WriteError::Filesystem`] error) is the file number of the new `MANIFEST`
    /// file.
    SetCurrent(SetCurrentError),
}

pub enum CorruptionError<InvalidKey, Decompression> {
    /// Attempted to open a database which appears to exist but which has no `CURRENT` file in the
    /// database directory.
    ///
    /// This can, theoretically, be encountered with normal usage of `leveldb`, `anchored-leveldb`,
    /// and many other LevelDB implementations on some operating systems and file systems.
    MissingCurrent,
    /// The `CURRENT` file of a database looks mostly valid (takes the form
    /// `MANIFEST-[u64 number][whitespace*]`), but the whitespace at the end of the file (if any)
    /// is not a valid newline.
    ///
    /// Valid whitespace endings are `CR`, `LF`, and `CRLF`.
    ///
    /// # Data
    /// The contents of the `CURRENT` file.
    CurrentWithoutNewline(Vec<u8>),
    /// The `CURRENT` file of a database starts with `MANIFEST-` but does not take the form
    /// `MANIFEST-[u64 number][whitespace*]`; in other words, it is significantly corrupted
    /// (and presumably not encrypted).
    ///
    /// # Data
    /// The contents of the `CURRENT` file.
    CorruptedCurrent(Vec<u8>),
    /// The database's `CURRENT` file points to a nonexistent `MANIFEST` file.
    ///
    /// # Data
    /// The `MANIFEST`'s file number.
    MissingManifest(FileNumber),
    /// The current `MANIFEST` file is corrupted.
    ///
    /// See [`BinaryBlockLogCorruptionError`] for the kinds of corruption that may be present in the
    /// physical and logical records of a `MANIFEST` file. Even if the logical records are intact,
    /// they may fail to correctly parse into [`VersionEdit`]s, resulting in a
    /// [`VersionEditDecodeError`]. Lastly, the sequence of [`VersionEdit`]s might not form a
    /// complete and coherent database manifest.
    ///
    /// # Data
    /// The `MANIFEST`'s file number, and information about what kind of corruption occurred.
    CorruptedManifest(FileNumber, CorruptedManifestError<InvalidKey>),
    /// The database refers to several table files (`.ldb` or `.sst` files) which do not exist.
    ///
    /// # Data
    /// A set of the `FileNumbers` of missing table files.
    MissingTableFiles(HashSet<FileNumber>),
    /// A `.log` write-ahead log file is corrupted.
    ///
    /// See [`BinaryBlockLogCorruptionError`] for the kinds of corruption that may be present in the
    /// physical and logical records of a `.log` file. Even if the logical records are intact, they
    /// may fail to correctly parse into [`WriteBatch`]es, resulting in a [`WriteBatchDecodeError`].
    /// Lastly, the sequence of [`WriteBatch`]es might not have monotonically increasing sequence
    /// numbers.
    ///
    /// # Data
    /// The file number of the `.log` file, and information about what kind of corruption occurred.
    CorruptedLog(FileNumber, CorruptedLogError),
    /// The database refers to a table files (a `.ldb` or `.sst` file) which does not exist.
    ///
    /// # Data
    /// The file number of the missing table file.
    MissingTableFile(FileNumber),
    /// A `.ldb` or `.sst` table file is corrupted.
    ///
    /// # Data
    /// The file number of the corrupted table file, and information about what kind of
    /// corruption occurred.
    CorruptedTable(FileNumber, CorruptedTableError<InvalidKey, Decompression>),
    /// The new [`Version`] produced by a compaction is corrupted. This version will be discarded,
    /// but the likely cause of this error is that some corruption already in the database was
    /// revealed by the compaction.
    CorruptedVersion(CorruptedVersionError<InvalidKey>),
    /// An [`OpenCorruptionHandler`] indicated that an error occurred, but did not provide
    /// information about the exact cause.
    ///
    /// [`OpenCorruptionHandler`]: crate::pub_traits::error_handler::OpenCorruptionHandler
    HandlerReportedError,
}

/// The current `MANIFEST` file is corrupted.
///
/// See [`BinaryBlockLogCorruptionError`] for the kinds of corruption that may be present in the
/// physical and logical records of a `MANIFEST` file. Even if the logical records are intact, they
/// may fail to correctly parse into [`VersionEdit`]s, resulting in a [`VersionEditDecodeError`].
/// Lastly, the sequence of [`VersionEdit`]s might not form a complete and coherent database
/// manifest.
#[derive(Debug)]
pub enum CorruptedManifestError<InvalidKey> {
    /// A physical or logical record of the current `MANIFEST` file's binary log format is
    /// corrupted (possibly due to a writer crashing while appending to the `MANIFEST`).
    ///
    /// The [`ignore_manifest_corruption`] setting determines which errors are ignored (and
    /// never reported as errors, only logged). The exact type of a reported error is likely
    /// irrelevant and unactionable, but it may be useful for tests.
    ///
    /// # Data
    /// The offset into the `MANIFEST` file at which the corrupted record began, followed by the
    /// kind of error.
    ///
    /// The "corrupted record" may either be a corrupted physical record or an incomplete
    /// fragmented logical record.
    BinaryBlockLogCorruption(FileOffset, BinaryBlockLogCorruptionError),
    /// A logical record of the current `MANIFEST` file failed to correctly parse into
    /// a [`VersionEdit`].
    ///
    /// The exact type of [`VersionEditDecodeError`] is likely irrelevant and unactionable, but it
    /// may be useful for tests.
    ///
    /// # Data
    /// The offset into the logical record at which the error occurred, followed by the kind of
    /// error.
    VersionEditDecode(LogicalRecordOffset, VersionEditDecodeError),
    /// Every database should record a lower bound for the file numbers of any write-ahead log
    /// files (`.log` files), but none of the [`VersionEdit`]s had a `min_log_number` entry.
    ///
    /// (Usually, this is the file number of the oldest `.log` file, unless there are no `.log`
    /// files at all.)
    MissingMinLogNumber,
    /// Every database should record the file number which will be assigned to the next-created
    /// manifest, write-ahead log, or table file, but none of the [`VersionEdit`]s had a
    /// `next_file_number` entry.
    ///
    /// (Temporary `.dbtmp` files do not use the same mechanism to choose their file numbers, and
    /// other files used by the database do not have file numbers at all.)
    MissingNextFileNumber,
    /// Every database should record the largest/most-recent [`SequenceNumber`] used in its entries,
    /// but none of the [`VersionEdit`]s had a `last_sequence` entry.
    MissingLastSequenceNumber,
    /// The [`VersionEdit`]s did not form a valid [`Version`]. Either the `Version` is internally
    /// inconsistent, or one of its table files has a file number greater than or equal to
    /// `next_file_number`.
    CorruptedVersion(CorruptedVersionError<InvalidKey>),
}

#[derive(Debug, Clone, Copy)]
#[expect(variant_size_differences, reason = "not all that large")]
pub enum VersionEditDecodeError {
    /// A varint32 was expected, but the end of input was reached.
    ///
    /// This occurs either if the varint32 is entirely missing (as every varint is at least `1`
    /// byte in length) or if a varint32 had its most-significant bit set to indicate that another
    /// byte should be read (and doing so would not exceed the maximum 5 byte length of a varint32),
    /// but the end of the input was reached.
    TruncatedVarint32,
    /// A varint64 was expected, but the end of input was reached.
    ///
    /// This occurs either if the varint64 is entirely missing (as every varint is at least `1`
    /// byte in length) or if a varint64 had its most-significant bit set to indicate that another
    /// byte should be read (and doing so would not exceed the maximum 10 byte length of a
    /// varint64), but the end of the input was reached.
    TruncatedVarint64,
    /// A varint32 was read that either exceeded 5 bytes in length or would overflow a u32.
    OverflowingVarint32,
    /// A varint64 was read that either exceeded 10 bytes in length or would overflow a u64.
    OverflowingVarint64,
    /// A length-prefixed byte slice was expected, and although its length was successfully read,
    /// the remaining input is shorter than the slice's length.
    TruncatedSlice,
    /// Expected a [`VersionEditTag`] indicating the type of an entry in a [`VersionEdit`], and
    /// found an unknown tag / entry type.
    ///
    /// # Data
    /// The unknown tag.
    UnknownVersionEditTag(u32),
    /// The `last_sequence` field of the [`VersionEdit`] is greater than the
    /// [`MAX_USABLE_SEQUENCE_NUMBER`] (whose value is `(1 << 56) - 2`).
    LastSequenceNumberTooLarge,
    /// A [`Level`] value was expected, but the read value exceeded the maximum level (
    /// which is `6`, one less than [`NUM_LEVELS`]).
    ///
    /// # Data
    /// The overly-large level value.
    LevelTooLarge(u8),
    /// A slice value was expected to be an internal key (which has an 8-byte suffix), but the
    /// slice was fewer than 8 bytes in length.
    InternalKeyTruncated,
    /// The byte of an internal key indicating its [`EntryType`] had an unknown value.
    ///
    /// # Data
    /// The unknown entry type.
    InternalKeyEntryTypeUnknown(u8),
}

impl From<Varint32DecodeError> for VersionEditDecodeError {
    #[inline]
    fn from(error: Varint32DecodeError) -> Self {
        match error {
            Varint32DecodeError::Truncated   => Self::TruncatedVarint32,
            Varint32DecodeError::Overflowing => Self::OverflowingVarint32,
        }
    }
}

// NOTE: in order to make these errors useful, I should save the corrupted files somewhere
// instead of immediately garbage-collecting them.
pub enum CorruptedVersionError<InvalidKey> {
    /// A [`Version`] should never record the existence of a table file whose file number is
    /// greater than or equal to the database's `next_file_number`.
    ///
    /// # Data
    /// The file number of the table, followed by the `next_file_number` value.
    TableFileNumberTooLarge(FileNumber, FileNumber),
    /// Every table file of the database should be in a specific level; it is never correct for one
    /// to be in multiple levels.
    ///
    /// (Previously-used table files, no longer in any level of the database, might linger in the
    /// database directory for a short time before removed. However, there is no such edge case
    /// allowing a table file to ever be in more than one level.)
    ///
    /// # Data
    /// The file number of the table, followed by two of the levels it's in.
    FileInMultipleLevels(FileNumber, Level, Level),
    /// Every non-zero level of the database should consist of a sorted list of table files
    /// whose key ranges do not overlap.
    ///
    /// That is, the largest key of any entry in one table should be strictly less than the
    /// smallest key of the next table.
    ///
    /// [`VersionEdit`]s and `MANIFEST` files are not required to record the table files in a
    /// sorted form, but two table files of any non-zero level *must* have non-overlapping
    /// key ranges.
    ///
    /// # Data
    /// The file numbers of two file numbers which overlap, and the nonzero level they are both
    /// located in.
    OverlappingFileKeyRanges(FileNumber, FileNumber, NonZeroLevel),
    /// The two endpoints of each table file's key range should be valid/comparable.
    ///
    /// However, the comparator chosen in database settings indicated that such a key is invalid.
    ///
    /// # Data
    /// The file number of the table file with an invalid user key, the contents of that user key
    /// (that is, excluding the 8-byte suffix of internal keys), and the [`InvalidKeyError`]
    /// returned by the chosen comparator.
    InvalidKeyError(FileNumber, Box<[u8]>, InvalidKey),
}

/// Possible kinds of corruption in a `.log` write-ahead log file.
///
/// See [`BinaryBlockLogCorruptionError`] for the kinds of corruption that may be present in the
/// physical and logical records of a `.log` file. Even if the logical records are intact, they
/// may fail to correctly parse into [`WriteBatch`]es, resulting in a [`WriteBatchDecodeError`].
/// Lastly, the sequence of [`WriteBatch`]es might not have monotonically increasing sequence
/// numbers.
#[derive(Debug, Clone, Copy)]
pub enum CorruptedLogError {
    /// A physical or logical record of a `.log` file's binary log format is
    /// corrupted.
    ///
    /// The [`ignore_write_ahead_log_corruption`] setting determines which errors are ignored (and
    /// never reported as errors, only logged). The exact type of a reported error is likely
    /// irrelevant and unactionable, but it may be useful for tests.
    ///
    /// # Data
    /// The offset into the `.log` file at which the corrupted record began, followed by the kind
    /// of error.
    ///
    /// The "corrupted record" may either be a corrupted physical record or an incomplete
    /// fragmented logical record.
    BinaryBlockLogCorruption(FileOffset, BinaryBlockLogCorruptionError),
    /// A logical record of a `.log` file failed to correctly parse into a [`WriteBatch`].
    ///
    /// The exact type of [`WriteBatchDecodeError`] is likely irrelevant and unactionable, but it
    /// may be useful for tests.
    ///
    /// # Data
    /// The offset into the logical record at which the error occurred, followed by the kind of
    /// error.
    WriteBatchDecode(LogicalRecordOffset, WriteBatchDecodeError),
    /// The sequence of [`WriteBatch`]es in the `.log` file do not have monotonically increasing
    /// sequence numbers.
    ///
    /// # Data
    /// The sequence number and length (in entries, not bytes) of one [`WriteBatch`], followed
    /// by the same data of the immediately following [`WriteBatch`] in the `.log` file,
    /// such that the final entry of the first [`WriteBatch`] has a sequence number greater than
    /// or equal to the sequence number of the first entry of the second [`WriteBatch`].
    DecreasingSequenceNumbers(SequenceNumber, u32, SequenceNumber, u32),
}

#[derive(Debug, Clone, Copy)]
pub enum WriteBatchDecodeError {
    /// The write batch was shorter than 12 bytes (the length of a write batch header).
    TruncatedHeader,
    /// The first entry of the write batch has sequence number `0`.
    FirstSequenceZero,
    /// The first entry of the write batch has a sequence number greater than the
    /// [`MAX_USABLE_SEQUENCE_NUMBER`] (whose value is `(1 << 56) - 2`).
    FirstSequenceTooLarge,
    /// The last entry of the write batch has a sequence number greater than the
    /// [`MAX_USABLE_SEQUENCE_NUMBER`] (whose value is `(1 << 56) - 2`).
    LastSequenceTooLarge,
    /// A varint32 was expected, but the end of input was reached.
    ///
    /// This occurs either if the varint32 is entirely missing (as every varint is at least `1`
    /// byte in length) or if a varint32 had its most-significant bit set to indicate that another
    /// byte should be read (and doing so would not exceed the maximum 5 byte length of a varint32),
    /// but the end of the input was reached.
    TruncatedVarint32,
    /// A varint32 was read that either exceeded 5 bytes in length or would overflow a u32.
    OverflowingVarint32,
    /// A length-prefixed byte slice was expected, and although its length was successfully read,
    /// the remaining input is shorter than the slice's length.
    TruncatedSlice,
    /// A key slice had a length strictly greater than `u32::MAX - 8`. All user keys are required to
    /// have length at most `u32::MAX - 8`, to ensure that an 8-byte internal key suffix can be
    /// added.
    KeyTooLong,
    /// The byte of a write batch entry indicating its [`EntryType`] had an unknown value.
    ///
    /// # Data
    /// The unknown entry type.
    UnknownEntryType(u8),
    /// The write batch contained more entries than indicated in its header.
    TooManyEntries,
    /// The write batch contained fewer entries than indicated in its header.
    TooFewEntries,
}

/// Possible kinds of corruption in the physical or logical records of LevelDB's binary block log
/// files (namely, `MANIFEST-X` files and `X.log` files).
///
/// Some errors can be ignored, as configured in the database options. The threshold for which
/// errors to ignore needs to balance:
/// - prevent crashes, which may occur while an entry is being appended to a log, from preventing
///   the database from being opened, and
/// - prevent genuine log corruption from going unnoticed.
///
/// # Background on the format
///
/// The binary block log format stores "logical records" by segmenting them into "physical records".
/// A logical record which is represented as a single physical record uses a [`Full`] physical
/// record[^1]. A logical record which is segmented across multiple physical records is called
/// a "fragmented" logical record and uses one [`First`], zero or more [`Middle`], and one
/// [`Last`] physical record[^1]. Additionally, there is a [`Zero`] physical record type which is
/// never valid. Its presence results in a [`ZeroRecord`] error.
///
/// Internally, the format uses a sequence of 32KiB blocks of data (except for the final block,
/// which may be partial), each of which may hold one or more physical records, while physical
/// records cannot cross a block boundary. The split between "logical records" and
/// "physical records" is necessary to ensure that entries in `MANIFEST` and `.log` files can
/// exceed 32KiB in size.
///
/// [^1]: Old versions of `leveldb` had a bug that allowed a logical record to begin with a
/// completely empty [`First`] physical record, followed by one of the two formats described. These
/// buggy logical records are never written by `anchored-leveldb`, only accepted when reading them.
///
/// [`ZeroRecord`]: BinaryBlockLogCorruptionError::ZeroRecord
#[derive(Debug, Clone, Copy)]
pub enum BinaryBlockLogCorruptionError {
    /// The last physical record in the binary block log file is truncated too short to store
    /// a header.
    TruncatedHeader,
    /// The last physical record in the binary block log file is truncated, as indicated by the
    /// length field of its header.
    ///
    /// This error is returned if the physical record *could* have the length indicated in its
    /// header, without overflowing the block it's in, if the final block of the log file were
    /// the full 32KiB in length.
    TruncatedPhysicalRecord,
    /// The last logical record in the binary block log file had a sequence of complete
    /// `First` and possibly `Middle` physical records, but no `Last` physical record.
    IncompleteLogicalRecord,
    /// The expected checksum from a physical record's header did not match the actual calculated
    /// checksum of the physical record.
    ChecksumMismatch,
    /// The length of a physical record, as given in its header, was too long to possibly be
    /// correct.
    CorruptedRecordLength,
    /// The record type of a physical record, as given in its header, was not among the known
    /// values.
    ///
    /// This includes a [`Zero`] record whose header did not have 0 in its length field; otherwise,
    /// it is based solely on the record type byte.
    ///
    /// # Data
    /// The field of the record's header which should indicate the record type.
    UnknownRecordType(u8),
    /// A [`Full`] physical record occurred in a fragmented record, at least one of whose previous
    /// physical records were nonempty. A fragmented record should not include `Full` physical
    /// records.
    ///
    /// Note that `leveldb`'s `log::Writer` used to have a bug where it could emit an empty
    /// [`First`] physical record in the last seven bytes of a block and fail to realize the record
    /// was fragmented, possibly resulting in the next physical record being a `Full` record;
    /// this error is not returned in that situation.
    ///
    /// This error does not result in the `Full` record being discarded; instead, the preceding
    /// parts of the fragmented logical record are dropped.
    FullInFragmentedRecord,
    /// A [`First`] physical record occurred in a fragmented record, at least one of whose previous
    /// physical records were nonempty. A fragmented record should only have a single `First`
    /// physical record at its beginning.
    ///
    /// Note that `leveldb`'s `log::Writer` used to have a bug where it could emit an empty `First`
    /// physical record in the last seven bytes of a block and fail to realize the record was
    /// fragmented, possibly resulting in the next physical record being a `First` record;
    /// this error is not returned in that situation.
    ///
    /// This error does not result in the `First` record being discarded; instead, the preceding
    /// parts of the fragmented logical record are dropped, and a new fragmented logical record is
    /// begun.
    ExtraFirstInFragmentedRecord,
    /// A [`Middle`] physical record occurred outside a fragmented record (or, a fragmented
    /// record failed to be started with a [`First`] physical record).
    ///
    /// It is possible that a previous would-be `First` or `Middle` physical record in the intended
    /// fragmented record was corrupted.
    ///
    /// The offending `Middle` record is dropped.
    MiddleWithoutFirst,
    /// A [`Last`] physical record occurred outside a fragmented record (or, a fragmented
    /// record failed to be started with a [`First`] physical record).
    ///
    /// It is possible that a previous would-be `First` or [`Middle`] physical record in the
    /// intended fragmented record was corrupted.
    ///
    /// The offending `Last` record is dropped.
    LastWithoutFirst,
    /// A physical record had only `0`'s in the length and record type fields of its header,
    /// denoting a [`Zero`] record.
    ///
    /// This error is returned without checking the checksum or the contents of the remainder
    /// of the block. Encountering this error causes the remainder of the block to be discarded.
    ///
    /// [`Zero`] records used to be produced from Google's `leveldb` back when its writable file
    /// interface was sometimes implemented with memory-mapped files.
    ///
    /// Other forms of corruption may also be able to cause a block to be erroneously filled with
    /// `0`s.
    ZeroRecord,
}

#[derive(Debug)]
pub enum CorruptedTableError<InvalidKey, Decompression> {
    /// A table file was shorter in length than recorded in the database's `MANIFEST`.
    ///
    /// # Data
    /// The expected length of the table file.
    TruncatedTableFile(FileSize),
    /// A table file which, when opened, had expected the file length, was unexpectedly truncated.
    ///
    /// # Data
    /// The expected length of the table file, followed by a block handle which could not be
    /// read due to an early end-of-file.
    SuddenlyTruncatedTableFile(FileSize, BlockHandle),
    /// The last eight bytes of the table file did not match the expected magic value.
    ///
    /// # Data
    /// The last eight bytes of the table file.
    MissingTableMagic([u8; 8]),
    /// The filter block of the table was fewer than 5 bytes in length, therefore lacking the
    /// filter block footer.
    ///
    /// # Data
    /// The block handle of the filter block.
    TruncatedFilterBlock(BlockHandle),
    /// The filter block did not contain a filter for a data block of the table.
    ///
    /// # Data
    /// The block handle of the filter block, the offset into the index block of the data block
    /// entry, and the handle of the data block which had no filter.
    FiltersTooShort(BlockHandle, TableBlockOffset, BlockHandle),
    /// The filter block contained invalid `start` or `end` offsets for a filter.
    ///
    /// Either the `start` or `end` offset went out-of-bounds of the filter data (including the
    /// hypothetical case where the `u32` offset values overflowed a `usize`), or the `end`
    /// offset was strictly less than the `start` offset.
    ///
    /// # Data
    /// The block handle of the filter block, the offset into the filter block of the `start`
    /// filter offset (which is immediately followed in the filter block by the `end` offset),
    /// the value of the `start` offset, and the value of the `end` offset.
    InvalidFilterOffsets(BlockHandle, TableBlockOffset, u32, u32),
    /// A handle to one of the blocks in the table file is corrupted.
    ///
    /// # Data
    /// The type of the block, the offset into the table file of the corrupted block
    /// handle, and the type of corruption, respectively.
    CorruptedBlockHandle(BlockType, FileOffset, BlockHandleCorruption),
    /// One of the data block handles listed in the index block is corrupted.
    ///
    /// # Data
    /// The offset into the index block of the corrupted handle, followed by the type of corruption.
    CorruptedDataBlockHandle(TableBlockOffset, BlockHandleCorruption),
    /// One of the blocks in the table file is corrupted, and could not be decompressed.
    ///
    /// # Data
    /// The type of the block, the handle to the block, and the type of corruption that occurred.
    CorruptedCompressedBlock(BlockType, BlockHandle, CompressedBlockError<Decompression>),
    /// An uncompressed block of the table file is corrupted.
    ///
    /// # Data
    /// The type of the block, the handle to the block, the offset into the block of the
    /// start of the current entry (or `0`) when the corruption occurred, and the type of
    /// corruption.
    ///
    /// (Depending on the type of corruption, the offset of the current entry might be irrelevant.)
    CorruptedBlock(BlockType, BlockHandle, TableBlockOffset, CorruptedBlockError),
    /// An internal key in an uncompressed block of the table file is corrupted. It lacked an
    /// 8-byte suffix.
    ///
    /// # Data
    /// The type of the block, the handle to the block, and the offset into the block of the
    /// start of the entry with a corrupted key.
    TruncatedInternalKey(BlockType, BlockHandle, TableBlockOffset),
    /// An internal key in an uncompressed block of the table file is corrupted. It had an
    /// unknown entry type.
    ///
    /// # Data
    /// The type of the block, the handle to the block, and the offset into the block of the
    /// start of the entry with a corrupted key.
    UnknownEntryType(BlockType, BlockHandle, TableBlockOffset, TableBlockOffset),
    /// The comparator chosen in database settings indicated that a user key in a data block of the
    /// table file was invalid.
    ///
    /// # Data
    /// The handle to the block, the offset into the block of the start of the entry with a
    /// corrupted key, and the [`InvalidKeyError`] returned by the chosen comparator.
    InvalidUserKey(BlockHandle, TableBlockOffset, InvalidKey)
}

pub enum CompressedBlockError<Decompression> {
    /// The expected checksum recorded in a block's footer did not match the actual calculated
    /// checksum of the block.
    ///
    /// # Data
    /// The expected checksum from the block's footer and the computed checksum, respectively.
    ChecksumMismatch(u32, u32),
    /// Attempting to decompress data failed (for some reason other than not supporting the
    /// indicated type of compression or failing to allocate a buffer).
    ///
    /// This might indicate that the incorrect types of compression were chosen in the database
    /// settings to open this database (though, given that data may since have been *written*
    /// to the database with the incorrect compression settings, it's very likely that some
    /// sort of corruption has occurred).
    ///
    /// # Data
    /// The fields indicate the selected decompressor, the compressed data, and the resulting
    /// compression error, respectively.
    Decompression(CompressorId, Vec<u8>, Decompression),
}

#[derive(Debug, Clone, Copy)]
pub enum CorruptedBlockError {
    /// The table block was fewer than 4 bytes long, meaning that its 4-byte footer, which indicates
    /// the number of restarts in the block, is missing.
    MissingNumRestarts,
    /// The table block indicated an impossibly large number of restarts, such that the restarts
    /// and footer would together exceed the size of the block.
    NumRestartsTooLarge,
    /// A restart index was out-of-bounds of the `entries` segment of the table block.
    RestartOutOfBounds,
    /// A varint32 was expected, but the end of the `entries` segment of the table block was
    /// reached.
    ///
    /// This occurs either if the varint32 is entirely missing (as every varint is at least `1`
    /// byte in length) or if a varint32 had its most-significant bit set to indicate that another
    /// byte should be read (and doing so would not exceed the maximum 5 byte length of a varint32),
    /// but the end of the `entries` data was reached.
    TruncatedVarint32,
    /// A varint32 was read that either exceeded 5 bytes in length or would overflow a u32.
    OverflowingVarint32,
    /// The end of a key slice went out-of-bounds of the `entries` segment of the table block.
    TruncatedKey,
    /// The end of a value slice went out-of-bounds of the `entries` segment of the table block.
    TruncatedValue,
}

impl From<Varint32DecodeError> for CorruptedBlockError {
    #[inline]
    fn from(error: Varint32DecodeError) -> Self {
        match error {
            Varint32DecodeError::Truncated   => Self::TruncatedVarint32,
            Varint32DecodeError::Overflowing => Self::OverflowingVarint32,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BlockType {
    Metaindex,
    Filter,
    Index,
    Data,
}

#[derive(Debug, Clone, Copy)]
pub enum BlockHandleCorruption {
    TruncatedOffset,
    OverflowingOffset,
    TruncatedSize,
    OverflowingSize,
    PastEndOfFile,
}

// ================================================================
//  Other miscellanenous errors
// ================================================================

/// The error returned by [`OpenCorruptionHandler::finished_manifest`] or
/// [`OpenCorruptionHandler::finished_all_logs`].
///
/// [`OpenCorruptionHandler::finished_manifest`]:
///     crate::pub_traits::error_handler::OpenCorruptionHandler::finished_manifest
/// [`OpenCorruptionHandler::finished_all_logs`]:
///     crate::pub_traits::error_handler::OpenCorruptionHandler::finished_all_logs
#[derive(Debug, Clone, Copy)]
pub struct FinishError;

/// The errors which may be returned by an [`OpenCorruptionHandler`].
///
/// If a handler ever returned a `BreakError` control flow variant or a [`FinishError`] but does
/// not report a [`HandlerError`], then a [`HandlerReportedError`] is reported as the cause
/// of the database failing to open.
///
/// [`HandlerReportedError`]: CorruptionError::HandlerReportedError
/// [`OpenCorruptionHandler`]: crate::pub_traits::error_handler::OpenCorruptionHandler
#[derive(Debug, Clone, Copy)]
pub enum HandlerError {
    ManifestFile(FileOffset, BinaryBlockLogCorruptionError),
    VersionEdit(LogicalRecordOffset, VersionEditDecodeError),
    LogFile(FileNumber, FileOffset, BinaryBlockLogCorruptionError),
    WriteBatch(FileNumber, LogicalRecordOffset, WriteBatchDecodeError),
}

#[derive(Debug)]
pub(crate) struct BinaryBlockLogReadError {
    pub error:  IoError,
    pub offset: FileOffset,
}

#[derive(Debug, Clone, Copy)]
pub enum WriteBatchValidationError {
    /// A varint32 was expected, but the end of input was reached.
    ///
    /// This occurs either if the varint32 is entirely missing (as every varint is at least `1`
    /// byte in length) or if a varint32 had its most-significant bit set to indicate that another
    /// byte should be read (and doing so would not exceed the maximum 5 byte length of a varint32),
    /// but the end of the input was reached.
    TruncatedVarint32,
    /// A varint32 was read that either exceeded 5 bytes in length or would overflow a u32.
    OverflowingVarint32,
    /// A length-prefixed byte slice was expected, and although its length was successfully read,
    /// the remaining input is shorter than the slice's length.
    TruncatedSlice,
    /// A key slice had a length strictly greater than `u32::MAX - 8`. All user keys are required to
    /// have length at most `u32::MAX - 8`, to ensure that an 8-byte internal key suffix can be
    /// added.
    KeyTooLong,
    /// The byte of a write batch entry indicating its [`EntryType`] had an unknown value.
    ///
    /// # Data
    /// The unknown entry type.
    UnknownEntryType(u8),
    /// The write batch contained more entries than indicated in its header.
    TooManyEntries,
    /// The write batch contained fewer entries than indicated in its header.
    TooFewEntries,
}

impl WriteBatchValidationError {
    #[must_use]
    pub(crate) const fn from_prefixed_bytes_err(error: PrefixedBytesParseError) -> Self {
        match error {
            PrefixedBytesParseError::TruncatedVarint32   => Self::TruncatedVarint32,
            PrefixedBytesParseError::OverflowingVarint32 => Self::OverflowingVarint32,
            PrefixedBytesParseError::TruncatedSlice      => Self::TruncatedSlice,
        }
    }
}

impl From<Varint32DecodeError> for WriteBatchValidationError {
    #[inline]
    fn from(error: Varint32DecodeError) -> Self {
        match error {
            Varint32DecodeError::Truncated   => Self::TruncatedVarint32,
            Varint32DecodeError::Overflowing => Self::OverflowingVarint32,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum WriteBatchPutError {
    /// The write batch already contained the maximum number of entries, [`u32::MAX`].
    MaxEntries,
    /// The key slice had a length strictly greater than `u32::MAX - 8`. All user keys are required
    /// to have length at most `u32::MAX - 8`, to ensure that an 8-byte internal key suffix can be
    /// added.
    KeyTooLong,
    /// The value slice had a length strictly greater than `u32::MAX`.
    ValueTooLong,
}

#[derive(Debug, Clone, Copy)]
pub enum WriteBatchDeleteError {
    /// The write batch already contained the maximum number of entries, [`u32::MAX`].
    MaxEntries,
    /// The key slice had a length strictly greater than `u32::MAX - 8`. All user keys are required
    /// to have length at most `u32::MAX - 8`, to ensure that an 8-byte internal key suffix can be
    /// added.
    KeyTooLong,
}

/// The total number of entries in the two write batches (the destination and the one being pushed)
/// exceeds [`u32::MAX`], the maximum number of entries in a single write batch.
#[derive(Debug, Clone, Copy)]
pub struct PushBatchError;

#[derive(Debug, Clone, Copy)]
pub enum PrefixedBytesParseError {
    /// The varint32 prefix of a length-prefixed byte slice was truncated.
    ///
    /// This occurs either if the input ends without yielding even a single byte of data (as
    /// every varint is at least `1` byte in length) or if the varint32 had its most-significant bit
    /// set to indicate that another byte should be read (and doing so would not exceed the maximum
    /// 5 byte length of a varint32), but the end of the input was reached.
    TruncatedVarint32,
    /// The varint32 prefix either exceeded 5 bytes in length or would overflow a u32.
    OverflowingVarint32,
    /// Although the length of a length-prefixed byte slice was successfully read, the remaining
    /// input is shorter than the slice's length.
    TruncatedSlice,
}

impl From<Varint32DecodeError> for PrefixedBytesParseError {
    #[inline]
    fn from(error: Varint32DecodeError) -> Self {
        match error {
            Varint32DecodeError::Truncated   => Self::TruncatedVarint32,
            Varint32DecodeError::Overflowing => Self::OverflowingVarint32,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Varint32DecodeError {
    Truncated,
    Overflowing,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Varint64DecodeError {
    Truncated,
    Overflowing,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OutOfFileNumbers;

#[derive(Debug, Clone, Copy)]
pub(crate) struct OutOfSequenceNumbers;

#[derive(Debug, Clone)]
pub(crate) enum InvalidInternalKey<InvalidKey> {
    /// The slice was greater than `u32::MAX` bytes in length.
    TooLong,
    /// All internal keys have an 8-byte suffix, but the slice was fewer than 8 bytes in length.
    Truncated,
    /// The byte of an internal key indicating its [`EntryType`] had an unknown value.
    ///
    /// # Data
    /// The unknown entry type.
    UnknownEntryType(u8),
    /// The comparator chosen in database settings indicated that a key in a table block was
    /// invalid.
    ///
    /// # Data
    /// The contents of that user key (that is, excluding the 8-byte suffix of internal keys)
    /// and the [`InvalidKeyError`] returned by the chosen comparator.
    InvalidUserKey(Box<[u8]>, InvalidKey),
}

#[derive(Debug)]
pub(crate) enum BlockSeekError<E> {
    Block(CorruptedBlockError),
    Cmp(E),
}

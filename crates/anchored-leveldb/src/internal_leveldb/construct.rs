use std::{thread, sync::mpsc};
use std::{borrow::Cow, collections::HashSet, io::Error as IoError};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
};

use clone_behavior::FastMirroredClone;

use anchored_vfs::{
    CreateParentDir, FSLockError as _, IntoChildFileIterator as _, LevelDBFilesystem, SyncParentDir,
    WritableFile,
};

use crate::{
    compaction::flush_memtable,
    internal_logger::InternalLogger,
    memtable::UniqueMemtable,
    snapshot::SnapshotList,
    table_file::TableFileBuilder,
};
use crate::{
    all_errors::{
        aliases::{RecoveryErrorAlias, RecoveryErrorKindAlias},
        types::{
            CorruptionError, FilesystemError, InitEmptyDatabaseError, OpenError, OpenFsError,
            RecoveryErrorKind, WriteError, WriteFsError,
        },
    },
    binary_block_log::{BinaryBlockLogReaderBuffers, LogRecordResult, Slices, WriteLogWriter},
    contention_queue::{ContentionQueue, PanicOptions},
    database_files::{LevelDBFileName, set_current},
    options::{
        AtomicDynamicOptions, DynamicOptions, InternalCompactionOptions, InternallyMutableOptions,
        InternalOpenOptions, InternalOptions,
        pub_options::{ClampOptions, OpenOptions},
    },
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        error_handler::{FinishedLogControlFlow, LogControlFlow, OpenCorruptionHandler},
        pool::BufferPool,
    },
    pub_typed_bytes::{BinaryLogBlockSize, FileNumber, FileSize, SequenceNumber, ShortSlice},
    table_caches::{BlockCache, TableCache},
    table_format::{InternalComparator, InternalFilterPolicy},
    typed_bytes::{AtomicCloseStatus, CloseStatus, ContinueReadingLogs, NextFileNumber},
    version::{BeginVersionSetRecovery, VersionEdit, VersionSet, VersionSetBuilder},
};
use super::state::{
    BackgroundCompactorHandle, CompactionState, ForegroundCompactor, FrontWriterState,
    InternalDBState, PerHandleState, SharedMutableState,
};


#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + Send,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
    // TODO: Loosen `Send + Sync` requirements
    Self:               Send + Sync + 'static,
    FS::WriteFile:      Send,
    Codecs::Encoders:   Send,
    Pool::PooledBuffer: Send,
{
    /// Open an existing database or create a new one, depending on settings.
    ///
    /// After the database is successfully opened, garbage collection should be performed on
    /// the database folder's files and a background compaction (if enabled) may need to be
    /// performed. This function does not perform those two steps.
    //
    // TODO: it probably should perform those two steps.
    pub fn open(
        options: OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
    ) -> Result<
        (Arc<Self>, PerHandleState<Codecs::Decoders>),
        RecoveryErrorAlias<FS, Cmp, Codecs>,
    > {
        todo!()
    }

    /// The function executes several steps that need to happen early on in the opening process.
    ///
    /// - Determine whether the database already exists or not (and may return an error based on
    ///   `create_if_missing` and `error_if_exists`).
    ///   - Execute several checks to improve the accuracy of this judgement.
    /// - Acquire the lockfile (and if the database should be opened, the lockfile is created if
    ///   it does not already exist).
    /// - If we are creating a new database, create the database directory and initialize it to
    ///   an empty database.
    /// - Execute `clamp_options`.
    fn begin_open(
        options: &mut OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
    ) -> Result<BeginOpen<FS::Lockfile, FS::WriteFile>, RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        todo!()
    }

    /// Determine whether a database already exists in the database directory.
    ///
    /// On success, the existing lockfile (if any) is returned, and the returned boolean is
    /// `true` iff the database definitely exists.
    fn definitely_exists_or_not(
        options:      &mut OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
        lock_path:    &Path,
        current_path: &Path,
    ) -> Result<
        DefinitelyExistsOrNot<FS::Lockfile>,
        RecoveryErrorKindAlias<FS, Cmp, Codecs>,
    > {
        todo!()
    }

    /// A database appears not to exist in the database directory.
    ///
    /// To increase confidence that the database does not exist, confirm that no `MANIFEST-` files
    /// exist in that directory. Return an error if any exist.
    fn confirm_does_not_exist(
        filesystem:   &mut FS,
        db_directory: &Path,
    ) -> Result<(), RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        todo!()
    }

    /// Create a new LevelDB database, assuming that no database currently exists in
    /// `db_directory`. (If there was one, it may be overwritten.)
    ///
    /// To create an empty `InternalDBState` struct, the rest of the normal recovery process can
    /// then be performed on the just-written database.
    fn init_empty_database(
        filesystem:   &FS,
        db_directory: &Path,
        cmp_name:     ShortSlice<'static>,
        block_size:   BinaryLogBlockSize,
    ) -> Result<(), OpenError<FS::Error>> {
        todo!()
    }

    /// Create a `DBBuilder` which manages the recovery of an existing database.
    fn begin_recovery(
        options:      OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
        lockfile:     FS::Lockfile,
        logger:       InternalLogger<FS::WriteFile>,
        current_path: PathBuf,
    ) -> DBBuilder<FS, Cmp, Policy, Codecs, Pool> {
        todo!()
    }

    /// Set up the background compaction thread (if necessary) and rearrange state into its
    /// final form.
    fn build(
        builder:   DBBuilder<FS, Cmp, Policy, Codecs, Pool>,
        recovered: RecoveredDB<FS::WriteFile>,
    ) -> (Arc<Self>, PerHandleState<Codecs::Decoders>) {
        todo!()
    }
}

// TODO: implement Debug
struct DBBuilder<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    opts:                    InternalOptions<Cmp, Policy, Codecs>,
    mut_opts:                InternallyMutableOptions<FS, Policy, Pool>,
    open_corruption_handler: Box<dyn OpenCorruptionHandler<Cmp::InvalidKeyError>>,
    open_opts:               InternalOpenOptions,
    lockfile:                FS::Lockfile,
    current_path:            PathBuf,
    memtable:                UniqueMemtable<Cmp>,
    /// This is filled only on the last cal to `self.recover_log_file(..)`, if ever.
    ///
    /// Also, `self.memtable` is only ever written to inside `self.recover_log_file(..)`, and
    /// it is reset if and only if the log is *not* reused.
    reused_log:              Option<ReusedLog<FS::WriteFile>>,
    table_builder:           TableFileBuilder<FS::WriteFile, Policy, Pool>,
    encoders:                Codecs::Encoders,
    decoders:                Codecs::Decoders,
}

impl<FS, Cmp, Policy, Codecs, Pool> DBBuilder<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Rearrange the state in `options` and `begin_open` into a shape more suitable for database
    /// recovery.
    ///
    /// `clamp_options` should already have been executed.
    pub(self) fn new(
        options:    OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
        begin_open: BeginOpen<FS::Lockfile, FS::WriteFile>,
    ) -> Self {
        todo!()
    }

    /// Recover a LevelDB database which is thought to exist.
    ///
    /// The `LOCK` file must have been acquired and `CURRENT` should exist, though might
    /// not be a file.
    pub(self) fn recover(
        &mut self,
    ) -> Result<RecoveredDB<FS::WriteFile>, RecoveryErrorAlias<FS, Cmp, Codecs>> {
        todo!()
    }

    /// Recover the current `MANIFEST` file and all write-ahead `.log` files.
    ///
    /// # `manifest_file_number` Outpointer
    /// If the manifest file number cannot be read, the provided outpointer is left unchanged.
    fn begin_recovery(
        &mut self,
        manifest_file_number_out: &mut FileNumber,
    ) -> Result<
        VersionSetBuilder<FS::WriteFile, true>,
        RecoveryErrorKindAlias<FS, Cmp, Codecs>,
    > {
        todo!()
    }

    /// Look through every file in the db directory to ensure that all expected table files are
    /// present and determine which `.log` files to recover.
    ///
    /// Returns an unsorted list of all `.log` files which should be recovered.
    fn enumerate_files(
        &mut self,
        vset_builder: &mut VersionSetBuilder<FS::WriteFile, false>,
    ) -> Result<
        Vec<(FileNumber, FileSize)>,
        RecoveryErrorKindAlias<FS, Cmp, Codecs>,
    > {
        todo!()
    }

    /// Recover one write-ahead `.log` file.
    fn recover_log_file(
        &mut self,
        vset_builder:         &mut VersionSetBuilder<FS::WriteFile, true>,
        manifest_file_number: FileNumber,
        log_buffers:          &mut BinaryBlockLogReaderBuffers,
        log_number:           FileNumber,
        log_file_size:        FileSize,
        last_log:             bool,
    ) -> Result<ContinueReadingLogs, RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        todo!()
    }

    /// Flush a memtable to a level-0 table file.
    ///
    /// Note that the persisted `MANIFEST` is not updated, so this operation cannot result in
    /// immediate corruption (...though it could lead to corruption if someone uses Google's
    /// less-than-perfect LevelDB recovery tool...), and we can perform consistency checks on the
    /// recovered version set later.
    fn flush_memtable(
        &mut self,
        vset_builder:         &mut VersionSetBuilder<FS::WriteFile, true>,
        manifest_file_number: FileNumber,
    ) -> Result<(), RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        todo!()
    }

    /// Try to reuse a write-ahead `.log` file.
    ///
    /// Reuse needs to be enabled by the user (via the `OpenCorruptionHandler` setting) and must not
    /// result in corruption (as determined in part by the `OpenCorruptionHandler`, and in part
    /// by whether the memtable was flushed). Only the most-recent log file (if any) may be reused,
    /// and for database consistency, the most-recent log file must exactly correspond to the
    /// most-recent memtable (implying that, if a memtable flush occurred, the corresponding log
    /// cannot be reused).
    ///
    /// For the sake of performance, the log is reused only if the virtual filesystem efficiently
    /// supports appending to existing files and if the log is not too large.
    #[expect(
        clippy::fn_params_excessive_bools,
        reason = "cleaner to do all the checks in one place",
    )]
    fn try_reuse_log(
        &self,
        last_log:         bool,
        flushed_memtable: bool,
        reuse_permitted:  bool,
        log_file_path:    &Path,
    ) -> Option<WriteLogWriter<FS::WriteFile>> {
        todo!()
    }

    /// Performs many actions to finalize recovery, including possible updates to persistent state.
    ///
    /// - Create a new write-ahead `.log` file (if one was not reused).
    /// - Perform error checks on the recovered version (if requested).
    /// - Update the `MANIFEST` file (if necessary).
    fn finish_recovery(
        &mut self,
        mut vset_builder: VersionSetBuilder<FS::WriteFile, true>,
    ) -> Result<
        RecoveredDB<FS::WriteFile>,
        RecoveryErrorKindAlias<FS, Cmp, Codecs>,
    > {
        todo!()
    }
}

/// Returned by [`InternalDBState::begin_open`].
#[derive(Debug)]
struct BeginOpen<Lockfile, WriteFile> {
    lockfile: Lockfile,
    logger:   InternalLogger<WriteFile>,
    current:  PathBuf,
}

/// Returned by [`InternalDBState::definitely_exists_or_not`].
#[derive(Debug)]
struct DefinitelyExistsOrNot<Lockfile> {
    lockfile: Option<Lockfile>,
    /// `true` iff (with high confidence) the database exists, `false` iff (with high confidence)
    /// the database does not exist.
    exists:   bool,
}

/// A reused write-ahead `.log` file.
struct ReusedLog<File> {
    log:        WriteLogWriter<File>,
    log_number: FileNumber,
}

impl<File> Debug for ReusedLog<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ReusedLog")
            .field("log",        &self.log)
            .field("log_number", &self.log_number)
            .finish()
    }
}

/// Returned by [`DBBuilder::recover`].
#[derive(Debug)]
struct RecoveredDB<Writefile> {
    version_set:       VersionSet<Writefile>,
    current_write_log: WriteLogWriter<Writefile>,
    next_file_number:  NextFileNumber,
}

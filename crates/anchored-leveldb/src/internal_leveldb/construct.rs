use std::{thread, sync::mpsc};
use std::{borrow::Cow, collections::HashSet, io::Error as IoError};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, mpsc::{Receiver, SyncSender}, Mutex},
};

use clone_behavior::FastMirroredClone;
use tracing::level_filters::LevelFilter;

use anchored_vfs::{
    CreateParentDir, FSLockError as _, IntoChildFileIterator as _, LevelDBFilesystem, SyncParentDir,
    WritableFile,
};

use crate::{
    internal_logger::InternalLogger,
    memtable::UniqueMemtable,
    snapshot::SnapshotList,
    table_file::TableFileBuilder,
    typed_bytes::ContinueReadingLogs,
};
use crate::{
    all_errors::{
        aliases::{RecoveryErrorAlias, RecoveryErrorKindAlias},
        types::{
            CorruptionError, FilesystemError, FinishError, InitEmptyDatabaseError, OpenError,
            OpenFsError, OutOfFileNumbers, OutOfSequenceNumbers, RecoveryError, RecoveryErrorKind,
            RwErrorKind, WriteBatchDecodeError, WriteError, WriteFsError,
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
        logger::TracingLogger,
        pool::BufferPool,
    },
    pub_typed_bytes::{
        BinaryLogBlockSize, CloseStatus, FileNumber, FileOffset, FileSize, LogicalRecordOffset,
        SequenceNumber, ShortSlice,
    },
    table_caches::{BlockCache, TableCache},
    table_format::{InternalComparator, InternalFilterPolicy},
    version::{BeginVersionSetRecovery, VersionEdit, VersionSet, VersionSetBuilder},
    write_batch::{BorrowedWriteBatch, ChainedWriteBatchIter},
};
use super::state::{
    BackgroundCompactor, CompactionState, ForegroundCompactor, FrontWriterState, InternalDBState,
    ManualCompaction, PerHandleState, SharedMutableState,
};


#[derive(Debug)]
pub(crate) struct OpenFinisher<S> {
    db_state: Arc<S>,
    channels: Option<(SyncSender<Arc<S>>, Receiver<()>)>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> OpenFinisher<InternalDBState<FS, Cmp, Policy, Codecs, Pool>>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator + FastMirroredClone,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone + Send,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
    // TODO: Loosen `Send + Sync` requirements
    InternalDBState<FS, Cmp, Policy, Codecs, Pool>: Send + Sync + 'static,
    // FS::WriteFile:      Send,
    // Codecs::Encoders:   Send,
    // Codecs::Decoders:   Send,
    // Pool::PooledBuffer: Send,
{
    pub fn finish_open(self, decoders: &mut Codecs::Decoders) {
        #[expect(
            clippy::expect_used,
            reason = "there's no reason this should ever panic, so better to loudly error \
                      instead of silently deadlocking (when no compactions happen)",
        )]
        if let Some((sender, ready_receiver)) = self.channels {
            sender.send(Arc::clone(&self.db_state)).expect("Background compaction thread failed");

            ready_receiver.recv().expect("Background compaction thread failed");
        }

        // Garbage collect files (the recovery process may have obviated the need for some
        // previously existing files, or a previous database invocation may have crashed, and
        // any incomplete files can be discarded).
        let mut mut_state = self.db_state.lock_mutable_state();
        mut_state = self.db_state.garbage_collect_files(mut_state);
        // Maybe start a compaction (we do this whenever setting the current `Version`; the
        // recovery process does exactly that).
        let _drop = self.db_state.maybe_start_compaction(mut_state, decoders);
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator + FastMirroredClone,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone + Send,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
    // TODO: Loosen `Send + Sync` requirements
    Self:               Send + Sync + 'static,
    FS::WriteFile:      Send,
    Codecs::Encoders:   Send,
    Codecs::Decoders:   Send,
    Pool::PooledBuffer: Send,
{
    /// Open an existing database or create a new one, depending on settings.
    ///
    /// # Correctness
    /// [`Self::close_owned`] must be called exactly once on the returned `Arc<Self>`.
    ///
    /// The [`DB`]/[`DBState`] refcount of the returned `InternalDBState` is initialized to `1`;
    /// this is a lie. `OpenFinisher::finish_open` should be called on the returned finisher
    /// once the `Arc<Self>` is actually placed into a [`DB`] or [`DBState`].
    ///
    /// See [`DB`] and [`DBState`] for more about how the reference counts are handled.
    ///
    /// [`DB`]: crate::pub_leveldb::DB
    /// [`DBState`]: crate::pub_leveldb::DBState
    #[expect(clippy::type_complexity, reason = "reasonably flat structure")]
    pub fn open(
        mut options: OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
    ) -> Result<
        (Arc<Self>, OpenFinisher<Self>, PerHandleState<Codecs::Decoders>),
        RecoveryErrorAlias<FS, Cmp, Codecs>,
    > {
        let begin_open = match Self::begin_open(&mut options) {
            Ok(begin_open) => begin_open,
            Err(kind)       => return Err(RecoveryError {
                db_directory: options.database_directory,
                kind,
            }),
        };

        let mut builder = DBBuilder::new(options, begin_open);

        let recovered = match builder.recover() {
            Ok(recovered) => recovered,
            Err(mut kind) => {
                if let Some(other_err) = builder.open_corruption_handler.get_error() {
                    kind.merge_worst_error(RecoveryErrorKind::Corruption(
                        other_err.into_corruption_error(builder.manifest_file_number),
                    ));
                }

                return Err(RecoveryError {
                    db_directory: builder.opts.db_directory,
                    kind,
                });
            }
        };

        Self::build(builder, recovered)
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
    /// - Optionally create a `LOG` file.
    #[expect(clippy::type_complexity, reason = "only complex because of generics, but very flat")]
    fn begin_open(
        options: &mut OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
    ) -> Result<BeginOpen<FS::Lockfile, FS::WriteFile>, RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        let lock_path = LevelDBFileName::Lockfile.file_path(&options.database_directory);
        let current_path = LevelDBFileName::Current.file_path(&options.database_directory);

        let DefinitelyExistsOrNot { lockfile, exists } = Self::definitely_exists_or_not(
            options,
            &lock_path,
            &current_path,
        )?;

        #[expect(clippy::collapsible_else_if, reason = "make tree more obvious")]
        let lockfile = if exists {
            if options.error_if_exists {
                return Err(RecoveryErrorKind::Open(OpenError::DatabaseExists));
            } else {
                if let Some(lockfile) = lockfile {
                    lockfile
                } else {
                    // Attempt to acquire the lockfile.
                    options.filesystem
                        .create_and_lock(&lock_path, CreateParentDir::False, SyncParentDir::False)
                        .map_err(|lock_err| {
                            if lock_err.is_already_locked() {
                                RecoveryErrorKind::Open(OpenError::DatabaseLocked)
                            } else {
                                RecoveryErrorKind::Open(OpenError::Filesystem(
                                    FilesystemError::FsError(lock_err.into()),
                                    OpenFsError::AcquireLockfile,
                                ))
                            }
                        })?
                }
            }
        } else {
            if options.create_if_missing {
                let lockfile = if let Some(lockfile) = lockfile {
                    lockfile
                } else {
                    options.filesystem
                        .create_and_lock(
                            &lock_path,
                            CreateParentDir::True,
                            SyncParentDir::True,
                        )
                        .map_err(|lock_err| {
                            if lock_err.is_already_locked() {
                                RecoveryErrorKind::Open(OpenError::DatabaseLocked)
                            } else {
                                RecoveryErrorKind::Open(OpenError::Filesystem(
                                    FilesystemError::FsError(lock_err.into()),
                                    OpenFsError::CreateLockfile,
                                ))
                            }
                        })?
                };

                Self::init_empty_database(
                    &options.filesystem,
                    &options.database_directory,
                    options.format.comparator().name(),
                    options.format.binary_log_block_size(),
                ).map_err(RecoveryErrorKind::Open)?;

                lockfile
            } else {
                return Err(RecoveryErrorKind::Open(OpenError::DatabaseDoesNotExist));
            }
        };

        // Enforce a maximum of `u32::MAX/2`.
        #[expect(clippy::integer_division, reason = "taking the floor is intentional")]
        {
            options.seek_compaction.iter_sample_period = options.seek_compaction.iter_sample_period
            .min(u32::MAX/2);
        };

        match options.clamp_options {
            ClampOptions::NoClamping => {},
            ClampOptions::BackwardsCompatibilityClamping => {
                for max_file_size in &mut options.sstable.max_sstable_sizes {
                    max_file_size.0 = max_file_size.0.clamp(1 << 20_u8, 1 << 30_u8);
                }
                options.sstable.sstable_block_size = options.sstable.sstable_block_size
                    .clamp(1 << 10_u8, 4 << 20_u8);

                options.cache.table_cache_capacity = options.cache.table_cache_capacity
                    .clamp(54, 49_990);
            }
        }

        let infolog_file = if matches!(options.logger.log_file_filter, LevelFilter::OFF) {
            None
        } else {
            Self::create_log_file(&options.filesystem, &options.database_directory)
        };

        let logger = InternalLogger::new(
            infolog_file,
            options.logger.log_file_filter,
            options.logger.custom_logger.take().unwrap_or_else(|| Box::new(TracingLogger)),
            options.logger.logger_filter,
        );

        Ok(BeginOpen {
            lockfile,
            logger,
            current: current_path,
        })
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
        let lockfile = options.filesystem.open_and_lock(lock_path);
        let lockfile_definitely_does_not_exist = if let Err(err) = &lockfile {
            if err.is_already_locked() {
                return Err(RecoveryErrorKind::Open(OpenError::DatabaseLocked));
            } else {
                err.is_not_found()
            }
        } else {
            false
        };
        let lockfile = lockfile.ok();

        match options.filesystem.file_exists(current_path) {
            // Continue.
            Ok(true) => Ok(DefinitelyExistsOrNot {
                lockfile,
                exists: true,
            }),
            Ok(false) => {
                if !lockfile_definitely_does_not_exist {
                    // The LOCK file might exist, even though the CURRENT file doesn't. Do
                    // a more extensive check for whether a database should exist here.
                    Self::confirm_does_not_exist(
                        &options.filesystem,
                        &options.database_directory,
                    )?;
                }
                // We've confirmed that the directory does not seem to contain a database.
                Ok(DefinitelyExistsOrNot {
                    lockfile,
                    exists: false,
                })
            }
            Err(fs_err) => Err(RecoveryErrorKind::Open(OpenError::Filesystem(
                FilesystemError::FsError(fs_err),
                OpenFsError::UnknownExistence,
            ))),
        }
    }

    /// A database appears not to exist in the database directory.
    ///
    /// To increase confidence that the database does not exist, confirm that no `MANIFEST-` files
    /// exist in that directory. Return an error if any exist.
    fn confirm_does_not_exist(
        filesystem:   &FS,
        db_directory: &Path,
    ) -> Result<(), RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        let files = filesystem.child_files(db_directory)
            .map_err(|fs_err| RecoveryErrorKind::Open(
                OpenError::DatabaseProbablyDoesNotExist(
                    FilesystemError::FsError(fs_err),
                ),
            ))?;

        for file in files.child_files() {
            let (file, _) = file.map_err(|dir_err| RecoveryErrorKind::Open(
                OpenError::DatabaseProbablyDoesNotExist(
                    FilesystemError::FsError(dir_err.into()),
                ),
            ))?;

            if file.starts_with("MANIFEST-") {
                // It looks like this directory should hold a database, even though
                // `CURRENT` is missing. Return an error.
                return Err(RecoveryErrorKind::Corruption(
                    CorruptionError::MissingCurrent,
                ));
            }
        }

        Ok(())
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
        fn try_scope<File: WritableFile>(
            new_db:        &VersionEdit,
            manifest_file: File,
            block_size:    BinaryLogBlockSize,
        ) -> Result<(), (IoError, InitEmptyDatabaseError)> {
            let mut manifest_writer = WriteLogWriter::new_empty(manifest_file, block_size);
            let mut new_db_record = Vec::new();
            new_db.encode(&mut new_db_record);
            manifest_writer.add_record(Slices::new_single(&new_db_record))
                .map_err(|io_err| (io_err, InitEmptyDatabaseError::WriteManifest))?;
            manifest_writer.sync_log_data()
                .map_err(|io_err| (io_err, InitEmptyDatabaseError::SyncManifest))?;
            Ok(())
        }

        let mut new_db = VersionEdit::new_empty();
        new_db.comparator_name  = Some(Cow::Borrowed(cmp_name.inner()));
        // No file is actually created with file number `0`. And during the recovery process,
        // there will be no previous `.log` file to reuse, so a new `.log` file will be created;
        // that new file will have a file number of at least `2` (from `new_db.next_file_number`).
        new_db.log_number       = Some(FileNumber(0));
        let manifest_number     = FileNumber(1);
        new_db.next_file_number = Some(FileNumber(2));
        new_db.last_sequence    = Some(SequenceNumber::ZERO);

        let manifest = LevelDBFileName::Manifest { file_number: manifest_number };
        let manifest_path = manifest.file_path(db_directory);
        let manifest_file = filesystem
            .open_writable(&manifest_path, CreateParentDir::False, SyncParentDir::False)
            .map_err(|fs_err| OpenError::Filesystem(
                FilesystemError::FsError(fs_err),
                OpenFsError::InitEmptyDatabase(InitEmptyDatabaseError::OpenManifest),
            ))?;

        try_scope(&new_db, manifest_file, block_size)
            .inspect_err(|_| {
                // Try to clean up the now-pointless manifest file. No worries if that fails,
                // the next time that file is opened, it'll be with `open_writable` not
                // `open_appendable`, so no corruption can occur.
                // Also, any leftover file will eventually be garbage-collected.
                let _err = filesystem.remove_file(&manifest_path);
            })
            .map_err(|(io_err, init_err)| OpenError::Filesystem(
                FilesystemError::Io(io_err),
                OpenFsError::InitEmptyDatabase(init_err),
            ))?;

        set_current(filesystem, db_directory, manifest_number, &manifest.file_name())
            .map_err(|(fs_err, current_err)| OpenError::Filesystem(
                fs_err,
                OpenFsError::InitEmptyDatabase(InitEmptyDatabaseError::SetCurrent(current_err)),
            ))?;

        Ok(())
    }

    /// Attempt to create a `LOG` file, returning `None` on error.
    fn create_log_file(
        filesystem:   &FS,
        db_directory: &Path,
    ) -> Option<FS::WriteFile> {
        let infolog_path = LevelDBFileName::InfoLog.file_path(db_directory);
        let old_infolog_path = LevelDBFileName::OldInfoLog.file_path(db_directory);

        // Does not matter whether this succeeds or fails. Perhaps there isn't an existing `LOG`
        // file.
        #[expect(
            let_underscore_drop,
            clippy::let_underscore_must_use,
            reason = "err doesn't matter",
        )]
        let _: Result<(), _> = filesystem.rename(
            &infolog_path,
            &old_infolog_path,
            SyncParentDir::False
        );

        filesystem
            .open_writable(&infolog_path, CreateParentDir::False, SyncParentDir::False)
            .inspect_err(|_err| {
                // TODO: log error
            })
            .ok()
    }

    /// Set up the background compaction thread (if necessary) and rearrange state into its
    /// final form.
    ///
    /// If the `open_corruption_handler` reports an error, it is returned.
    ///
    /// Does *not* garbage collect old files or start a compaction, which still needs to be done.
    #[expect(
        clippy::too_many_lines,
        reason = "feels more understandable to initialize the remaining state in one place",
    )]
    #[expect(clippy::type_complexity, reason = "only complex because of generics, but very flat")]
    fn build(
        builder:   DBBuilder<FS, Cmp, Policy, Codecs, Pool>,
        recovered: RecoveredDB<FS::WriteFile>,
    ) -> Result<
        (Arc<Self>, OpenFinisher<Self>, PerHandleState<Codecs::Decoders>),
        RecoveryErrorAlias<FS, Cmp, Codecs>,
    > {
        let DBBuilder {
            opts,
            mut_opts,
            open_corruption_handler,
            open_opts,
            lockfile,
            current_path: _unneeded,
            manifest_file_number,
            memtable,
            // We call `builder.recover` to obtain `recovered`, so `reused_log` is `None`.
            reused_log: _already_consumed,
            table_builder,
            encoders,
            decoders,
        } = builder;

        let RecoveredDB {
            version_set,
            current_write_log,
        } = recovered;

        if let Some(err) = open_corruption_handler.get_error() {
            return Err(RecoveryError {
                db_directory: opts.db_directory,
                kind:         RecoveryErrorKind::Corruption(
                    err.into_corruption_error(manifest_file_number),
                ),
            });
        }

        let memtable_writer = memtable.into_memtable(
            opts.unwrap_poison,
            open_opts.memtable_pool_size,
        );
        let current_memtable = memtable_writer.reader();

        let (background, channels, foreground) = if open_opts.compact_in_background {
            let (sender, receiver) = mpsc::sync_channel::<Arc<Self>>(0);
            let (ready_sender, ready_receiver) = mpsc::sync_channel(0);

            let background_decoders = opts.codecs.init_decoders();

            thread::spawn(move || {
                let Ok(shared_state) = receiver.recv() else { return };
                drop(receiver);

                shared_state.background_compaction(
                    table_builder,
                    encoders,
                    background_decoders,
                    ready_sender,
                );
            });

            let background = BackgroundCompactor {
                start_compaction: Condvar::new(),
            };

            (Some(background), Some((sender, ready_receiver)), None)
        } else {
            let foreground = ForegroundCompactor {
                table_builder,
                encoders,
            };
            (None, None, Some(foreground))
        };

        let compaction_state = CompactionState {
            has_ongoing_compaction:     false,
            suspending_compactions:     false,
            memtable_under_compaction:  None,
            pending_compaction_outputs: HashSet::new(),
            manual_compaction:          ManualCompaction {
                level:       None,
                lower_bound: None,
                upper_bound: None,
            },
            manual_compaction_counter: 0,
        };

        let mutable_state = SharedMutableState {
            lockfile:                     Some(lockfile),
            lockfile_refcount:            0,
            compactor_lockfile_refcounts: 0,
            non_compactor_arc_refcounts:  1,
            write_status:                 Ok(()),
            close_status:                 CloseStatus::Open,
            version_set,
            current_memtable,
            iter_read_sample_seed:        0,
            foreground_compactor:         foreground,
            compaction_state,
        };
        let contention_queue = ContentionQueue::new_with_options(
            FrontWriterState {
                memtable_writer,
                current_write_log,
            },
            PanicOptions {
                unwrap_mutex_poison: opts.unwrap_poison,
                unwrap_queue_poison: opts.unwrap_poison,
            },
        );

        let this = Arc::new(Self {
            opts,
            mut_opts,
            mutable_state:        Mutex::new(mutable_state),
            compaction_finished:  Condvar::new(),
            resume_compactions:   Condvar::new(),
            background_compactor: background,
            contention_queue,
            snapshot_list:        SnapshotList::new(),
        });

        let per_handle = PerHandleState {
            decoders,
            iter_key_buf: Vec::new(),
        };

        // Technically, we bump the `Arc` reference count slightly more than strictly necessary.
        // I think it's worth it for encapsulation.
        let finisher = OpenFinisher {
            db_state: Arc::clone(&this),
            channels,
        };

        Ok((this, finisher, per_handle))
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
    open_corruption_handler: Box<dyn OpenCorruptionHandler<Cmp::InvalidKeyError> + Send + Sync>,
    open_opts:               InternalOpenOptions,
    lockfile:                FS::Lockfile,
    current_path:            PathBuf,
    /// Only guaranteed to be accurate on successful recovery.
    manifest_file_number:    FileNumber,
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
    Cmp:    LevelDBComparator + FastMirroredClone,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Rearrange the state in `options` and `begin_open` into a shape more suitable for the
    /// recovery of an existing database.
    ///
    /// `clamp_options` should already have been executed.
    pub(self) fn new(
        options:    OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
        begin_open: BeginOpen<FS::Lockfile, FS::WriteFile>,
    ) -> Self {
        let BeginOpen {
            lockfile,
            logger,
            current: current_path,
        } = begin_open;

        let compaction = InternalCompactionOptions {
            max_level_for_memtable_flush: options.compaction.max_level_for_memtable_flush,
            max_compaction_inputs:        options.compaction.max_compaction_inputs,
            max_grandparent_overlap:      options.compaction.max_grandparent_overlap,
            size_compactions:             options.size_compaction,
            seek_compactions:             options.seek_compaction,
        };

        let (cmp, codecs, binary_log_block_size) = options.format.into_pieces();
        let opts = InternalOptions {
            db_directory:               options.database_directory,
            cmp:                        InternalComparator(cmp),
            policy:                     options.filter.filter_policy.map(InternalFilterPolicy),
            filter_chunk_size_log2:     options.filter.filter_chunk_size_log2,
            codecs,
            binary_log_block_size,
            verify_data_checksums:      options.consistency.verify_data_checksums,
            verify_index_checksums:     options.consistency.verify_index_checksums,
            unwrap_poison:              options.consistency.unwrap_poison,
            web_scale:                  options.consistency.web_scale,
            max_memtable_size:          options.memtable.max_memtable_size,
            max_write_log_file_size:    options.memtable.max_write_log_file_size,
            max_sstable_sizes:          options.sstable.max_sstable_sizes,
            compaction,
            write_throttling:           options.write_throttling,
            iter_buffer_capacity_limit: options.buffer_pool.iter_buffer_capacity_limit,
        };

        let dynamic = AtomicDynamicOptions::new(DynamicOptions {
            memtable_compressor:            options.compression.memtable_compressor,
            table_compressors:              options.compression.table_compressors,
            memtable_compression_goal:      options.compression.memtable_compression_goal,
            table_compression_goals:        options.compression.table_compression_goals,
            sstable_block_size:             options.sstable.sstable_block_size,
            sstable_block_restart_interval: options.sstable.block_restart_interval,
        });

        let block_cache = BlockCache::new(
            options.cache.block_cache_size,
            options.cache.average_block_size,
        );
        let table_cache = TableCache::new(options.cache.table_cache_capacity);

        let mut_opts = InternallyMutableOptions {
            filesystem: options.filesystem,
            dynamic,
            logger,
            buffer_pool: options.buffer_pool.buffer_pool,
            block_cache,
            table_cache,
        };

        let open_corruption_handler = options.consistency.open_corruption_handler;

        let open_opts = InternalOpenOptions {
            max_reused_manifest_size:  options.manifest.max_reused_manifest_size,
            initial_memtable_capacity: options.memtable.initial_memtable_capacity,
            max_reused_write_log_size: options.memtable.max_reused_write_log_size,
            memtable_pool_size:        options.memtable.memtable_pool_size,
            compact_in_background:     options.compaction.compact_in_background,
        };

        let manifest_file_number = FileNumber(0);
        let memtable = UniqueMemtable::new(
            open_opts.initial_memtable_capacity,
            #[expect(clippy::unusual_byte_groupings, reason = "random fun number")]
            0x_42_deadbeef_68,
            opts.cmp.fast_mirrored_clone(),
        );
        let reused_log = None;
        let table_builder = TableFileBuilder::new(&opts);
        let encoders = opts.codecs.init_encoders();
        let decoders = opts.codecs.init_decoders();

        Self {
            opts,
            mut_opts,
            open_corruption_handler,
            open_opts,
            lockfile,
            current_path,
            manifest_file_number,
            memtable,
            reused_log,
            table_builder,
            encoders,
            decoders,
        }
    }

    /// Recover a LevelDB database which is thought to exist.
    ///
    /// The `LOCK` file must have been acquired and `CURRENT` should exist, though might
    /// not be a file.
    ///
    /// On success, `self.reused_log` is `None`.
    ///
    /// Regardless of the result, the `open_corruption_handler` needs to be checked for an error.
    ///
    /// Does *not* garbage collect old files or start a compaction, which still needs to be done.
    pub(self) fn recover(
        &mut self,
    ) -> Result<RecoveredDB<FS::WriteFile>, RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        let vset_builder = self.begin_recovery()?;
        self.finish_recovery(vset_builder)
    }

    /// Recover the current `MANIFEST` file and all write-ahead `.log` files.
    ///
    /// On success, `self.manifest_number` is set to the `MANIFEST` file's file number.
    fn begin_recovery(&mut self) -> Result<
        VersionSetBuilder<FS::WriteFile, true>,
        RecoveryErrorKindAlias<FS, Cmp, Codecs>,
    > {
        // Recover the `MANIFEST` file.
        let BeginVersionSetRecovery {
            builder: mut vset_builder,
            mut log_buffers,
        } = VersionSetBuilder::begin_recovery(
            &self.opts,
            &self.mut_opts,
            &mut *self.open_corruption_handler,
            self.open_opts,
            &self.current_path,
            &mut self.manifest_file_number,
        )?;

        // Make sure that all expected table files are present, and figure out which `.log`
        // files to recover.
        let mut log_files = self.enumerate_files(&mut vset_builder)?;

        let mut vset_builder = vset_builder.finish_listing_old_logs();

        // Recover the log files in increasing order of their file numbers, so that older
        // log files are recovered first (in the order they were written).
        // (Note that the file size is second, and thus is less significant for the sort.)
        log_files.sort_unstable();
        let mut log_files = log_files.into_iter();

        // Separate this one out to recover last
        let last_log = log_files.next_back();
        // Recover all the non-last log files
        for (log_number, log_size) in log_files {
            let continue_reading_logs = self.recover_log_file(
                &mut vset_builder,
                &mut log_buffers,
                log_number,
                log_size,
                false,
            )?;

            if matches!(continue_reading_logs, ContinueReadingLogs::False) {
                return Ok(vset_builder);
            }
        }
        // Recover and maybe reuse the last log
        if let Some((log_number, log_size)) = last_log {
            self.recover_log_file(
                &mut vset_builder,
                &mut log_buffers,
                log_number,
                log_size,
                true,
            )?;
        }

        Ok(vset_builder)
    }

    /// Look through every file in the db directory to ensure that all expected table files are
    /// present and determine which `.log` files to recover.
    ///
    /// Returns an unsorted list of all `.log` files which should be recovered.
    fn enumerate_files(
        &self,
        vset_builder: &mut VersionSetBuilder<FS::WriteFile, false>,
    ) -> Result<
        Vec<(FileNumber, FileSize)>,
        RecoveryErrorKindAlias<FS, Cmp, Codecs>,
    > {
        let mut expected_table_files = vset_builder.expected_table_files();
        let mut log_files = Vec::new();
        let db_files = self.mut_opts.filesystem
            .child_files(&self.opts.db_directory)
            .map_err(|fs_err| RecoveryErrorKind::Open(OpenError::Filesystem(
                FilesystemError::FsError(fs_err),
                OpenFsError::ReadDatabaseDirectory,
            )))?;

        for child_file in db_files.child_files() {
            // Note that the relative path should not begin with `/`.
            let (relative_path, file_size) = child_file
                .map_err(|fs_err| RecoveryErrorKind::Open(OpenError::Filesystem(
                    FilesystemError::FsError(fs_err.into()),
                    OpenFsError::ReadDatabaseDirectory,
                )))?;

            let Some(maybe_filename) = relative_path.to_str() else { continue };
            let Some(maybe_filename) = LevelDBFileName::parse(maybe_filename) else { continue };

            match maybe_filename {
                LevelDBFileName::Log { file_number } => {
                    if vset_builder.log_should_be_recovered(file_number) {
                        log_files.push((file_number, FileSize(file_size)));
                        vset_builder
                            .mark_file_used(file_number)
                            .map_err(OutOfFileNumbers::into_recovery_err)?;
                    }
                }
                LevelDBFileName::Table { file_number }
                | LevelDBFileName::TableLegacyExtension { file_number } => {
                    expected_table_files.remove(&file_number);
                }
                LevelDBFileName::Lockfile
                | LevelDBFileName::Manifest { .. }
                | LevelDBFileName::Current
                | LevelDBFileName::Temp { .. }
                | LevelDBFileName::InfoLog
                | LevelDBFileName::OldInfoLog => {}
            }
        }

        if !expected_table_files.is_empty() {
            return Err(RecoveryErrorKind::Corruption(
                CorruptionError::MissingTableFiles(expected_table_files),
            ));
        }

        Ok(log_files)
    }

    /// Recover one write-ahead `.log` file.
    fn recover_log_file(
        &mut self,
        vset_builder:         &mut VersionSetBuilder<FS::WriteFile, true>,
        log_buffers:          &mut BinaryBlockLogReaderBuffers,
        log_number:           FileNumber,
        log_file_size:        FileSize,
        last_log:             bool,
    ) -> Result<ContinueReadingLogs, RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        let log_file_path = LevelDBFileName::Log { file_number: log_number }
            .file_path(&self.opts.db_directory);
        let log_file = self.mut_opts.filesystem
            .open_sequential(&log_file_path)
            .map_err(|fs_err| RecoveryErrorKind::Write(WriteError::Filesystem(
                FilesystemError::FsError(fs_err),
                log_number,
                WriteFsError::OpenWritableLog,
            )))?;

        let mut log_reader = log_buffers
            .read_log(log_file, log_number, log_file_size)
            .map_err(|io_err| RecoveryErrorKind::Open(OpenError::Filesystem(
                FilesystemError::Io(io_err),
                OpenFsError::OpenLog(log_number),
            )))?;

        let mut flushed_memtable = false;

        // Morally a while-loop, but with a very complicated condition in the first few lines.
        loop {
            let record = match log_reader.read_record(&mut *self.open_corruption_handler) {
                LogRecordResult::Some(record)      => record,
                LogRecordResult::EndOfFile         => break,
                LogRecordResult::ReadError(io_err) => return Err(RecoveryErrorKind::Open(
                    OpenError::Filesystem(
                        FilesystemError::Io(io_err),
                        OpenFsError::ReadLog(log_number),
                    ),
                )),
            };

            let parsed_write_batch = match parse_write_batch(record.data) {
                Ok(parsed_write_batch) => parsed_write_batch,
                Err((offset, decode_err)) => {
                    match self.open_corruption_handler
                        .write_batch_corruption(log_number, offset, decode_err)
                    {
                        LogControlFlow::Continue => continue,
                        LogControlFlow::Break    => break,
                    }
                }
            };

            vset_builder.mark_sequence_used(parsed_write_batch.batch_last_sequence);

            self.memtable.insert_write_batches(parsed_write_batch.batch);

            if self.memtable.allocated_bytes() > self.opts.max_memtable_size {
                // Flush the memtable, and reset it for further reads. We can't reuse the log
                // file; the `.log` file is supposed to correspond to a memtable, but we won't
                // have a single memtable corresponding to the whole `.log` file.
                flushed_memtable = true;
                self.flush_memtable(vset_builder)?;
            }
        };

        let (finished, control_flow) = self.open_corruption_handler.finished_log();

        let continue_reading_logs = match control_flow {
            FinishedLogControlFlow::Continue     => ContinueReadingLogs::True,
            FinishedLogControlFlow::BreakSuccess => ContinueReadingLogs::False,
            FinishedLogControlFlow::BreakError   => return Err(
                RecoveryErrorKind::Corruption(CorruptionError::HandlerReportedError),
            ),
        };

        let last_log = last_log || matches!(continue_reading_logs, ContinueReadingLogs::False);

        if let Some(log) = self.try_reuse_log(
            last_log,
            flushed_memtable,
            finished.log_reuse_permitted,
            &log_file_path,
        ) {
            self.reused_log = Some(ReusedLog {
                log,
                log_number,
            });
            return Ok(continue_reading_logs);
        }

        // If we get here, we didn't reuse the log.
        self.flush_memtable(vset_builder)?;
        Ok(continue_reading_logs)
    }

    /// Flush a memtable to zero or more level-0 table files.
    ///
    /// If the memtable iterator is empty, zero table files are used. Otherwise, table files are
    /// split **only** when absolutely necessary (for the sake of not overfilling the table's index
    /// block), regardless of settings for table file size. (This means that, almost always, at
    /// most one table file is used.)
    ///
    /// Note that the persisted `MANIFEST` is not updated, so this operation cannot result in
    /// immediate corruption (...though it could lead to corruption if someone uses Google's
    /// less-than-perfect LevelDB recovery tool...), and we can perform consistency checks on the
    /// recovered version set later.
    fn flush_memtable(
        &mut self,
        vset_builder: &mut VersionSetBuilder<FS::WriteFile, true>,
    ) -> Result<(), RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        let mut created_file_metadata = Vec::new();
        let mut memtable_iter = self.memtable.iter();

        memtable_iter.next();
        while let Some(first) = memtable_iter.current() {
            let table_file_number = vset_builder
                .new_table_file_number()
                .map_err(|OutOfFileNumbers {}| RecoveryErrorKind::Write(
                    WriteError::OutOfFileNumbers,
                ))?;

            let created = self.table_builder
                .flush_once(
                    &self.opts,
                    &self.mut_opts,
                    &mut self.encoders,
                    &mut self.decoders,
                    self.manifest_file_number,
                    table_file_number,
                    None,
                    &mut memtable_iter,
                    first,
                )
                .map_err(RwErrorKind::into_recovery_err)?;

            created_file_metadata.push(created);
        }

        vset_builder.add_new_table_files(created_file_metadata);
        self.memtable.reset();

        Ok(())
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
        // Note that we can't reuse the log file if we flushed the memtable; the `.log` file is
        // supposed to correspond to a memtable, but we won't have a single memtable corresponding
        // to the whole `.log` file.
        if !last_log
            || flushed_memtable
            || !reuse_permitted
            || !self.mut_opts.filesystem.supports_efficient_appendable()
        {
            return None;
        }

        let log_size = FileSize(self.mut_opts.filesystem.size_of_file(log_file_path).ok()?);
        if log_size >= self.open_opts.max_reused_write_log_size {
            return None;
        }

        let log_file = self.mut_opts.filesystem
            .open_appendable(log_file_path, CreateParentDir::False, SyncParentDir::False)
            .inspect_err(|_err| {
                // TODO: log error
            }).ok()?;

        let start_offset = FileOffset(log_size.0);
        let log = WriteLogWriter::new_with_offset(
            log_file,
            start_offset,
            self.opts.binary_log_block_size,
        );

        Some(log)
    }

    /// Performs many actions to finalize recovery, including possible updates to persistent state.
    ///
    /// - Create a new write-ahead `.log` file (if one was not reused).
    /// - Perform error checks on the recovered version (if requested).
    /// - Update the `MANIFEST` file (if necessary).
    ///
    /// Does *not* garbage collect old files or start a compaction, which still needs to be done.
    fn finish_recovery(
        &mut self,
        mut vset_builder: VersionSetBuilder<FS::WriteFile, true>,
    ) -> Result<
        RecoveredDB<FS::WriteFile>,
        RecoveryErrorKindAlias<FS, Cmp, Codecs>,
    > {
        // Get the reused log (and its file number, and its corresponding memtable),
        // or create a new one.
        let (log, log_number) = if let Some(reused_log) = self.reused_log.take() {
            (reused_log.log, reused_log.log_number)
        } else {
            let new_log_number = vset_builder.new_log_file_number()
                .map_err(OutOfFileNumbers::into_recovery_err)?;

            let new_log_path = LevelDBFileName::Log { file_number: new_log_number }
                .file_path(&self.opts.db_directory);

            let log_file = self.mut_opts.filesystem
                .open_writable(&new_log_path, CreateParentDir::False, SyncParentDir::False)
                .map_err(|fs_err| RecoveryErrorKind::Write(WriteError::Filesystem(
                    FilesystemError::FsError(fs_err),
                    new_log_number,
                    WriteFsError::OpenWritableLog,
                )))?;

            (
                WriteLogWriter::new_empty(log_file, self.opts.binary_log_block_size),
                new_log_number,
            )
        };

        let verify_new_version = self.open_corruption_handler.finished_all_logs()
            .map_err(|FinishError {}| RecoveryErrorKind::Corruption(
                CorruptionError::HandlerReportedError,
            ))?
            .verify_new_version;

        let version_set = vset_builder.finish(
            &self.opts,
            &self.mut_opts,
            verify_new_version,
            log_number,
        )?;

        Ok(RecoveredDB {
            version_set,
            current_write_log: log,
        })
    }
}

fn parse_write_batch(
    record: &[u8],
) -> Result<ParsedWriteBatch<'_>, (LogicalRecordOffset, WriteBatchDecodeError)> {
    let offset_zero = LogicalRecordOffset(0);
    let (header, headerless_entries) = record.split_first_chunk::<12>()
        .ok_or((offset_zero, WriteBatchDecodeError::TruncatedHeader))?;

    #[expect(clippy::unwrap_used, reason = "`8 < 12`; cannot panic")]
    let sequence_number = u64::from_le_bytes(*header.first_chunk().unwrap());

    #[expect(clippy::unwrap_used, reason = "`4 < 12`; cannot panic")]
    let num_entries = u32::from_le_bytes(*header.last_chunk().unwrap());

    let mut input = headerless_entries;
    let batch = BorrowedWriteBatch::validate(num_entries, &mut input)
        .map_err(|decode_err| {
            let offset = LogicalRecordOffset(12 + headerless_entries.len() - input.len());
            (offset, decode_err.into())
        })?;

    let batch_first_sequence = SequenceNumber::new_usable(sequence_number)
        .ok_or((offset_zero, WriteBatchDecodeError::FirstSequenceTooLarge))?;

    let last_sequence_before_batch = batch_first_sequence
        .checked_decrement()
        .ok_or((offset_zero, WriteBatchDecodeError::FirstSequenceZero))?;

    let (batch, batch_last_sequence) = ChainedWriteBatchIter::new_single(
        last_sequence_before_batch,
        batch,
    ).map_err(|OutOfSequenceNumbers {}| {
        (offset_zero, WriteBatchDecodeError::LastSequenceTooLarge)
    })?;

    Ok(ParsedWriteBatch { batch, batch_last_sequence })
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
}

/// Returned by [`parse_write_batch`].
struct ParsedWriteBatch<'a> {
    batch:               ChainedWriteBatchIter<'a>,
    batch_last_sequence: SequenceNumber,
}

use std::path::Path;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::FastMirroredClone;

use anchored_vfs::{CreateParentDir, IntoChildFileIterator as _, LevelDBFilesystem, SyncParentDir};

use crate::{
    compaction::flush_memtable,
    database_files::LevelDBFileName,
    memtable::UniqueMemtable,
    table_file::TableFileBuilder,
};
use crate::{
    all_errors::{
        aliases::{RecoveryErrorAlias, RecoveryErrorKindAlias},
        types::{
            CorruptionError, FinishError, FilesystemError, OpenError, OpenFsError, OutOfFileNumbers,
            OutOfSequenceNumbers, RecoveryError, RecoveryErrorKind, RwErrorKind,
            WriteBatchDecodeError, WriteError, WriteFsError,
        },
    },
    binary_block_log::{BinaryBlockLogReaderBuffers, LogRecordResult, WriteLogWriter},
    options::{InternalOpenOptions, InternalOptions, InternallyMutableOptions},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        error_handler::{FinishedLogControlFlow, LogControlFlow, OpenCorruptionHandler},
        pool::BufferPool,
    },
    pub_typed_bytes::{FileNumber, FileOffset, FileSize, LogicalRecordOffset, SequenceNumber},
    typed_bytes::{ContinueReadingLogs, NextFileNumber},
    version::{BeginVersionSetRecovery, VersionSet, VersionSetBuilder},
    write_batch::{BorrowedWriteBatch, ChainedWriteBatchIter},
};


/// The data necessary to create a [`InternalDBState`](super::state::InternalDBState).
pub(super) struct BuildDB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub opts:              InternalOptions<Cmp, Policy, Codecs>,
    pub mut_opts:          InternallyMutableOptions<FS, Policy, Pool>,
    pub open_opts:         InternalOpenOptions,
    pub lockfile:          FS::Lockfile,
    pub version_set:       VersionSet<FS::WriteFile>,
    pub memtable:          UniqueMemtable<Cmp>,
    pub current_write_log: WriteLogWriter<FS::WriteFile>,
    pub next_file_number:  NextFileNumber,
    pub table_builder:     TableFileBuilder<FS::WriteFile, Policy, Pool>,
    pub encoders:          Codecs::Encoders,
    pub decoders:          Codecs::Decoders,
}

impl<FS, Cmp, Policy, Codecs, Pool> Debug for BuildDB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     Debug + LevelDBFilesystem<WriteFile: Debug, Lockfile: Debug>,
    Cmp:    Debug + LevelDBComparator,
    Policy: Debug,
    Codecs: Debug + CompressionCodecs<Encoders: Debug, Decoders: Debug>,
    Pool:   Debug + BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("BuildDB")
            .field("opts",              &self.opts)
            .field("mut_opts",          &self.mut_opts)
            .field("open_opts",         &self.open_opts)
            .field("lockfile",          &self.lockfile)
            .field("version_set",       &self.version_set)
            .field("memtable",          &self.memtable)
            .field("current_write_log", &self.current_write_log)
            .field("next_file_number",  &self.next_file_number)
            .field("table_builder",     &self.table_builder)
            .field("encoders",          &self.encoders)
            .field("decoders",          &self.decoders)
            .finish()
    }
}

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

pub(super) struct DBBuilder<FS, Cmp, Policy, Codecs, Pool>
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
    /// Recover a LevelDB database which is thought to exist.
    ///
    /// The `LOCK` file must have been acquired and `CURRENT` should exist, though might
    /// not be a file.
    #[expect(clippy::type_complexity, reason = "??? this is fine")]
    pub(super) fn recover_existing(
        opts:                    InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:                InternallyMutableOptions<FS, Policy, Pool>,
        open_corruption_handler: Box<dyn OpenCorruptionHandler<Cmp::InvalidKeyError>>,
        open_opts:               InternalOpenOptions,
        lockfile:                FS::Lockfile,
        current_path:            &Path,
    ) -> Result<
        BuildDB<FS, Cmp, Policy, Codecs, Pool>,
        RecoveryErrorAlias<FS, Cmp, Codecs>,
    > {
        let memtable = UniqueMemtable::new(
            open_opts.initial_memtable_capacity,
            #[expect(clippy::unusual_byte_groupings, reason = "random fun number")]
            0x_42_deadbeef_68,
            opts.cmp.fast_mirrored_clone(),
        );
        let table_builder = TableFileBuilder::new(&opts);
        let encoders = opts.codecs.init_encoders();
        let decoders = opts.codecs.init_decoders();
        let mut builder = Self {
            opts,
            mut_opts,
            open_corruption_handler,
            open_opts,
            lockfile,
            memtable,
            reused_log:    None,
            table_builder,
            encoders,
            decoders,
        };

        let mut manifest_number = FileNumber(0);

        let result = builder
            .begin_recovery(current_path, &mut manifest_number)
            .and_then(|vset_builder| builder.finish(vset_builder));

        let DBBuilderFinish {
            version_set,
            current_write_log,
            next_file_number,
        } = match result {
            Ok(finished) => finished,
            Err(mut err_kind) => {
                if let Some(other_err) = builder.open_corruption_handler.get_error() {
                    err_kind.merge_worst_error(RecoveryErrorKind::Corruption(
                        other_err.into_corruption_error(manifest_number),
                    ));
                }

                return Err(RecoveryError {
                    db_directory: builder.opts.db_directory,
                    kind:         err_kind,
                })
            }
        };

        #[expect(clippy::shadow_unrelated, reason = "they are related")]
        let Self {
            opts,
            mut_opts,
            open_corruption_handler,
            open_opts,
            lockfile,
            memtable,
            // Above, in `.and_then(|vset_builder| builder.finish(vset_builder));`, we already
            // called `self.reused_log.take()`, so this field is `None`.
            reused_log: _already_consumed,
            table_builder,
            encoders,
            decoders,
        } = builder;

        if let Some(err) = open_corruption_handler.get_error() {
            return Err(RecoveryError {
                db_directory: opts.db_directory,
                kind:         RecoveryErrorKind::Corruption(
                    err.into_corruption_error(manifest_number),
                ),
            });
        }

        Ok(BuildDB {
            opts,
            mut_opts,
            open_opts,
            lockfile,
            version_set,
            memtable,
            current_write_log,
            next_file_number,
            table_builder,
            encoders,
            decoders,
        })
    }

    /// # `manifest_file_number` Outpointer
    /// If the manifest file number cannot be read, the provided outpointer is left unchanged.
    fn begin_recovery(
        &mut self,
        current_path: &Path,
        manifest_file_number_out: &mut FileNumber,
    ) -> Result<
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
            current_path,
            manifest_file_number_out,
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
                *manifest_file_number_out,
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
                *manifest_file_number_out,
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
        &mut self,
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

    fn recover_log_file(
        &mut self,
        vset_builder:         &mut VersionSetBuilder<FS::WriteFile, true>,
        manifest_file_number: FileNumber,
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
                self.flush_memtable(vset_builder, manifest_file_number)?;
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
        self.flush_memtable(vset_builder, manifest_file_number)?;
        Ok(continue_reading_logs)
    }

    fn flush_memtable(
        &mut self,
        vset_builder:         &mut VersionSetBuilder<FS::WriteFile, true>,
        manifest_file_number: FileNumber,
    ) -> Result<(), RecoveryErrorKindAlias<FS, Cmp, Codecs>> {
        let created_tables = flush_memtable(
            &mut self.table_builder,
            &self.opts,
            &self.mut_opts,
            &mut self.encoders,
            &mut self.decoders,
            manifest_file_number,
            || vset_builder.new_table_file_number(),
            self.memtable.iter(),
        ).map_err(|rw_err| match rw_err {
            RwErrorKind::Options(err)    => RecoveryErrorKind::Options(err),
            RwErrorKind::Read(err)       => RecoveryErrorKind::Read(err),
            RwErrorKind::Write(err)      => RecoveryErrorKind::Write(err),
            RwErrorKind::Corruption(err) => RecoveryErrorKind::Corruption(err),
        })?;

        vset_builder.add_new_table_files(created_tables);
        self.memtable.reset();

        Ok(())
    }

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

    fn finish(
        &mut self,
        mut vset_builder: VersionSetBuilder<FS::WriteFile, true>,
    ) -> Result<
        DBBuilderFinish<FS::WriteFile>,
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

        let (version_set, next_file_number) = vset_builder.finish(
            &self.opts,
            &self.mut_opts,
            verify_new_version,
            log_number,
        )?;

        Ok(DBBuilderFinish {
            version_set,
            current_write_log: log,
            next_file_number,
        })
    }
}

struct DBBuilderFinish<File> {
    version_set:       VersionSet<File>,
    current_write_log: WriteLogWriter<File>,
    next_file_number:  NextFileNumber,
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

    Ok(ParsedWriteBatch {
        batch,
        last_sequence_before_batch,
        batch_last_sequence,
    })
}

struct ParsedWriteBatch<'a> {
    batch:                      ChainedWriteBatchIter<'a>,
    last_sequence_before_batch: SequenceNumber,
    batch_last_sequence:        SequenceNumber,
}

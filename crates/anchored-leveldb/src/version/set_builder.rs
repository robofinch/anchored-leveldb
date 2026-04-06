use std::{mem, str};
use std::{borrow::Cow, collections::HashSet, sync::Arc};
use std::{
    io::{Error as IoError, Read},
    fmt::{Debug, Formatter, Result as FmtResult},
    path::{Path, PathBuf},
};

use clone_behavior::FastMirroredClone as _;

use anchored_vfs::{CreateParentDir, LevelDBFilesystem, SyncParentDir, WritableFile};

use crate::{
    file_tracking::FileMetadata,
    internal_logger::InternalLogger,
    table_format::InternalComparator,
};
use crate::{
    all_errors::{
        aliases::RecoveryErrorKindAlias,
        types::{
            CorruptedManifestError, CorruptionError, FilesystemError, FinishError, OpenError,
            OpenFsError, OptionsError, OutOfFileNumbers, RecoveryErrorKind, WriteError,
            WriteFsError,
        },
    },
    binary_block_log::{BinaryBlockLogReaderBuffers, ManifestRecordResult, Slices, WriteLogWriter},
    database_files::{LevelDBFileName, set_current},
    options::{
        InternallyMutableOptions, InternalOpenOptions, InternalOptions,
        pub_options::SeekCompactionOptions,
    },
    pub_traits::{
        cmp_and_policy::LevelDBComparator,
        compression::CompressionCodecs,
        error_handler::{ManifestControlFlow, OpenCorruptionHandler},
        pool::BufferPool,
    },
    pub_typed_bytes::{
        BinaryLogBlockSize, FileNumber, FileOffset, FileSize, IndexLevel as _, Level,
        LogicalRecordOffset, NUM_LEVELS_USIZE, SequenceNumber, ShortSlice,
    },
    typed_bytes::{NextFileNumber, OptionalCompactionPointer},
};

use super::{
    edit::VersionEdit,
    set::VersionSet,
    version_struct::Version,
    version_tracking::CurrentVersion,
};
use super::version_builder::{CheckBuiltVersion, VersionBuilder};


/// The data necessary to create a [`VersionSet`].
pub(super) struct BuildVersionSet<File> {
    pub current_log_number:   FileNumber,
    pub prev_log_number:      FileNumber,
    pub last_sequence:        SequenceNumber,

    pub manifest_file_number: FileNumber,
    pub manifest_writer:      WriteLogWriter<File>,
    /// Must be empty. Used solely for its capacity.
    pub edit_record_buffer:   Vec<u8>,

    pub current_version:      CurrentVersion,
    pub compaction_pointers:  [OptionalCompactionPointer; NUM_LEVELS_USIZE.get()],
}

impl<File> Debug for BuildVersionSet<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("BuildVersionSet")
            .field("current_log_number",   &self.current_log_number)
            .field("prev_log_number",      &self.prev_log_number)
            .field("last_sequence",        &self.last_sequence)
            .field("manifest_file_number", &self.manifest_file_number)
            .field("manifest_writer",      &self.manifest_writer)
            .field("edit_record_buffer",   &format!(
                "<buffer of length {} and capacity {}>",
                self.edit_record_buffer.len(),
                self.edit_record_buffer.capacity(),
            ))
            .field("current_version",      &self.current_version)
            .field("compaction_pointers",  &self.compaction_pointers)
            .finish()
    }
}

/// A [`VersionSet`] cannot be created in a single step; some fields may initially be missing
/// or not in their final state. This builder handles all the transient initialization states
/// so that (almost) every [`VersionSet`] is fully initialized (in a logical sense, not a
/// memory-safety sense).
///
/// (The "almost" exception is because a [`VersionSet`] in the middle of the apply->log->install
/// procedure might not be in a normal state.)
///
/// This builder provides some guardrails for the database recovery process, but does not cover
/// everything.
///
/// # MANIFEST write during `finish`
/// If there was at most one old `.log` file and both the old `MANIFEST` and `.log` files could
/// be reused, then nothing that happened during recovery would necessitate a `MANIFEST` write.
///
/// However, if the old `MANIFEST` file or some `.log` files could not be reused, then
/// [`VersionSetBuilder::finish`] needs to persist some data.
///
/// ## Possible causes
///
/// - The creation of a new `MANIFEST` file necessitates that a full description of the constructed
///   [`VersionSet`] is written to the new file, and the old `MANIFEST` file may later be discarded.
///
/// - Noting that only the last `.log` file might be able to be reused, any `.log` files which are
///   not reused are flushed to table files. This is necessary in order to discard out-of-date
///   `.log` files. To avoid losing data from discarding the old log files, the current `MANIFEST`
///   must be updated to refer to both the new table files _and_ the new `.log` file corresponding
///   to the current memtable, which may contain some information not yet flushed to a table file.
///   The old `.log` files can be marked for garbage collection by setting `min_log_number` equal to
///   the current `.log` file and setting `prev_log_number` to 0.
///
/// Even if no `.log` files were flushed to table files, we can update `min_log_number` and
/// `prev_log_number` during the `MANIFEST` write; in such a scenario, there must be only one
/// `.log` file (either reused, or newly created if there was no previous `.log` file), so
/// discarding all log files except the current `.log` file is a no-op.
///
/// ## Actions which do _not_ cause the write
///
/// - Creating a new `.log` file
///   - If, for some reason, there was no previous `.log` file, we don't need to touch
///     `min_log_number`, since it would be less than the value of `self.next_file_number` at the
///     time a file number was assigned to the new `.log` file. We do not need to persist the log
///     number of the latest log file; we need only persist a minimum file number of logs which
///     should be preserved. We only need to update the persisted write-ahead log number
///     (`min_log_number` and `prev_log_number`) when we want to delete old, irrelevant write-ahead
///     log files, which isn't relevant in this case.
/// - Recovering an old write-ahead log file, marking it as used, and mutating `next_file_number`
///   and `last_sequence`.
///   - This _does_ impact the `next_file_number` and `last_sequence` of the recovered `VersionSet`.
///   - Because changes to those values can be handled during the recovery process of later
///     database invocations, we have no need to update those values right away.
///   - We do not forget to persist the numbers later, since every write to a MANIFEST log is
///     required to include:
///     - `min_log_number`
///     - `prev_log_number`
///     - `next_file_number`
///     - `last_sequence`
pub(crate) struct VersionSetBuilder<File, const ALL_OLD_LOGS_FOUND: bool> {
    /// The minimum log number to keep. Any write-ahead log file with a number greater than or
    /// equal to this number should not be deleted. (Additionally, and separately, the
    /// `prev_log_number` log should not be deleted if it exists.)
    ///
    /// When old log files are compacted into a new table file, the next MANIFEST update should set
    /// this to the current log number.
    min_log_number:      FileNumber,
    /// The file number of the previous write-ahead log is no longer used, but is still tracked
    /// as older versions of LevelDB might read it.
    ///
    /// When old log files are compacted into a new table file, the next MANIFEST update should set
    /// this to 0.
    prev_log_number:     FileNumber,
    next_file_number:    FileNumber,
    last_sequence:       SequenceNumber,

    /// `Some` if and only if the old manifest is being reused, unless inside
    /// [`VersionSetBuilder::finish_try_scope`], in which case this field is always `None`.
    ///
    /// If the old manifest is not being reused, a new `MANIFEST` file must be created and written
    /// to, including the application of any `recovery_edit`.
    reused_manifest:     Option<(WriteLogWriter<File>, FileNumber)>,

    current_version:     CurrentVersion,
    compaction_pointers: [OptionalCompactionPointer; NUM_LEVELS_USIZE.get()],

    /// If nonempty, the current `MANIFEST` file (whether reused or new) must be written to,
    /// to record both these new `added_files` and an updated `min_log_number` and
    /// `prev_log_number`.
    added_table_files:   Vec<(Level, Arc<FileMetadata>)>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File: WritableFile> VersionSetBuilder<File, false> {
    /// Recover the `MANIFEST` file, and prepare to determine which `.log` files must be recovered.
    ///
    /// `current_path` should be the path of the `CURRENT` file.
    ///
    /// # `manifest_file_number` Outpointer
    /// If the manifest file number cannot be read, the provided outpointer is left unchanged.
    pub fn begin_recovery<FS, Cmp, Policy, Codecs, Pool>(
        opts:                     &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:                 &InternallyMutableOptions<FS, Policy, Pool>,
        open_corruption_handler:  &mut (
            dyn OpenCorruptionHandler<Cmp::InvalidKeyError> + Send + Sync
        ),
        open_opts:                InternalOpenOptions,
        current_path:             &Path,
        manifest_file_number_out: &mut FileNumber,
    ) -> Result<BeginVersionSetRecovery<File>, RecoveryErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<WriteFile = File>,
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
        Pool:   BufferPool,
    {
        let (manifest_path, manifest_file_number) = Self::read_manifest_name(
            opts,
            mut_opts,
            current_path,
        )?;
        *manifest_file_number_out = manifest_file_number;

        let manifest_file_size = mut_opts.filesystem
            .size_of_file(&manifest_path)
            .map(FileSize)
            .map_err(|fs_err| {
                RecoveryErrorKind::Open(OpenError::Filesystem(
                    FilesystemError::FsError(fs_err),
                    OpenFsError::SizeOfManifest(manifest_file_number),
                ))
            })?;
        let manifest_file = mut_opts.filesystem
            .open_sequential(&manifest_path)
            .map_err(|fs_err| {
                RecoveryErrorKind::Open(OpenError::Filesystem(
                    FilesystemError::FsError(fs_err),
                    OpenFsError::OpenManifest(manifest_file_number),
                ))
            })?;

        let mut log_buffers = BinaryBlockLogReaderBuffers::new(opts.binary_log_block_size);
        let mut compaction_pointers = Default::default();

        let mut recovered_manifest = RecoveredManifest::recover(
            open_corruption_handler,
            opts.compaction.seek_compactions,
            &opts.cmp,
            manifest_file,
            manifest_file_number,
            manifest_file_size,
            &mut log_buffers,
            &mut compaction_pointers,
        )?;

        let finish_manifest = open_corruption_handler.finished_manifest()
            .map_err(|FinishError {}| RecoveryErrorKind::Corruption(
                CorruptionError::HandlerReportedError,
            ))?;

        let check_built_version = if finish_manifest.verify_recovered_version {
            CheckBuiltVersion::Check {
                next_file_number: recovered_manifest.next_file_number,
            }
        } else {
            CheckBuiltVersion::NoCheck
        };

        let recovered_version = recovered_manifest.builder
            .finish(&opts.cmp, check_built_version)
            .map_err(|version_err| {
                RecoveryErrorKind::Corruption(CorruptionError::CorruptedManifest(
                    manifest_file_number,
                    CorruptedManifestError::CorruptedVersion(version_err),
                ))
            })?;
        let current_version = CurrentVersion::new(
            recovered_version,
            opts.compaction.size_compactions,
        );

        let reused_manifest = try_reuse_manifest(
            &mut_opts.filesystem,
            &mut_opts.logger,
            open_opts.max_reused_manifest_size,
            opts.binary_log_block_size,
            finish_manifest.manifest_reuse_permitted,
            &manifest_path,
            manifest_file_number,
            manifest_file_size,
        ).map(|manifest| (manifest, manifest_file_number));

        Ok(BeginVersionSetRecovery {
            builder: Self {
                min_log_number:      recovered_manifest.min_log_number,
                prev_log_number:     recovered_manifest.prev_log_number,
                next_file_number:    recovered_manifest.next_file_number,
                last_sequence:       recovered_manifest.last_sequence,
                reused_manifest,
                current_version,
                compaction_pointers,
                added_table_files:   Vec::new(),
            },
            log_buffers,
        })
    }

    pub fn read_manifest_name<FS, Cmp, Policy, Codecs, Pool>(
        opts:         &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:     &InternallyMutableOptions<FS, Policy, Pool>,
        current_path: &Path,
    ) -> Result<(PathBuf, FileNumber), RecoveryErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<WriteFile = File>,
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
        Pool:   BufferPool,
    {
        let mut current = mut_opts.filesystem.open_sequential(current_path)
            .map_err(|fs_err| RecoveryErrorKind::Open(OpenError::Filesystem(
                FilesystemError::FsError(fs_err),
                OpenFsError::OpenCurrent,
            )))?;

        let mut full_manifest_name = Vec::new();

        current.read_to_end(&mut full_manifest_name)
            .map_err(|io_err| RecoveryErrorKind::Open(OpenError::Filesystem(
                FilesystemError::Io(io_err),
                OpenFsError::ReadCurrent,
            )))?;

        drop(current);

        let manifest_name = full_manifest_name.trim_ascii_end();
        #[expect(clippy::expect_used, reason = "cannot panic")]
        let whitespace = full_manifest_name
            .strip_prefix(manifest_name)
            .expect("`string.trim_ascii_end()` is a prefix of `string`");

        let Some((manifest_name, manifest_file_number)) = str::from_utf8(manifest_name).ok()
            .and_then(|manifest_name| Some((
                manifest_name,
                LevelDBFileName::parse(manifest_name)?,
            )))
            .and_then(|(manifest_name, file_name)| {
                if let LevelDBFileName::Manifest { file_number } = file_name {
                    Some((manifest_name, file_number))
                } else {
                    None
                }
            }) else {
                if full_manifest_name.starts_with(b"MANIFEST-") {
                    // The database starts with `MANIFEST-`, so it's clearly not encrypted,
                    // so this is a corruption error.
                    return Err(RecoveryErrorKind::Corruption(
                        CorruptionError::CorruptedCurrent(full_manifest_name),
                    ));
                } else {
                    // The database doesn't even start with `MANIFEST-`. Encryption cannot be
                    // excluded as a possibility.
                    return Err(RecoveryErrorKind::Open(
                        OpenError::EncryptedDatabaseOrCorruptedCurrent(full_manifest_name),
                    ));
                }
            };

        if !matches!(whitespace, b"\n" | b"\r" | b"\r\n") {
            return Err(RecoveryErrorKind::Corruption(
                CorruptionError::CurrentWithoutNewline(full_manifest_name),
            ));
        }

        let manifest_path = opts.db_directory.join(manifest_name);
        Ok((manifest_path, manifest_file_number))
    }

    #[must_use]
    pub fn expected_table_files(&self) -> HashSet<FileNumber> {
        let num_expected = Level::ALL_LEVELS.into_iter()
            .map(|level| {
                self.current_version.level_files(level).inner().len()
            })
            .sum();
        let mut expected_files = HashSet::with_capacity(num_expected);

        for level in Level::ALL_LEVELS {
            expected_files.extend(
                self.current_version.level_files(level).inner()
                    .iter().map(|file_metadata| file_metadata.file_number()),
            );
        }

        expected_files
    }

    /// Check whether a `.log` file with the indicated file number should be recovered.
    ///
    /// Any log file whose file number is either at least `min_log_number` or equal to
    /// `prev_log_number` must be recovered; the others can be discarded.
    #[must_use]
    pub fn log_should_be_recovered(&self, log_file_number: FileNumber) -> bool {
        log_file_number < self.min_log_number && log_file_number != self.prev_log_number
    }

    // This function ***must not*** be called after `Self::new_file_number`; else, the caller
    // could easily assign the same file number to multiple files.
    // We enforce this requirement with a const generic parameter.
    // Note that file numbers cannot be assumed unique because Google's leveldb has that problem;
    // here, we enforce at the type level that the problem cannot occur.
    //
    /// Mark the number of a file created by a previous database invocation as used.
    ///
    /// Using this function is never incorrect, and at worst consumes extra file numbers;
    /// err on the side of calling it, even if the associated file turns out to be incomplete
    /// or corrupted.
    ///
    /// Currently, this is used solely to record existing `.log` files which might have been
    /// written after the most recent `MANIFEST` write.
    pub fn mark_file_used(&mut self, file_number: FileNumber) -> Result<(), OutOfFileNumbers> {
        if self.next_file_number <= file_number {
            self.next_file_number = file_number.next()?;
        }
        Ok(())
    }

    /// Finish determining which `.log` files should be recovered, and begin recovering them.
    #[must_use]
    pub fn finish_listing_old_logs(self) -> VersionSetBuilder<File, true> {
        VersionSetBuilder {
            min_log_number:      self.min_log_number,
            prev_log_number:     self.prev_log_number,
            next_file_number:    self.next_file_number,
            last_sequence:       self.last_sequence,
            reused_manifest:     self.reused_manifest,
            current_version:     self.current_version,
            compaction_pointers: self.compaction_pointers,
            added_table_files:   self.added_table_files,
        }
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File> VersionSetBuilder<File, true> {
    fn new_file_number(&mut self) -> Result<FileNumber, OutOfFileNumbers> {
        let new_file_number = self.next_file_number;
        self.next_file_number = self.next_file_number.next()?;
        Ok(new_file_number)
    }

    /// Allocate a new file number for a new `.ldb` table file, to be compacted from some or all of
    /// the data in old `.log` files.
    ///
    /// On successful compaction, [`Self::add_new_table_file`] should then be called.
    pub fn new_table_file_number(&mut self) -> Result<FileNumber, OutOfFileNumbers> {
        self.new_file_number()
    }

    /// Record the successful creation of new table files, compacted from some or all of the data
    /// in old `.log` files.
    ///
    /// The table file is placed into level 0.
    pub fn add_new_table_files(&mut self, file_metadata: Vec<FileMetadata>) {
        let file_metadata = file_metadata.into_iter().map(|meta| (Level::ZERO, Arc::new(meta)));
        self.added_table_files.extend(file_metadata);
    }

    /// Allocate a new file number for a `.log` file (NOT a table file corresponding to a
    /// compacted `.log` file).
    pub fn new_log_file_number(&mut self) -> Result<FileNumber, OutOfFileNumbers> {
        self.new_file_number()
    }

    // `VersionSetBuilder` does not expose anything which depends on `mark_sequence_used`
    // other than `finish`, so unlike `mark_file_used`, this method can be called at any point
    // during recovery without issue.
    pub fn mark_sequence_used(&mut self, sequence_number: SequenceNumber) {
        self.last_sequence = self.last_sequence.max(sequence_number);
    }

    /// Any existing `.log` file _other_ than the one whose file number is `current_log_number`
    /// must have been flushed to a table file.
    pub fn finish<FS, Cmp, Policy, Codecs, Pool>(
        mut self,
        opts:               &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:           &InternallyMutableOptions<FS, Policy, Pool>,
        verify_new_version: bool,
        current_log_number: FileNumber,
    ) -> Result<(VersionSet<File>, NextFileNumber), RecoveryErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<WriteFile = File>,
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
        Pool:   BufferPool,
        File:   WritableFile,
    {
        // All older `.log` files can be discarded. Each old `.log` file was either reused,
        // and is thus the one referred to by `current_log_number`, or was flushed to a table file
        // recorded in `self.added_table_files`. The below code ensures that any `MANIFEST`
        // write persists `self.added_table_files` (if nonempty).
        self.min_log_number  = current_log_number;
        self.prev_log_number = FileNumber(0);

        if let Some((manifest_writer, manifest_file_number)) = self.reused_manifest.take() {
            if self.added_table_files.is_empty() {
                // We have not yet flushed any `.log` file to a `.ldb` file.
                // We may have created a new `.log` file, but we do not need to mutate
                // `min_log_number`. Even if we were to crash before issuing a `MANIFEST` write,
                // the recovery process would recover any fields we may have mutated (new `.log`,
                // `next_file_number`, `last_sequence`).
                //
                // TLDR: Do nothing.

                Ok((
                    VersionSet::new(BuildVersionSet {
                        current_log_number:   self.min_log_number,
                        prev_log_number:      self.prev_log_number,
                        last_sequence:        self.last_sequence,
                        manifest_file_number,
                        manifest_writer,
                        edit_record_buffer:   Vec::new(),
                        current_version:      self.current_version,
                        compaction_pointers:  self.compaction_pointers,
                    }),
                    NextFileNumber::new(self.next_file_number),
                ))
            } else {
                // We need to issue a `MANIFEST` write, but need not write the base version.
                self.finish_with_manifest_write(
                    opts,
                    mut_opts,
                    verify_new_version,
                    manifest_writer,
                    manifest_file_number,
                    None,
                )
            }
        } else {
            // We need to issue a `MANIFEST` write, including writing the base version.
            let file_number = self.new_file_number()
                .map_err(OutOfFileNumbers::into_recovery_err)?;
            let manifest_name = LevelDBFileName::Manifest { file_number }.file_name();
            let manifest_path = opts.db_directory.join(&manifest_name);
            let manifest_file = mut_opts.filesystem
                .open_writable(&manifest_path, CreateParentDir::False, SyncParentDir::False)
                .map_err(|fs_err| RecoveryErrorKind::Write(WriteError::Filesystem(
                    FilesystemError::FsError(fs_err),
                    file_number,
                    WriteFsError::OpenWritableManifest,
                )))?;

            self.finish_with_manifest_write(
                opts,
                mut_opts,
                verify_new_version,
                WriteLogWriter::new_empty(manifest_file, opts.binary_log_block_size),
                file_number,
                Some(&manifest_name),
            ).inspect_err(|_error| {
                // Try to clean up the now-pointless manifest file. No worries if that fails,
                // the next time that file is opened, it'll be with `open_writable` not
                // `open_appendable`, so no corruption can occur.
                // Also, any leftover file will eventually be garbage-collected.
                let _err = mut_opts.filesystem.remove_file(&manifest_path);
            })
        }
    }

    /// We created a new manifest file iff `new_manifest_name` is `Some`.
    ///
    /// This function should only be called from [`Self::finish`], after
    /// `self.min_log_number` and `self.prev_log_number` have been updated.
    fn finish_with_manifest_write<FS, Cmp, Policy, Codecs, Pool>(
        mut self,
        opts:                 &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:             &InternallyMutableOptions<FS, Policy, Pool>,
        verify_new_version:   bool,
        mut manifest_writer:  WriteLogWriter<File>,
        manifest_file_number: FileNumber,
        new_manifest_name:    Option<&str>,
    ) -> Result<(VersionSet<File>, NextFileNumber), RecoveryErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<WriteFile = File>,
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
        Pool:   BufferPool,
        File:   WritableFile,
    {
        // The `VersionEdit` has at least the minimum four fields, plus `added_files`.
        let edit = VersionEdit {
            log_number:       Some(self.min_log_number),
            prev_log_number:  Some(self.prev_log_number),
            next_file_number: Some(self.next_file_number),
            last_sequence:    Some(self.last_sequence),
            added_files:      self.added_table_files,
            ..VersionEdit::new_empty()
        };

        let mut builder = VersionBuilder::new(
            self.current_version.version().fast_mirrored_clone(),
            &mut self.compaction_pointers,
        );
        builder.apply(&edit);

        let check_built_version = if verify_new_version {
            CheckBuiltVersion::Check {
                next_file_number: self.next_file_number,
            }
        } else {
            CheckBuiltVersion::NoCheck
        };

        let built_version = builder.finish(&opts.cmp, check_built_version)
            .map_err(|version_err| {
                RecoveryErrorKind::Corruption(CorruptionError::CorruptedVersion(version_err))
            })?;
        let built_version = CurrentVersion::new(
            built_version,
            opts.compaction.size_compactions,
        );

        let mut edit_record_buffer = Vec::new();

        if new_manifest_name.is_some() {
            // Note that if `edit` had any compaction pointers, `write_base_version(..)`
            // and `edit.encode(..)` below would together record the same compaction pointers twice.
            // Since the pointers contain arbitrary bytes from user keys, that clone should be
            // avoided with `edit.compaction_pointers.clear()`; however, `edit.compaction_pointers`
            // is already empty.
            write_base_version(
                opts.cmp.0.name(),
                &self.current_version,
                &self.compaction_pointers,
                &mut manifest_writer,
                &mut edit_record_buffer,
            ).map_err(|io_err| RecoveryErrorKind::Write(WriteError::Filesystem(
                FilesystemError::Io(io_err),
                manifest_file_number,
                WriteFsError::WriteManifest,
            )))?;
            // Clear the record buffer; `write_base_version` does not itself clear the buffer
            // it's given.
            // Note that we might NOT clear the buffer if we return early due to an error;
            // that's fine, since the buffer only escapes this function if it returns successfully.
            edit_record_buffer.clear();
        }

        edit.encode(&mut edit_record_buffer);
        manifest_writer
            .add_record(Slices::new_single(&edit_record_buffer))
            .map_err(|io_err| RecoveryErrorKind::Write(WriteError::Filesystem(
                FilesystemError::Io(io_err),
                manifest_file_number,
                WriteFsError::WriteManifest,
            )))?;
        edit_record_buffer.clear();

        if let Some(manifest_name) = new_manifest_name {
            set_current(
                &mut_opts.filesystem,
                &opts.db_directory,
                manifest_file_number,
                manifest_name,
            ).map_err(|(fs_err, set_current_err)| {
                RecoveryErrorKind::Write(WriteError::Filesystem(
                    fs_err,
                    manifest_file_number,
                    WriteFsError::SetCurrent(set_current_err),
                ))
            })?;
        }

        Ok((
            VersionSet::new(BuildVersionSet {
                current_log_number:  self.min_log_number,
                prev_log_number:     self.prev_log_number,
                last_sequence:       self.last_sequence,
                manifest_file_number,
                manifest_writer,
                edit_record_buffer,
                current_version:     built_version,
                compaction_pointers: self.compaction_pointers,
            }),
            NextFileNumber::new(self.next_file_number),
        ))
    }
}

impl<File, const ALL_OLD_LOGS_FOUND: bool> Debug for VersionSetBuilder<File, ALL_OLD_LOGS_FOUND> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("VersionSetBuilder")
            .field("min_log_number",      &self.min_log_number)
            .field("prev_log_number",     &self.prev_log_number)
            .field("next_file_number",    &self.next_file_number)
            .field("last_sequence",       &self.last_sequence)
            .field("reused_manifest",     &self.reused_manifest)
            .field("current_version",     &self.current_version)
            .field("compaction_pointers", &self.compaction_pointers)
            .field("added_table_files",   &self.added_table_files)
            .finish()
    }
}

pub(crate) struct BeginVersionSetRecovery<File> {
    pub builder:     VersionSetBuilder<File, false>,
    pub log_buffers: BinaryBlockLogReaderBuffers,
}

impl<File> Debug for BeginVersionSetRecovery<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("BeginVersionSetRecovery")
            .field("builder",     &self.builder)
            .field("log_buffers", &self.log_buffers)
            .finish()
    }
}

#[derive(Debug)]
struct RecoveredManifest<'a> {
    min_log_number:   FileNumber,
    prev_log_number:  FileNumber,
    next_file_number: FileNumber,
    last_sequence:    SequenceNumber,
    builder:          VersionBuilder<'a>,
}

impl<'a> RecoveredManifest<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "not easy to meaningfully group the args together",
    )]
    fn recover<ReadFile: Read, Fs, Cmp: LevelDBComparator, Compression, Decompression>(
        open_corruption_handler: &mut (
            dyn OpenCorruptionHandler<Cmp::InvalidKeyError> + Send + Sync
        ),
        seek_opts:               SeekCompactionOptions,
        cmp:                     &InternalComparator<Cmp>,
        manifest_file:           ReadFile,
        manifest_file_number:    FileNumber,
        manifest_file_size:      FileSize,
        log_buffers:             &mut BinaryBlockLogReaderBuffers,
        compaction_pointers:     &'a mut [OptionalCompactionPointer; NUM_LEVELS_USIZE.get()],
    ) -> Result<Self, RecoveryErrorKind<Fs, Cmp::InvalidKeyError, Compression, Decompression>> {
        let mut min_log_number = None;
        let mut prev_log_number = None;
        let mut next_file_number = None;
        let mut last_sequence = None;

        let mut builder = VersionBuilder::new(
            Arc::new(Version::new_empty()),
            compaction_pointers,
        );

        let mut manifest_reader = log_buffers.read_manifest(manifest_file, manifest_file_size)
            .map_err(|io_err| RecoveryErrorKind::Open(OpenError::Filesystem(
                FilesystemError::Io(io_err),
                OpenFsError::ReadManifest(manifest_file_number),
            )))?;

        // Morally a while-loop, but with a very complicated condition in the first few lines
        loop {
            let record = match manifest_reader.read_record(open_corruption_handler) {
                ManifestRecordResult::Some(record)         => record,
                ManifestRecordResult::EndOfFile            => break,
                ManifestRecordResult::HandlerReportedError => return Err(
                    RecoveryErrorKind::Corruption(CorruptionError::HandlerReportedError),
                ),
                ManifestRecordResult::ReadError(io_err)    => return Err(
                    RecoveryErrorKind::Open(OpenError::Filesystem(
                        FilesystemError::Io(io_err),
                        OpenFsError::ReadManifest(manifest_file_number),
                    )),
                ),
            };

            // `VersionEdit::decode_from` consumes `edit_input`, leaving it empty on success.
            // That is, the number of bytes it read is given by
            // `LogicalRecordOffset(record.data.len() - edit_input.len())`.
            let mut edit_input = record.data;
            let edit_result = VersionEdit::decode_from(
                &mut edit_input,
                seek_opts,
                cmp.validate_user(),
            );
            let edit = match edit_result {
                Ok(mut edit) => {
                    if let Some(recorded_cmp_name) = &mut edit.comparator_name {
                        let chosen_comparator_name = cmp.0.name();

                        if chosen_comparator_name.inner() != &**recorded_cmp_name {
                            return Err(RecoveryErrorKind::Options(
                                OptionsError::MismatchedComparator {
                                    chosen:   chosen_comparator_name,
                                    recorded: mem::take(recorded_cmp_name).into_owned(),
                                },
                            ));
                        }
                    }
                    edit
                }
                Err(edit_err) => {
                    let offset = LogicalRecordOffset(record.data.len() - edit_input.len());
                    match open_corruption_handler.version_edit_corruption(offset, edit_err) {
                        ManifestControlFlow::Continue => continue,
                        ManifestControlFlow::BreakSuccess => break,
                        ManifestControlFlow::BreakError => return Err(
                            RecoveryErrorKind::Corruption(CorruptionError::HandlerReportedError),
                        ),
                    }
                }
            };

            builder.apply(&edit);

            if edit.log_number.is_some() {
                min_log_number = edit.log_number;
            }
            if edit.prev_log_number.is_some() {
                prev_log_number = edit.prev_log_number;
            }
            if edit.next_file_number.is_some() {
                next_file_number = edit.next_file_number;
            }
            if edit.last_sequence.is_some() {
                last_sequence = edit.last_sequence;
            }
        }

        let min_log_number = min_log_number
            .ok_or(RecoveryErrorKind::Corruption(CorruptionError::CorruptedManifest(
                manifest_file_number,
                CorruptedManifestError::MissingMinLogNumber,
            )))?;
        let prev_log_number = prev_log_number.unwrap_or(FileNumber(0));
        let mut next_file_number = next_file_number
            .ok_or(RecoveryErrorKind::Corruption(CorruptionError::CorruptedManifest(
                manifest_file_number,
                CorruptedManifestError::MissingNextFileNumber,
            )))?;
        let last_sequence = last_sequence
            .ok_or(RecoveryErrorKind::Corruption(CorruptionError::CorruptedManifest(
                manifest_file_number,
                CorruptedManifestError::MissingLastSequenceNumber,
            )))?;

        let larger_log_num = min_log_number.max(prev_log_number);
        if next_file_number <= larger_log_num {
            next_file_number = larger_log_num.next().map_err(OutOfFileNumbers::into_recovery_err)?;
        }

        Ok(Self {
            min_log_number,
            prev_log_number,
            next_file_number,
            last_sequence,
            builder,
        })
    }
}

#[expect(clippy::too_many_arguments, reason = "not easy to meaningfully group the args together")]
#[must_use]
fn try_reuse_manifest<FS: LevelDBFilesystem>(
    filesystem:               &FS,
    logger:                   &InternalLogger<FS::WriteFile>,
    max_reused_manifest_size: FileSize,
    block_size:               BinaryLogBlockSize,
    reuse_permitted:          bool,
    manifest_path:            &Path,
    manifest_file_number:     FileNumber,
    manifest_file_size:       FileSize,
) -> Option<WriteLogWriter<FS::WriteFile>> {
    if max_reused_manifest_size.0 == 0
        || manifest_file_size.0 > max_reused_manifest_size.0
        || !reuse_permitted
        || !filesystem.supports_efficient_appendable()
    {
        return None;
    }

    let manifest_file = filesystem.open_appendable(
        manifest_path, CreateParentDir::False, SyncParentDir::False)
        .inspect_err(|_err| {
            let _: _ = logger;
            let _: _ = manifest_file_number;
            // TODO: log error
        }).ok()?;

    let start_offset = FileOffset(manifest_file_size.0);

    Some(WriteLogWriter::new_with_offset(manifest_file, start_offset, block_size))
}

/// The input `edit_record_buffer` must be empty. Its contents after the function returns are
/// unspecified.
fn write_base_version<File: WritableFile>(
    cmp_name:            ShortSlice<'static>,
    current_version:     &CurrentVersion,
    compaction_pointers: &[OptionalCompactionPointer; NUM_LEVELS_USIZE.get()],
    manifest_writer:     &mut WriteLogWriter<File>,
    edit_record_buffer:  &mut Vec<u8>,
) -> Result<(), IoError> {
    let mut edit = VersionEdit::new_empty();
    edit.comparator_name = Some(Cow::Borrowed(cmp_name.inner()));

    edit.compaction_pointers.reserve(NUM_LEVELS_USIZE.get());
    for (level, compaction_pointer) in compaction_pointers.enumerated_iter() {
        if let Some(pointer) = compaction_pointer.internal_key() {
            // This allocation could be avoided by, for instance, using `Cow`s
            // and adding a lifetime to VersionEdit, or by making a specialized function
            // for adding an encoded version edit with just the fields used and available here.
            // TODO(micro-opt): consider avoiding this allocation.
            edit.compaction_pointers.push((level, pointer.to_owned()));
        }
    }

    for level in Level::ALL_LEVELS {
        let level_files: &[Arc<FileMetadata>] = current_version.level_files(level).inner();

        edit.added_files.reserve(level_files.len());
        for file in level_files {
            edit.added_files.push((level, file.fast_mirrored_clone()));
        }
    }

    edit.encode(edit_record_buffer);
    manifest_writer.add_record(Slices::new_single(edit_record_buffer))
}

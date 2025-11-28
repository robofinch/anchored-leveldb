use std::{cell::Cell, collections::HashSet, io::Read, path::Path};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::MirroredClone as _;
use generic_container::FragileTryContainer as _;

use anchored_vfs::traits::{WritableFile, WritableFilesystem};

use crate::{
    config_constants::NUM_LEVELS_USIZE,
    containers::RefcountedFamily,
    inner_leveldb::InnerDBOptions,
};
use crate::{
    compaction::{CompactionPointer, OptionalCompactionPointer},
    database_files::{LevelDBFileName, set_current},
    file_tracking::{
        FileMetadata, IndexLevel as _, Level,
        RefcountedFileMetadata, SeeksBetweenCompactionOptions,
    },
    format::{FileNumber, OutOfFileNumbers, SequenceNumber},
    table_traits::{InternalComparator, LevelDBComparator},
    write_log::{LogWriteError, ReadRecord, WriteLogReader, WriteLogWriter},
};

use super::{
    set::VersionSet,
    version_builder::VersionBuilder,
    version_struct::Version,
    version_tracking::CurrentVersion,
};
use super::edit::{DebugAddedFiles, VersionEdit};


/// The data necessary to create a [`VersionSet`].
pub(super) struct BuildVersionSet<Refcounted: RefcountedFamily, File> {
    pub current_log_number:   FileNumber,
    pub prev_log_number:      FileNumber,
    pub next_file_number:     FileNumber,
    pub last_sequence:        SequenceNumber,

    pub manifest_file_number: FileNumber,
    pub manifest_writer:      WriteLogWriter<File>,
    /// Must be empty. Used solely for its capacity.
    pub edit_record_buffer:   Vec<u8>,

    pub current_version:      CurrentVersion<Refcounted>,
    pub compaction_pointers:  [OptionalCompactionPointer; NUM_LEVELS_USIZE],
}

impl<Refcounted: RefcountedFamily, File> Debug for BuildVersionSet<Refcounted, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("BuildVersionSet")
            .field("current_log_number",   &self.current_log_number)
            .field("prev_log_number",      &self.prev_log_number)
            .field("next_file_number",     &self.next_file_number)
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
pub(crate) struct VersionSetBuilder<
    Refcounted: RefcountedFamily,
    File,
    const ALL_OLD_LOGS_FOUND: bool,
> {
    /// The minimum log number to keep. Any write-ahead log file with a number greater than or
    /// equal to this number should not be deleted. (Additionally, and separately, the
    /// `prev_log_number` log should not be deleted if it exists.)
    ///
    /// When old log files are compacted into a new table file, the next MANIFEST update should set
    /// this to the current log number.
    min_log_number:               FileNumber,
    /// The file number of the previous write-ahead log is no longer used, but is still tracked
    /// as older versions of LevelDB might read it.
    ///
    /// When old log files are compacted into a new table file, the next MANIFEST update should set
    /// this to 0.
    prev_log_number:              FileNumber,
    next_file_number:             FileNumber,
    last_sequence:                SequenceNumber,

    /// `Some` if and only if the old manifest is being reused, unless inside
    /// [`VersionSetBuilder::finish_try_scope`], in which case this field is always `None`.
    ///
    /// If the old manifest is not being reused, a new `MANIFEST` file must be created and written
    /// to, including the application of any `recovery_edit`.
    reused_manifest:              Option<(WriteLogWriter<File>, FileNumber)>,

    current_version:              CurrentVersion<Refcounted>,
    compaction_pointers:          [OptionalCompactionPointer; NUM_LEVELS_USIZE],

    /// Verify that the file metadata of the recovered version set is plausible: nonzero levels
    /// should have disjoint files.
    verify_recovered_version_set: bool,
    /// If nonempty, the current `MANIFEST` file (whether reused or new) must be written to,
    /// to record both these new `added_files` and an updated `min_log_number` and
    /// `prev_log_number`.
    added_table_files:            Vec<(Level, RefcountedFileMetadata<Refcounted>)>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily, File> VersionSetBuilder<Refcounted, File, false> {
    /// Recover the `MANIFEST` file, and prepare to determine which `.log` files must be recovered.
    pub fn begin_recovery<FS, Cmp>(
        filesystem:                  &mut FS,
        db_directory:                &Path,
        cmp:                         &InternalComparator<Cmp>,
        db_options:                  InnerDBOptions,
    ) -> Result<Self, ()>
    where
        FS:  WritableFilesystem<WriteFile = File>,
        Cmp: LevelDBComparator,
    {
        let current_path = LevelDBFileName::Current.file_path(db_directory);

        let mut current = filesystem.open_sequential(&current_path).map_err(|_| ())?;
        let mut manifest_name = String::new();
        current.read_to_string(&mut manifest_name).map_err(|_| ())?;

        if manifest_name.pop().is_none_or(|last_char| last_char != '\n') {
            // CURRENT should end in a newline
            return Err(());
        }
        drop(current);

        let manifest_path = db_directory.join(&manifest_name);
        let manifest_file = filesystem.open_sequential(&manifest_path).map_err(|_| ())?;

        let mut records_read = 0;
        let mut compaction_pointers = Default::default();

       let mut recovered_manifest = RecoveredManifest::<Refcounted>::recover(
            db_options.seek_options,
            cmp.0.name(),
            manifest_file,
            &mut records_read,
            &mut compaction_pointers,
        ).map_err(|_| ())?;

        let recovered_version = recovered_manifest.builder.finish(
            cmp,
            db_options.verify_recovered_version_set,
        )?;
        let reused_manifest = try_reuse_manifest(
            filesystem,
            db_options.try_reuse_manifest,
            db_options.file_size_limit,
            recovered_manifest.incomplete_final_record,
            &manifest_name,
            &manifest_path,
        );

        Ok(Self {
            min_log_number:               recovered_manifest.min_log_number,
            prev_log_number:              recovered_manifest.prev_log_number,
            next_file_number:             recovered_manifest.next_file_number,
            last_sequence:                recovered_manifest.last_sequence,
            reused_manifest,
            current_version:              CurrentVersion::new(recovered_version),
            compaction_pointers,
            verify_recovered_version_set: db_options.verify_recovered_version_set,
            added_table_files:            Vec::new(),
        })
    }

    #[must_use]
    pub fn expected_table_files(&self) -> HashSet<FileNumber> {
        let num_expected = Level::all_levels().map(|level| {
            self.current_version.level_files(level).inner().len()
        }).sum();
        let mut expected_files = HashSet::with_capacity(num_expected);

        for level in Level::all_levels() {
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
    /// written before
    pub fn mark_file_used(&mut self, file_number: FileNumber) -> Result<(), OutOfFileNumbers> {
        if self.next_file_number <= file_number {
            self.next_file_number = file_number.next()?;
        }
        Ok(())
    }

    /// Finish determining which `.log` files should be recovered, and begin
    #[must_use]
    pub fn finish_listing_old_logs(self) -> VersionSetBuilder<Refcounted, File, true> {
        VersionSetBuilder {
            min_log_number:               self.min_log_number,
            prev_log_number:              self.prev_log_number,
            next_file_number:             self.next_file_number,
            last_sequence:                self.last_sequence,
            reused_manifest:              self.reused_manifest,
            current_version:              self.current_version,
            compaction_pointers:          self.compaction_pointers,
            verify_recovered_version_set: self.verify_recovered_version_set,
            added_table_files:            self.added_table_files,
        }
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily, File> VersionSetBuilder<Refcounted, File, true> {
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

    /// Record the successful creation of a new table file, compacted from some or all of the data
    /// in old `.log` files.
    ///
    /// The table file is placed into level 0.
    pub fn add_new_table_file(&mut self, file_metadata: FileMetadata) {
        self.added_table_files.push((
            Level::ZERO,
            Refcounted::Container::new_container(file_metadata),
        ));
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
    pub fn finish<FS, Cmp>(
        mut self,
        filesystem:         &mut FS,
        db_directory:       &Path,
        cmp:                &InternalComparator<Cmp>,
        current_log_number: FileNumber,
    ) -> Result<VersionSet<Refcounted, File>, ()>
    where
        FS:   WritableFilesystem<WriteFile = File>,
        Cmp:  LevelDBComparator,
        File: WritableFile,
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

                Ok(VersionSet::new(BuildVersionSet {
                    current_log_number:   self.min_log_number,
                    prev_log_number:      self.prev_log_number,
                    next_file_number:     self.next_file_number,
                    last_sequence:        self.last_sequence,
                    manifest_file_number,
                    manifest_writer,
                    edit_record_buffer:   Vec::new(),
                    current_version:      self.current_version,
                    compaction_pointers:  self.compaction_pointers,
                }))
            } else {
                // We need to issue a `MANIFEST` write, but need not write the base version.
                self.finish_with_manifest_write(
                    filesystem,
                    db_directory,
                    cmp,
                    manifest_writer,
                    manifest_file_number,
                    None,
                )
            }
        } else {
            // We need to issue a `MANIFEST` write, including writing the base version.
            let file_number = self.new_file_number().map_err(|_| ())?;
            let manifest_name = LevelDBFileName::Manifest { file_number }.file_name();
            let manifest_path = db_directory.join(&manifest_name);
            let manifest_file = filesystem.open_writable(&manifest_path, false).map_err(|_| ())?;

            self.finish_with_manifest_write(
                filesystem,
                db_directory,
                cmp,
                WriteLogWriter::new_empty(manifest_file),
                file_number,
                Some(&manifest_name),
            ).map_err(|_error| {
                // Try to clean up the now-pointless manifest file. No worries if that fails,
                // the next time that file is opened, it'll be with `open_writable` not
                // `open_appendable`, so no corruption can occur.
                // Also, any leftover file will eventually be garbage-collected.
                let _err = filesystem.delete(&manifest_path);
            })
        }
    }

    /// We created a new manifest file iff `new_manifest_name` is `Some`.
    ///
    /// This function should only be called from [`Self::finish`], after
    /// `self.min_log_number` and `self.prev_log_number` have been updated.
    fn finish_with_manifest_write<FS, Cmp>(
        mut self,
        filesystem:           &mut FS,
        db_directory:         &Path,
        cmp:                  &InternalComparator<Cmp>,
        mut manifest_writer:  WriteLogWriter<File>,
        manifest_file_number: FileNumber,
        new_manifest_name:    Option<&str>,
    ) -> Result<VersionSet<Refcounted, File>, ()>
    where
        FS:   WritableFilesystem<WriteFile = File>,
        Cmp:  LevelDBComparator,
        File: WritableFile,
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
            self.current_version.refcounted_version().mirrored_clone(),
            &mut self.compaction_pointers,
        );
        builder.apply(&edit);
        let built_version = CurrentVersion::new(
            builder.finish(cmp, self.verify_recovered_version_set)?,
        );

        let mut edit_record_buffer = Vec::new();

        if new_manifest_name.is_some() {
            // Note that if `edit` had any compaction pointers, `write_base_version(..)`
            // and `edit.encode(..)` below would together record the same compaction pointers twice.
            // Since the pointers contain arbitrary bytes from user keys, that clone should be
            // avoided with `edit.compaction_pointers.clear()`; however, `edit.compaction_pointers`
            // is already empty.
            write_base_version(
                cmp.0.name(),
                &self.current_version,
                &self.compaction_pointers,
                &mut manifest_writer,
                &mut edit_record_buffer,
            ).map_err(|_| ())?;
            // Clear the record buffer; `write_base_version` does not itself clear the buffer
            // it's given.
            // Note that we might NOT clear the buffer if we return early due to an error;
            // that's fine, since the buffer only escapes this function if it returns successfully.
            edit_record_buffer.clear();
        }

        edit.encode(&mut edit_record_buffer);
        manifest_writer.add_record(&edit_record_buffer).map_err(|_| ())?;
        edit_record_buffer.clear();

        if let Some(manifest_name) = new_manifest_name {
            set_current(filesystem, db_directory, manifest_file_number, manifest_name)
                .map_err(|_| ())?;
        }

        Ok(VersionSet::new(BuildVersionSet {
            current_log_number:  self.min_log_number,
            prev_log_number:     self.prev_log_number,
            next_file_number:    self.next_file_number,
            last_sequence:       self.last_sequence,
            manifest_file_number,
            manifest_writer,
            edit_record_buffer,
            current_version:     built_version,
            compaction_pointers: self.compaction_pointers,
        }))
    }
}

impl<Refcounted: RefcountedFamily, File, const ALL_OLD_LOGS_FOUND: bool> Debug
for VersionSetBuilder<Refcounted, File, ALL_OLD_LOGS_FOUND>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let added_table_files = DebugAddedFiles::<Refcounted>::new(&self.added_table_files);

        f.debug_struct("VersionSetBuilder")
            .field("min_log_number",               &self.min_log_number)
            .field("prev_log_number",              &self.prev_log_number)
            .field("next_file_number",             &self.next_file_number)
            .field("last_sequence",                &self.last_sequence)
            .field("reused_manifest",              &self.reused_manifest)
            .field("current_version",              &self.current_version)
            .field("compaction_pointers",          &self.compaction_pointers)
            .field("verify_recovered_version_set", &self.verify_recovered_version_set)
            .field("added_table_files",            &added_table_files)
            .finish()
    }
}

struct RecoveredManifest<'a, Refcounted: RefcountedFamily> {
    min_log_number:          FileNumber,
    prev_log_number:         FileNumber,
    next_file_number:        FileNumber,
    last_sequence:           SequenceNumber,
    builder:                 VersionBuilder<'a, Refcounted>,
    incomplete_final_record: bool,
}

impl<'a, Refcounted: RefcountedFamily> RecoveredManifest<'a, Refcounted> {
    fn recover<ReadFile: Read>(
        seek_opts:           SeeksBetweenCompactionOptions,
        cmp_name:            &[u8],
        manifest_file:       ReadFile,
        records_read:        &mut u32,
        compaction_pointers: &'a mut [OptionalCompactionPointer; NUM_LEVELS_USIZE],
    ) -> Result<Self, ()> {
        let mut min_log_number = None;
        let mut prev_log_number = None;
        let mut next_file_number = None;
        let mut last_sequence = None;

        let mut builder = VersionBuilder::new(
            Refcounted::Container::new_container(Version::new_empty()),
            compaction_pointers,
        );

        let error = Cell::new(None);
        let mut incomplete_final_record = false;

        let mut manifest_reader = WriteLogReader::new(manifest_file, |bytes_dropped, cause| {
            error.set(Some(()));
        });

        // Morally a while-loop, but with a very complicated condition in the first few lines
        loop {
            let record = match manifest_reader.read_record() {
                ReadRecord::Record { data, .. } => data,
                ReadRecord::IncompleteRecord => {
                    incomplete_final_record = true;
                    break;
                }
                ReadRecord::EndOfFile => break,
            };
            if let Some(error) = error.take() {
                return Err(error);
            }

            *records_read += 1;
            let edit = VersionEdit::<Refcounted>::decode_from(record, seek_opts)?;
            if edit.comparator_name.as_ref()
                .is_some_and(|edit_cmp_name| edit_cmp_name != cmp_name)
            {
                return Err(());
            }

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

        let min_log_number = min_log_number.ok_or(())?;
        let prev_log_number = prev_log_number.unwrap_or(FileNumber(0));
        let mut next_file_number = next_file_number.ok_or(())?;
        let last_sequence = last_sequence.ok_or(())?;

        if next_file_number <= min_log_number {
            next_file_number = min_log_number.next().map_err(|_| ())?;
        }
        if next_file_number <= prev_log_number {
            next_file_number = prev_log_number.next().map_err(|_| ())?;
        }

        Ok(Self {
            min_log_number,
            prev_log_number,
            next_file_number,
            last_sequence,
            builder,
            incomplete_final_record,
        })
    }
}

impl<Refcounted: RefcountedFamily> Debug for RecoveredManifest<'_, Refcounted> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("RecoveredManifest")
            .field("min_log_number",          &self.min_log_number)
            .field("prev_log_number",         &self.prev_log_number)
            .field("next_file_number",        &self.next_file_number)
            .field("last_sequence",           &self.last_sequence)
            .field("builder",                 &self.builder)
            .field("incomplete_final_record", &self.incomplete_final_record)
            .finish()
    }
}

#[must_use]
#[expect(clippy::fn_params_excessive_bools, reason = "internal function, called once")]
fn try_reuse_manifest<FS: WritableFilesystem>(
    filesystem:              &mut FS,
    try_reuse_manifest:      bool,
    file_size_limit:         u64,
    incomplete_final_record: bool,
    manifest_name:           &str,
    manifest_path:           &Path,
) -> Option<(WriteLogWriter<FS::WriteFile>, FileNumber)> {
    if !try_reuse_manifest || incomplete_final_record {
        return None;
    }

    let LevelDBFileName::Manifest { file_number } = LevelDBFileName::parse(manifest_name)? else {
        return None;
    };

    let manifest_size = filesystem.size_of(manifest_path).ok()?;
    if manifest_size >= file_size_limit {
        return None;
    }

    let manifest_file = filesystem.open_appendable(manifest_path, false)
        .inspect_err(|_err| {
            // TODO: log error
        }).ok()?;

    Some((
        WriteLogWriter::new_with_offset(manifest_file, manifest_size),
        file_number,
    ))
}

/// The input `edit_record_buffer` must be empty. Its contents after the function returns are
/// unspecified.
fn write_base_version<Refcounted: RefcountedFamily, File: WritableFile>(
    cmp_name:            &[u8],
    current_version:     &CurrentVersion<Refcounted>,
    compaction_pointers: &[OptionalCompactionPointer; NUM_LEVELS_USIZE],
    manifest_writer:     &mut WriteLogWriter<File>,
    edit_record_buffer:  &mut Vec<u8>,
) -> Result<(), LogWriteError> {
    let mut edit = VersionEdit::<Refcounted>::new_empty();
    edit.comparator_name = Some(cmp_name.to_vec());

    edit.compaction_pointers.reserve(NUM_LEVELS_USIZE);
    for (level, compaction_pointer) in compaction_pointers.enumerated_iter() {
        if let Some(pointer) = compaction_pointer.internal_key() {
            // Note that `CompactionPointer::new` performs an allocation.
            // The allocation could be avoided by, for instance, using `Cow`s in CompactionPointer
            // and adding a lifetime to VersionEdit, or by making a specialized function
            // for adding an encoded version edit with just the fields used and available here.
            // TODO(opt): consider avoiding this allocation.
            edit.compaction_pointers.push((level, CompactionPointer::new(pointer)));
        }
    }

    for level in Level::all_levels() {
        let level_files: &[RefcountedFileMetadata<Refcounted>] = current_version
            .level_files(level)
            .inner();

        edit.added_files.reserve(level_files.len());
        for file in level_files {
            edit.added_files.push((level, file.mirrored_clone()));
        }
    }

    edit.encode(edit_record_buffer);
    manifest_writer.add_record(edit_record_buffer)
}

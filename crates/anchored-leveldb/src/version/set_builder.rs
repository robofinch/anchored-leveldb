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
    file_tracking::{IndexLevel as _, Level, RefcountedFileMetadata, SeeksBetweenCompactionOptions},
    format::{FileNumber, OutOfFileNumbers, SequenceNumber},
    table_traits::{adapters::InternalComparator, trait_equivalents::LevelDBComparator},
    write_log::{LogWriteError, ReadRecord, WriteLogReader, WriteLogWriter},
};

use super::{
    edit::VersionEdit,
    set::VersionSet,
    version_builder::VersionBuilder,
    version_struct::Version,
    version_tracking::CurrentVersion,
};


/// The data necessary to create a [`VersionSet`].
///
/// [`VersionSet`]: super::version_set::VersionSet
pub(super) struct BuildVersionSet<Refcounted: RefcountedFamily, File> {
    pub log_number:           FileNumber,
    /// The file number of the previous log is no longer used, but is still tracked as older
    /// versions of LevelDB might read it.
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
            .field("log_number",           &self.log_number)
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
/// [`VersionSet`]: super::version_set::VersionSet
pub(crate) struct VersionSetBuilder<Refcounted: RefcountedFamily, File> {
    log_number:                   FileNumber,
    /// The file number of the previous log is no longer used, but is still tracked as older
    /// versions of LevelDB might read it.
    prev_log_number:              FileNumber,
    next_file_number:             FileNumber,
    last_sequence:                SequenceNumber,

    /// `Some` if and only if the old manifest is being reused, unless inside
    /// [`VersionSetBuilder::finish_try_scope`], in which case this field is always `None`.
    reused_manifest:              Option<(WriteLogWriter<File>, FileNumber)>,

    current_version:              CurrentVersion<Refcounted>,
    compaction_pointers:          [OptionalCompactionPointer; NUM_LEVELS_USIZE],

    verify_recovered_version_set: bool,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily, File> VersionSetBuilder<Refcounted, File> {
    pub fn begin_recovery<FS, Cmp>(
        filesystem:                  &mut FS,
        db_directory:                &Path,
        cmp:                         &InternalComparator<Cmp>,
        db_options:                  InnerDBOptions,
    ) -> Result<Self, ()>
    where
        FS:  WritableFilesystem<AppendFile = File>,
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
            log_number:           recovered_manifest.log_number,
            prev_log_number:      recovered_manifest.prev_log_number,
            next_file_number:     recovered_manifest.next_file_number,
            last_sequence:        recovered_manifest.last_sequence,
            reused_manifest,
            current_version:      CurrentVersion::new(recovered_version),
            compaction_pointers,
            verify_recovered_version_set: db_options.verify_recovered_version_set,
        })
    }

    #[must_use]
    pub fn expected_files(&self) -> HashSet<FileNumber> {
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

    #[must_use]
    pub const fn log_number(&self) -> FileNumber {
        self.log_number
    }

    #[must_use]
    pub const fn prev_log_number(&self) -> FileNumber {
        self.prev_log_number
    }

    /// This function ***should not*** be called after [`Self::new_file_number`].
    /// The caller would almost certainly face logical correctness issues.
    pub fn mark_file_used(&mut self, file_number: FileNumber) -> Result<(), OutOfFileNumbers> {
        if self.next_file_number <= file_number {
            self.next_file_number = file_number.next()?;
        }
        Ok(())
    }

    pub fn mark_sequence_used(&mut self, sequence_number: SequenceNumber) {
        self.last_sequence = self.last_sequence.max(sequence_number);
    }

    pub fn new_file_number(&mut self) -> Result<FileNumber, OutOfFileNumbers> {
        let new_file_number = self.next_file_number;
        self.next_file_number = self.next_file_number.next()?;
        Ok(new_file_number)
    }

    pub fn finish<FS, Cmp>(
        mut self,
        filesystem:                  &mut FS,
        db_directory:                &Path,
        cmp:                         &InternalComparator<Cmp>,
        edit:                        VersionEdit<Refcounted>,
    ) -> Result<VersionSet<Refcounted, File>, ()>
    where
        FS:   WritableFilesystem<WriteFile = File, AppendFile = File>,
        Cmp:  LevelDBComparator,
        File: WritableFile,
    {
        let (
            manifest_writer,
            manifest_file_number,
            new_manifest_name,
            new_manifest_path,
        ) = if let Some((writer, file_number)) = self.reused_manifest.take() {
            (writer, file_number, None, None)
        } else {
            let file_number = self.new_file_number().map_err(|_| ())?;
            let manifest_name = LevelDBFileName::Manifest { file_number }.file_name();
            let manifest_path = db_directory.join(&manifest_name);
            let manifest_file = filesystem.open_writable(&manifest_path, false).map_err(|_| ())?;
            (
                WriteLogWriter::new_empty(manifest_file),
                file_number,
                Some(manifest_name),
                Some(manifest_path),
            )
        };

        self.finish_try_scope(
            filesystem,
            db_directory,
            cmp,
            edit,
            manifest_writer,
            manifest_file_number,
            new_manifest_name.as_deref(),
        ).map_err(|_error| {
            if let Some(new_manifest_path) = new_manifest_path {
                // Try to clean up the now-pointless manifest file. No worries if that fails,
                // the next time that file is opened, it'll be with `open_writable` not
                // `open_appendable`, so no corruption can occur.
                // Also, any leftover file will eventually be garbage-collected.
                let _err = filesystem.delete(&new_manifest_path);
            }
        })
    }

    #[expect(clippy::too_many_arguments, reason = "internal helper function")]
    fn finish_try_scope<FS, Cmp>(
        mut self,
        filesystem:                  &mut FS,
        db_directory:                &Path,
        cmp:                         &InternalComparator<Cmp>,
        mut edit:                    VersionEdit<Refcounted>,
        mut manifest_writer:         WriteLogWriter<File>,
        manifest_file_number:        FileNumber,
        new_manifest_name:           Option<&str>,
    ) -> Result<VersionSet<Refcounted, File>, ()>
    where
        FS:   WritableFilesystem<WriteFile = File, AppendFile = File>,
        Cmp:  LevelDBComparator,
        File: WritableFile,
    {
        // Ensure that the `VersionEdit` has at least these fields
        let log_number        = *edit.log_number.get_or_insert(self.log_number);
        let prev_log_number   = *edit.prev_log_number.get_or_insert(self.prev_log_number);
        edit.next_file_number = Some(self.next_file_number);
        edit.last_sequence    = Some(self.last_sequence);

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
            // Note that `self.compaction_pointers` was mutated above. It's not truly critical to
            // preserve the original compaction pointers. Plus, the new version would not be
            // recorded in CURRENT unless the subsequent VersionEdit record was also successfully
            // recorded (which would also update the compaction pointers).
            // Therefore, we actually *clear* the version edit's compaction pointers if making a
            // new version. The compaction pointers contain arbitrary bytes from user keys; might
            // as well avoid a useless clone.
            write_base_version(
                cmp.0.name(),
                &self.current_version,
                &self.compaction_pointers,
                &mut manifest_writer,
                &mut edit_record_buffer,
            ).map_err(|_| ())?;
            edit.compaction_pointers.clear();
            // Clear the record buffer; `edit.encode` does not itself clear the buffer it's given.
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
            log_number,
            prev_log_number,
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

impl<Refcounted: RefcountedFamily, File> Debug for VersionSetBuilder<Refcounted, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("VersionSetBuilder")
            .field("log_number",                   &self.log_number)
            .field("prev_log_number",              &self.prev_log_number)
            .field("next_file_number",             &self.next_file_number)
            .field("last_sequence",                &self.last_sequence)
            .field("reused_manifest",              &self.reused_manifest)
            .field("current_version",              &self.current_version)
            .field("compaction_pointers",          &self.compaction_pointers)
            .field("verify_recovered_version_set", &self.verify_recovered_version_set)
            .finish()
    }
}

struct RecoveredManifest<'a, Refcounted: RefcountedFamily> {
    log_number:              FileNumber,
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
        let mut log_number = None;
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
            if error.get().is_some() {
                break;
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
                log_number = edit.log_number;
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

        if let Some(error) = error.get() {
            return Err(error);
        }

        let log_number = log_number.ok_or(())?;
        let prev_log_number = prev_log_number.unwrap_or(FileNumber(0));
        let mut next_file_number = next_file_number.ok_or(())?;
        let last_sequence = last_sequence.ok_or(())?;

        if next_file_number <= log_number {
            next_file_number = log_number.next().map_err(|_| ())?;
        }
        if next_file_number <= prev_log_number {
            next_file_number = prev_log_number.next().map_err(|_| ())?;
        }

        Ok(Self {
            log_number,
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
            .field("log_number",              &self.log_number)
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
) -> Option<(WriteLogWriter<FS::AppendFile>, FileNumber)> {
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

/// `edit_record_buffer` must be empty. Its contents after the function returns are unspecified.
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

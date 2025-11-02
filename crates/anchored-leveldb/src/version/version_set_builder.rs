use std::{cell::Cell, collections::HashSet, io::Read, path::Path};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use generic_container::FragileTryContainer as _;

use anchored_vfs::traits::WritableFilesystem;

use crate::{compaction::OptionalCompactionPointer, containers::RefcountedFamily};
use crate::{
    file_tracking::{Level, SeeksBetweenCompactionOptions},
    format::{FileNumber, LevelDBFileName, NUM_LEVELS_USIZE, OutOfFileNumbers, SequenceNumber},
    table_traits::{adapters::InternalComparator, trait_equivalents::LevelDBComparator},
    write_log::{ReadRecord, WriteLogReader, WriteLogWriter},
};

use super::{version_builder::VersionBuilder, version_edit::VersionEdit};
use super::version_struct::{CurrentVersion, Version};


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
    log_number:           FileNumber,
    /// The file number of the previous log is no longer used, but is still tracked as older
    /// versions of LevelDB might read it.
    prev_log_number:      FileNumber,
    next_file_number:     FileNumber,
    last_sequence:        SequenceNumber,

    /// `Some` if and only if the old manifest is being reused
    reused_manifest:      Option<(WriteLogWriter<File>, FileNumber)>,

    current_version:      CurrentVersion<Refcounted>,
    compaction_pointers:  [OptionalCompactionPointer; NUM_LEVELS_USIZE],
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily, File> VersionSetBuilder<Refcounted, File> {
    pub fn begin_recovery<FS, Cmp>(
        filesystem:     &mut FS,
        db_directory:   &Path,
        cmp:            &InternalComparator<Cmp>,
        seek_opts:      SeeksBetweenCompactionOptions,
        reuse_manifest: bool,
        max_file_size:  u64,
    ) -> Result<Self, ()>
    where
        FS:  WritableFilesystem<AppendFile = File>,
        Cmp: LevelDBComparator,
    {
        #![expect(clippy::similar_names, reason = "`reuse_manifest` vs `reused_manifest`")]

        let current_filename = LevelDBFileName::Current.file_name();
        let current_path = db_directory.join(current_filename);

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
            seek_opts,
            cmp.0.name(),
            manifest_file,
            &mut records_read,
            &mut compaction_pointers,
        ).map_err(|_| ())?;

        let recovered_version = CurrentVersion::new(recovered_manifest.builder.finish(cmp)?);

        let reused_manifest = try_reuse_manifest(
            filesystem,
            reuse_manifest,
            max_file_size,
            recovered_manifest.incomplete_final_record,
            manifest_name.as_ref(),
            &manifest_path,
        );

        Ok(Self {
            log_number:           recovered_manifest.log_number,
            prev_log_number:      recovered_manifest.prev_log_number,
            next_file_number:     recovered_manifest.next_file_number,
            last_sequence:        recovered_manifest.last_sequence,
            reused_manifest,
            current_version:      recovered_version,
            compaction_pointers,
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
                    .iter().map(|file_metadata| file_metadata.file_number())
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

    // #[must_use]
    // pub fn finish<FS>(self) -> VersionSet<Refcounted, File>
    // where
    //     FS: WritableFilesystem<WriteFile = File, AppendFile = File>,
    // {
    //     todo!()
    // }
}

impl<Refcounted: RefcountedFamily, File> Debug for VersionSetBuilder<Refcounted, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("VersionSetBuilder")
            .field("log_number",          &self.log_number)
            .field("prev_log_number",     &self.prev_log_number)
            .field("next_file_number",    &self.next_file_number)
            .field("last_sequence",       &self.last_sequence)
            .field("reused_manifest",     &self.reused_manifest)
            .field("current_version",     &self.current_version)
            .field("compaction_pointers", &self.compaction_pointers)
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
    reuse_manifest:          bool,
    max_file_size:           u64,
    incomplete_final_record: bool,
    manifest_name:           &Path,
    manifest_path:           &Path,
) -> Option<(WriteLogWriter<FS::AppendFile>, FileNumber)> {
    if !reuse_manifest || incomplete_final_record {
        return None;
    }

    let LevelDBFileName::Manifest { file_number } = LevelDBFileName::parse(manifest_name)? else {
        return None;
    };

    let manifest_size = filesystem.size_of(manifest_path).ok()?;
    if manifest_size >= max_file_size {
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

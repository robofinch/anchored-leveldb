use std::mem;
use std::{collections::HashSet, io::Error as IoError};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::MirroredClone as _;
use thiserror::Error;

use anchored_vfs::traits::WritableFile;

use crate::{
    config_constants::NUM_LEVELS_USIZE,
    containers::RefcountedFamily,
    compaction::OptionalCompactionPointer,
};
use crate::{
    file_tracking::{Level, StartSeekCompaction},
    format::{FileNumber, OutOfFileNumbers, SequenceNumber},
    table_traits::{adapters::InternalComparator, trait_equivalents::LevelDBComparator},
    write_log::{LogWriteError, WriteLogWriter},
};

use super::{
    edit::VersionEdit,
    set_builder::BuildVersionSet,
    version_builder::VersionBuilder,
};
use super::{
    version_struct::{RefcountedVersion, Version},
    version_tracking::{CurrentVersion, NeedsSeekCompaction, OldVersions}
};


pub(crate) struct VersionSet<Refcounted: RefcountedFamily, File> {
    log_number:           FileNumber,
    /// The file number of the previous log is no longer used, but is still tracked as older
    /// versions of LevelDB might read it.
    prev_log_number:      FileNumber,
    next_file_number:     FileNumber,
    last_sequence:        SequenceNumber,

    manifest_file_number: FileNumber,
    /// Should always be `Some`, except when executing apply->log->install.
    ///
    /// The "apply" step temporarily takes out the `manifest_writer` and `edit_record_buffer` fields
    /// for use in the "log" step, and the fields are restored in the "install" step.
    manifest_writer:      Option<WriteLogWriter<File>>,
    /// Should always be empty, except transiently inside functions. Used solely for its capacity.
    edit_record_buffer:   Vec<u8>,

    current_version:      CurrentVersion<Refcounted>,
    old_versions:         OldVersions<Refcounted>,

    compaction_pointers:  [OptionalCompactionPointer; NUM_LEVELS_USIZE],
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily, File: WritableFile> VersionSet<Refcounted, File> {
    #[must_use]
    pub(super) fn new(build_version: BuildVersionSet<Refcounted, File>) -> Self {
        // Make sure that no field of `BuildVersionSet` is forgotten
        let BuildVersionSet {
            log_number,
            prev_log_number,
            next_file_number,
            last_sequence,
            manifest_file_number,
            manifest_writer,
            edit_record_buffer,
            current_version,
            compaction_pointers,
        } = build_version;

        Self {
            log_number,
            prev_log_number,
            next_file_number,
            last_sequence,
            manifest_file_number,
            manifest_writer: Some(manifest_writer),
            edit_record_buffer,
            current_version,
            old_versions:    OldVersions::new(),
            compaction_pointers,
        }
    }

    /// The process of logging a [`VersionEdit`] to a MANIFEST file is broken into three steps:
    /// [`VersionSet::apply`], then [`VersionSet::log_to_manifest`], and lastly
    /// [`VersionSet::install`].
    ///
    /// An error occurring during the middle step should be considered fatal for writes,
    /// including this apply->log->install process. Reads may still be performed, but the database
    /// must be closed and reopened before writes _may_ be permissible. If an fsync error occurred,
    /// the manifest file must not be reused.
    ///
    /// # Panics
    /// No mutex needs to be held during the middle step, but _only one thread_ should even
    /// **attempt** to call apply->log->install at any given time. Failing to meet that requirement
    /// can result in a panic or database corruption.
    ///
    /// If the apply->log->install process is attempted after a past call to
    /// [`VersionSet::log_to_manifest`] failed, a panic will occur.
    pub fn apply<'a, Cmp: LevelDBComparator>(
        &mut self,
        cmp:  &InternalComparator<Cmp>,
        edit: &'a mut VersionEdit<Refcounted>,
    ) -> Result<LogToken<'a, Refcounted, File>, ()> {
        // NOTE: we do NOT return early from this function without restoring `self.manifest_writer`.
        #[expect(clippy::expect_used, reason = "only a bug could trigger the documented panic")]
        let manifest_writer = self.manifest_writer.take()
            .expect("VersionSet's apply->log->install process must be strictly synchronized");

        // Ensure that the `VersionEdit` has at least these fields
        edit.log_number.get_or_insert(self.log_number);
        edit.prev_log_number.get_or_insert(self.prev_log_number);
        edit.next_file_number = Some(self.next_file_number);
        edit.last_sequence = Some(self.last_sequence);

        let mut builder = VersionBuilder::new(
            self.current_version.refcounted_version().mirrored_clone(),
            &mut self.compaction_pointers,
        );
        builder.apply(edit);

        // Can't use `map_err`, since the closure would move `manifest_writer`.
        let new_version = match builder.finish(cmp, false) {
            Ok(new_version) => new_version,
            Err(error) => {
                self.manifest_writer = Some(manifest_writer);
                return Err(error);
            }
        };

        Ok(LogToken {
            version_edit:       edit,
            manifest_writer,
            new_version,
            edit_record_buffer: mem::take(&mut self.edit_record_buffer),
        })
    }

    /// No mutex needs to be held during this step, but the entire apply->log->install process
    /// must be strictly synchronized such that only one thread is performing the process at a
    /// given time.
    ///
    /// # Errors
    /// An error occurring during this step should be considered fatal for writes,
    /// including this apply->log->install process. Reads may still be performed, but the database
    /// must be closed and reopened before writes _may_ be permissible. If an fsync error occurred,
    /// the manifest file must not be reused.
    pub fn log_to_manifest(
        mut token: LogToken<'_, Refcounted, File>,
    ) -> Result<InstallToken<'_, Refcounted, File>, ManifestLogError> {
        token.version_edit.encode(&mut token.edit_record_buffer);
        let result = token.manifest_writer.add_record(&token.edit_record_buffer);
        token.edit_record_buffer.clear();
        result?;

        token.manifest_writer.sync_log_data().map_err(ManifestLogError::FsyncData)?;

        Ok(InstallToken {
            version_edit:       token.version_edit,
            manifest_writer:    token.manifest_writer,
            new_version:        token.new_version,
            edit_record_buffer: token.edit_record_buffer,
        })
    }

    pub fn install(
        &mut self,
        token: InstallToken<'_, Refcounted, File>,
    ) {
        self.manifest_writer    = Some(token.manifest_writer);
        self.edit_record_buffer = token.edit_record_buffer;

        let old_version = self.current_version.set(token.new_version);
        self.old_versions.add_old_version(old_version);

        // See `Self::apply`. These fields are guaranteed to be in the version edit.
        // The version edit is not mutated since then.
        {
            #![expect(clippy::unwrap_used, reason = "they are `Some`, as guaranteed by `apply`")]
            self.log_number = token.version_edit.log_number.unwrap();
            self.prev_log_number = token.version_edit.prev_log_number.unwrap();
        }
    }
}

// TODO: create a free function which does apply->log->install
// on the relevant parts of the DB struct, once there *is* a DB struct

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily, File> VersionSet<Refcounted, File> {
    #[must_use]
    pub const fn log_number(&self) -> FileNumber {
        self.log_number
    }

    #[must_use]
    pub const fn prev_log_number(&self) -> FileNumber {
        self.prev_log_number
    }

    #[must_use]
    pub const fn manifest_file_number(&self) -> FileNumber {
        self.manifest_file_number
    }

    pub fn new_file_number(&mut self) -> Result<FileNumber, OutOfFileNumbers> {
        let new_file_number = self.next_file_number;
        self.next_file_number = self.next_file_number.next()?;
        Ok(new_file_number)
    }

    /// Reuse the given file number if possible.
    ///
    /// If the passed `file_number` is not the newest file number (as returned by the most-recent
    /// call to [`Self::new_file_number`], for instance), nothing happens.
    pub const fn reuse_file_number(&mut self, file_number: FileNumber) {
        if self.next_file_number.0.saturating_sub(1) == file_number.0 {
            // Either `self.next_file_number == file_number` (...which shouldn't happen...)
            // and thus nothing changes, or `file_number` is one before the next file number
            // and was thus the newest in-use file number.
            self.next_file_number = file_number;
        }
    }

    #[must_use]
    pub const fn last_sequence(&self) -> SequenceNumber {
        self.last_sequence
    }

    /// The new sequence number should be at least `self.last_sequence()`.
    pub fn set_last_sequence(&mut self, new_sequence_number: SequenceNumber) {
        debug_assert!(
            self.last_sequence <= new_sequence_number,
            "attempted to set `VersionSet::last_sequence` to an older sequence number",
        );
        self.last_sequence = new_sequence_number;
    }

    #[must_use]
    pub const fn current(&self) -> &CurrentVersion<Refcounted> {
        &self.current_version
    }

    /// Get a reference-counted clone to the current version.
    #[must_use]
    pub fn cloned_current_version(&self) -> RefcountedVersion<Refcounted> {
        self.current_version.refcounted_version().mirrored_clone()
    }

    #[must_use]
    pub fn live_files(&mut self) -> HashSet<FileNumber> {
        let live_old_versions = self.old_versions.live().collect::<Vec<_>>();

        // Slight optimization: calculate the capacity in advance
        let old_live: usize = live_old_versions.iter().flat_map(|version| {
            Level::all_levels().map(|level| version.level_files(level).inner().len())
        }).sum();

        let current_live: usize = Level::all_levels().map(|level| {
            self.current_version.level_files(level).inner().len()
        }).sum();

        let mut live_files = HashSet::with_capacity(old_live + current_live);

        // Add all the live files
        for level in Level::all_levels() {
            live_files.extend(
                self.current_version.level_files(level).inner()
                    .iter().map(|file_metadata| file_metadata.file_number()),
            );
        }

        for version in &live_old_versions {
            for level in Level::all_levels() {
                live_files.extend(
                    version.level_files(level).inner()
                        .iter().map(|file_metadata| file_metadata.file_number()),
                );
            }
        }

        live_files
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily, File> VersionSet<Refcounted, File> {
    #[must_use]
    pub fn needs_seek_compaction(
        &mut self,
        maybe_current_version: &RefcountedVersion<Refcounted>,
        start_seek_compaction: StartSeekCompaction<Refcounted>,
    ) -> NeedsSeekCompaction {
        self.current_version.needs_seek_compaction(maybe_current_version, start_seek_compaction)
    }

    // pub fn pick_compaction(&self) -> Option<Compaction>;

    // pub fn compact_range(&self, level: Level, compactionrange: _) -> Option<Compaction>;

    // fn setup_other_inputs(&mut self, &mut compaction: Compaction);

    // pub fn compaction_inputs(&self, compaction: &Compaction) -> CompactionInputIter;
    // CompactionInputIter: mixture of TableIter and DisjointLevelIter, merged together

    // still need to learn more about how compaction works
}

impl<Refcounted: RefcountedFamily, File> Debug for VersionSet<Refcounted, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("VersionSet")
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
            .field("old_versions",         &self.old_versions)
            .field("compaction_pointers",  &self.compaction_pointers)
            .finish()
    }
}

/// Contains data for [`VersionSet::log_to_manifest`]. Returned by [`VersionSet::apply`].
pub(crate) struct LogToken<'a, Refcounted: RefcountedFamily, File> {
    version_edit:       &'a VersionEdit<Refcounted>,
    manifest_writer:    WriteLogWriter<File>,
    new_version:        Version<Refcounted>,
    edit_record_buffer: Vec<u8>,
}

impl<Refcounted: RefcountedFamily, File> Debug for LogToken<'_, Refcounted, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("LogToken")
            .field("version_edit",       &self.version_edit)
            .field("manifest_writer",    &self.manifest_writer)
            .field("new_version",        &self.new_version)
            .field("edit_record_buffer", &format!(
                "<buffer of length {} and capacity {}>",
                self.edit_record_buffer.len(),
                self.edit_record_buffer.capacity(),
            ))
            .finish()
    }
}

/// Contains data for [`VersionSet::install`]. Returned by [`VersionSet::log_to_manifest`].
pub(crate) struct InstallToken<'a, Refcounted: RefcountedFamily, File>  {
    version_edit:       &'a VersionEdit<Refcounted>,
    manifest_writer:    WriteLogWriter<File>,
    new_version:        Version<Refcounted>,
    edit_record_buffer: Vec<u8>,
}

impl<Refcounted: RefcountedFamily, File> Debug for InstallToken<'_, Refcounted, File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InstallToken")
            .field("version_edit",       &self.version_edit)
            .field("manifest_writer",    &self.manifest_writer)
            .field("new_version",        &self.new_version)
            .field("edit_record_buffer", &format!(
                "<buffer of length {} and capacity {}>",
                self.edit_record_buffer.len(),
                self.edit_record_buffer.capacity(),
            ))
            .finish()
    }
}

#[derive(Error, Debug)]
pub(crate) enum ManifestLogError {
    #[error("error writing to a MANIFEST file: {0}")]
    Write(#[from] LogWriteError),
    #[error("error fsyncing a MANIFEST file: {0}")]
    FsyncData(IoError),
}

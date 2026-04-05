use std::sync::MutexGuard;

use clone_behavior::FastMirroredClone;

use anchored_vfs::{IntoChildFileIterator as _, LevelDBFilesystem};

use crate::{
    database_files::LevelDBFileName,
    file_tracking::FileMetadata,
    memtable::MemtableIter,
    table_file::TableFileBuilder,
};
use crate::{
    all_errors::{
        aliases::RwErrorKindAlias,
        types::{
            AddTableEntryError, OutOfFileNumbers, RwErrorKind, WriteError,
        },
    },
    options::{InternallyMutableOptions, InternalOptions},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{CloseStatus, FileNumber, Level},
};
use super::state::{InternalDBState, SharedMutableState};


#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub fn maybe_start_compaction(
        &self,
        mutable_state: &mut MutexGuard<'_, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
    ) {
        match mutable_state.close_status {
            CloseStatus::Closed | CloseStatus::Closing | CloseStatus::ClosingAfterCompaction => {
                // Do not start a new compaction. Not that `ClosingAfterCompaction` allows
                // ongoing compactions to finish, but does not allow new ones to start.
                return;
            }
            CloseStatus::Open => {}
        }

        let flush = mutable_state.compaction_state.memtable_under_compaction.is_some();
        let manual_compaction = mutable_state.compaction_state.manual_compaction.level.is_some();
        let size_compaction = match mutable_state.version_set.current().size_compaction() {
            Some(Level::ZERO) => self.opts.compaction.size_compactions.autocompact_level_zero,
            Some(_other)      => self.opts.compaction.size_compactions.autocompact_nonzero_levels,
            None              => false,
        };
        // We only even bother to record seeks if
        // `self.opts.compaction.seek_compactions.seek_autocompactions` is enabled, so no need
        // to check that option here.
        let seek_compaction = mutable_state.version_set.current().seek_compaction().is_some();

        let has_compaction_work = flush || manual_compaction || size_compaction || seek_compaction;

        if mutable_state.compaction_state.has_ongoing_compaction {
            // Once the ongoing compaction is complete, it will maybe start another.
        } else if mutable_state.compaction_state.suspending_compactions {
            // Ongoing compactions are permitted to complete, but ongoing ones are not started.
        } else if mutable_state.write_status.is_err() {
            // We are in read-only mode due to a write error or corruption error.
            // No more compactions.
        } else if !has_compaction_work {
            // No compaction work needs to be done.
        } else {
            // Start a compaction.
            mutable_state.compaction_state.has_ongoing_compaction = true;
            if let Some(foreground_compactor) = &mut mutable_state.foreground_compactor {
                // Do compaction on this thread.
            }
            if let Some(background_compactor) = &self.background_compactor {
                // Do compaction in the background.
                background_compactor.start_compaction.notify_one();
            }
        }
    }

    #[expect(clippy::type_complexity, reason = "complaining solely because of the 5 generics")]
    pub fn garbage_collect_files<'a>(
        &self,
        mut mutable_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
    ) -> Option<MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>> {
        if mutable_state.write_status.is_err() {
            // After a write or corruption error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return Some(mutable_state);
        }

        let live_table_files = mutable_state.version_set.live_table_files();
        // Applies to `MANIFEST-` and `.dbtmp` files (and, together with `live_table_files`, to
        // `.ldb` and `.sst` files).
        let is_pending = |file_number| {
            mutable_state.compaction_state.pending_compaction_outputs.contains(&file_number)
        };
        // Applies to `.ldb` and `.sst` files.
        let table_is_live = |file_number| {
            live_table_files.contains(&file_number) || is_pending(file_number)
        };

        let Ok(db_files) = self.mut_opts.filesystem.child_files(&self.opts.db_directory) else {
            // Ignore error; garbage collecting files is not critical.
            return Some(mutable_state);
        };

        let mut files_to_delete = Vec::new();

        for file in db_files.child_files() {
            // Ignore error, as above.
            let Ok((file_name, _file_size)) = file else { continue };
            // All of LevelDB's files' names are ASCII and thus valid UTF-8, so any files
            // with non-UTF-8 names can be ignored.
            let Ok(file_name) = file_name.into_os_string().into_string() else { continue };
            // Only garbage collect LevelDB's files.
            let Some(parsed_name) = LevelDBFileName::parse(&file_name) else { continue };

            match parsed_name {
                LevelDBFileName::Log { file_number } => {
                    if file_number == mutable_state.version_set.prev_log_number()
                        && file_number >= mutable_state.version_set.current_log_number()
                    {
                        // Keep this write-ahead log
                        continue;
                    }
                }
                LevelDBFileName::Table { file_number: table_number }
                | LevelDBFileName::TableLegacyExtension { file_number: table_number }
                    => {
                        if table_is_live(table_number) {
                            // Keep this live table file
                            continue;
                        }
                    }
                LevelDBFileName::Manifest { file_number } => {
                    // Keep this invocation's current manifest file, any newer manifest file being
                    // created by this invocation, and any newer invocations' manifests
                    // (in case there is a race that allows other database invocations).
                    if file_number >= mutable_state.version_set.manifest_file_number() {
                        continue;
                    }
                }
                LevelDBFileName::Temp { file_number } => {
                    // `.dbtmp` files are created while changing the `CURRENT` file, and they are
                    // given file numbers corresponding to `MANIFEST-` files. The file numbers
                    // of pending `MANIFEST-` files are recorded in `pending_outputs`, which we
                    // check here; all other `.dbtmp` files can be deleted.
                    if is_pending(file_number) {
                        continue;
                    }
                }
                LevelDBFileName::Lockfile
                | LevelDBFileName::Current
                | LevelDBFileName::InfoLog
                | LevelDBFileName::OldInfoLog
                    => continue,
            }

            // TODO: log a message about `file_name` being removed.
            files_to_delete.push(file_name);
        }

        // Unblock other threads while deleting files. Even accounting for bugs in Google's leveldb
        // that can allow one file number to be given to two files (of different types), the
        // combination of file types and the file number counter give unique names to LevelDB's
        // files. (Technically, semantically distinct files could have the same name across
        // different database invocations -- say, a writer could crash after creating a file, and
        // a following database invocation could end up overwriting that file -- but the file names
        // are distinct within this invocation of the database.)
        //
        // Therefore, no new files will overwrite existing files, so we can safely delete these
        // existing files without causing a problem.
        drop(mutable_state);
        for file_name in files_to_delete {
            let _ignore_err: Result<(), _> = self.mut_opts
                .filesystem
                .remove_file(&self.opts.db_directory.join(file_name));
        }
        None
    }
}

/// Writes the entries of the memtable to zero or more table files.
///
/// Note that the given memtable iterator is not `reset()`.
///
/// If the memtable iterator is empty, zero table files are used. Otherwise, table files are split
/// **only** when absolutely necessary (for the sake of not overfilling the table's index block),
/// regardless of settings for table file size. (This means that, almost always, at most one table
/// file is used.)
///
/// Note that if the builder was already active, the previous table file would be closed, but
/// it would _not_ be properly finished *or* deleted. That file would be an invalid table file
/// and should eventually be garbage collected by this program.
///
/// This function can be called on a builder at any time (regardless of whether it's active).
/// When this function returns, the builder is [inactive].
///
/// [inactive]: TableFileBuilder::active
#[expect(
    clippy::too_many_arguments,
    reason = "the first five arguments can't easily be conglomerated",
)]
pub(super) fn flush_memtable<FS, Cmp, Policy, Codecs, Pool, F>(
    builder:             &mut TableFileBuilder<FS::WriteFile, Policy, Pool>,
    opts:                &InternalOptions<Cmp, Policy, Codecs>,
    mut_opts:            &InternallyMutableOptions<FS, Policy, Pool>,
    encoders:            &mut Codecs::Encoders,
    decoders:            &mut Codecs::Decoders,
    manifest_number:     FileNumber,
    mut get_file_number: F,
    mut memtable_iter:   MemtableIter<'_, Cmp>,
) -> Result<Vec<FileMetadata>, RwErrorKindAlias<FS, Cmp, Codecs>>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
    F:      FnMut() -> Result<FileNumber, OutOfFileNumbers>,
{
    let mut created_file_metadata = Vec::new();

    while let Some(mut current) = memtable_iter.next() {
        let table_file_number = get_file_number()
            .map_err(|OutOfFileNumbers {}| RwErrorKind::Write(WriteError::OutOfFileNumbers))?;

        builder.start(opts, mut_opts, table_file_number, None).map_err(RwErrorKind::Write)?;

        let smallest_key = current.0;

        // Correctness: the memtable is sorted solely by internal key
        // (in the same way in which `InternalComparator<Cmp>` would sort the internal keys)
        // and does not have any entries with duplicate keys.
        match builder.add_entry(opts, mut_opts, encoders, current.0, current.1) {
            Ok(()) => (),
            // Perhaps it would be ideal to avoid using `unreachable` (in favor of better
            // indicating the possible return values), but this is fine.
            #[expect(
                clippy::unreachable,
                reason = "not worth juggling where the proof of unreachability goes",
            )]
            Err(AddTableEntryError::AddEntryError) => unreachable!(
                "`TableBuilder::add_entry(empty_table, ..)` cannot return `AddEntryError`",
            ),
            Err(AddTableEntryError::Write(err)) => return Err(err),
        }

        let largest_key = loop {
            // Correctness: the memtable is sorted solely by internal key
            // (in the same way in which `InternalComparator<Cmp>` would sort the internal keys)
            // and does not have any entries with duplicate keys.
            match builder.add_entry(opts, mut_opts, encoders, current.0, current.1) {
                Ok(()) => {
                    if let Some(next) = memtable_iter.next() {
                        current = next;
                    } else {
                        break current.0;
                    }
                }
                Err(AddTableEntryError::AddEntryError) => break current.0,
                Err(AddTableEntryError::Write(err)) => return Err(err),
            }
        };

        created_file_metadata.push(builder.finish(
            opts,
            mut_opts,
            encoders,
            decoders,
            manifest_number,
            smallest_key.as_internal_key(),
            largest_key.as_internal_key(),
        )?);
    }

    Ok(created_file_metadata)
}

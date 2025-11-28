use std::cell::Cell;
use std::{
    path::{Path, PathBuf},
    fmt::{Debug, Formatter, Result as FmtResult},
};

use clone_behavior::MirroredClone as _;

use anchored_sstable::perf_options::KVCache;
use anchored_vfs::traits::{
    IntoDirectoryIterator as _,
    ReadableFilesystem as _,
    WritableFilesystem as _,
};

use crate::{
    containers::FragileRwCell as _,
    corruption_handler::InternalCorruptionHandler,
    database_files::LevelDBFileName,
    info_logger::InfoLogger,
    memtable::Memtable,
    table_file::build_table,
};
use crate::{
    format::{FileNumber, SequenceNumber},
    version::{VersionSet, VersionSetBuilder},
    leveldb_generics::{LdbFsCell, LdbPooledBuffer, LdbTableOptions, LdbWriteFile, LevelDBGenerics},
    write_batch::{UnvalidatedWriteBatch, WriteBatch},
    write_log::{ReadRecord, WriteLogReader, WriteLogWriter},
};
use super::{
    fs_guard::FSGuard,
    generic_db::InnerGenericDB,
    write_impl::DBWriteImpl,
    db_data::InnerDBOptions,
};


/// The options provided when opening a [`InnerGenericDB`], aside from the filesystem.
pub(crate) struct InitOptions<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> {
    pub db_directory:       PathBuf,
    pub table_cache:        LDBG::TableCache,
    pub table_options:      LdbTableOptions<LDBG>,
    pub db_options:         InnerDBOptions,
    pub corruption_handler: InternalCorruptionHandler<LDBG::Refcounted, LDBG::RwCell>,
    pub info_logger:        InfoLogger<LdbWriteFile<LDBG>>,
    pub write_impl:         WriteImpl,
}

impl<LDBG, WriteImpl> Debug for InitOptions<LDBG, WriteImpl>
where
    LDBG:                  LevelDBGenerics,
    LDBG::Skiplist:        Debug,
    LDBG::FS:              Debug,
    LDBG::Policy:          Debug,
    LDBG::Cmp:             Debug,
    LDBG::Pool:            Debug,
    LdbPooledBuffer<LDBG>: Debug,
    WriteImpl:             DBWriteImpl<LDBG> + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InitOptions")
            .field("db_directory",       &self.db_directory)
            .field("table_cache",        KVCache::debug(&self.table_cache))
            .field("table_options",      &self.table_options)
            .field("db_options",         &self.db_options)
            .field("corruption_handler", &self.corruption_handler)
            .field("info_logger",        &self.info_logger)
            .field("write_impl",         &self.write_impl)
            .finish()
    }
}

/// The data necessary to create a [`InnerGenericDB`].
///
/// [`InnerGenericDB`]: super::generic_db::InnerGenericDB
pub(super) struct BuildGenericDB<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> {
    pub db_directory:              PathBuf,
    pub filesystem:                FSGuard<LDBG>,
    pub table_cache:               LDBG::TableCache,
    pub table_options:             LdbTableOptions<LDBG>,
    pub db_options:                InnerDBOptions,
    pub corruption_handler:        InternalCorruptionHandler<LDBG::Refcounted, LDBG::RwCell>,
    pub version_set:               VersionSet<LDBG::Refcounted, LdbWriteFile<LDBG>>,
    pub current_memtable:          Memtable<LDBG::Cmp, LDBG::Skiplist>,
    pub current_log:               WriteLogWriter<LdbWriteFile<LDBG>>,
    pub info_logger:               InfoLogger<LdbWriteFile<LDBG>>,
    pub write_impl:                WriteImpl,
}

impl<LDBG, WriteImpl> Debug for BuildGenericDB<LDBG, WriteImpl>
where
    LDBG:                  LevelDBGenerics,
    LDBG::Skiplist:        Debug,
    LDBG::FS:              Debug,
    LDBG::Policy:          Debug,
    LDBG::Cmp:             Debug,
    LDBG::Pool:            Debug,
    LdbPooledBuffer<LDBG>: Debug,
    WriteImpl:             DBWriteImpl<LDBG> + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("BuildGenericDB")
            .field("db_directory",       &self.db_directory)
            .field("filesystem",         &self.filesystem)
            .field("table_cache",        KVCache::debug(&self.table_cache))
            .field("table_options",      &self.table_options)
            .field("db_options",         &self.db_options)
            .field("corruption_handler", &self.corruption_handler)
            .field("version_set",        &self.version_set)
            .field("current_memtable",   &self.current_memtable)
            .field("current_log",        &self.current_log)
            .field("info_logger",        &self.info_logger)
            .field("write_impl",         &self.write_impl)
            .finish()
    }
}

struct ReusedLog<LDBG: LevelDBGenerics> {
    memtable:   Memtable<LDBG::Cmp, LDBG::Skiplist>,
    log:        WriteLogWriter<LdbWriteFile<LDBG>>,
    log_number: FileNumber,
}

impl<LDBG> Debug for ReusedLog<LDBG>
where
    LDBG:                  LevelDBGenerics,
    LDBG::Skiplist:        Debug,
    LDBG::Cmp:             Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ReusedLog")
            .field("memtable",   &self.memtable)
            .field("log",        &self.log)
            .field("log_number", &self.log_number)
            .finish()
    }
}

pub(super) struct InnerGenericDBBuilder<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> {
    db_directory:             PathBuf,
    table_cache:              LDBG::TableCache,
    table_options:            LdbTableOptions<LDBG>,
    db_options:               InnerDBOptions,
    corruption_handler:       InternalCorruptionHandler<LDBG::Refcounted, LDBG::RwCell>,
    reused_log:               Option<ReusedLog<LDBG>>,
    info_logger:              InfoLogger<LdbWriteFile<LDBG>>,
    write_impl:               WriteImpl,
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDBBuilder<LDBG, WriteImpl> {
    /// Recover a LevelDB database which is thought to exist.
    ///
    /// The `LOCK` file must have been acquired and `CURRENT` should exist, though might
    /// not be a file.
    pub(super) fn recover_existing(
        init_options: InitOptions<LDBG, WriteImpl>,
        filesystem:   FSGuard<LDBG>,
    ) -> Result<InnerGenericDB<LDBG, WriteImpl>, ()> {
        let mut fs_lock = filesystem.filesystem.write();

        // Recover the `MANIFEST` file.
        let mut vset_builder = VersionSetBuilder::begin_recovery(
            &mut *fs_lock,
            &init_options.db_directory,
            &init_options.table_options.comparator,
            init_options.db_options,
        )?;

        let mut builder = Self::begin_recovery(init_options);

        // Make sure that all expected table files are present, and figure out which `.log`
        // files to recover.
        let mut log_files = builder.enumerate_files(&*fs_lock, &mut vset_builder)?;
        drop(fs_lock);

        let mut vset_builder = vset_builder.finish_listing_old_logs();

        // Recover the log files in increasing order of their file numbers, so that older
        // log files are recovered first (in the order they were written).
        log_files.sort_unstable();
        let mut log_files = log_files.into_iter();

        // Separate this one out to recover last
        let last_log_number = log_files.next_back();
        // Recover all the non-last log files
        for log_number in log_files {
            builder.recover_log_file(&filesystem.filesystem, &mut vset_builder, log_number, false)?;
        }
        // Recover and maybe reuse the last log
        if let Some(log_number) = last_log_number {
            builder.recover_log_file(&filesystem.filesystem, &mut vset_builder, log_number, true)?;
        }

        builder.finish(filesystem, vset_builder)
    }

    /// Initialize the builder. Has no side effects.
    #[must_use]
    fn begin_recovery(init_options: InitOptions<LDBG, WriteImpl>) -> Self {
        // Make sure no fields are forgotten.
        let InitOptions {
            db_directory,
            table_cache,
            table_options,
            db_options,
            corruption_handler,
            info_logger,
            write_impl,
        } = init_options;

        Self {
            db_directory,
            table_cache,
            table_options,
            db_options,
            corruption_handler,
            reused_log: None,
            info_logger,
            write_impl,
        }
    }

    #[must_use]
    fn cmp(&self) -> LDBG::Cmp {
        self.table_options.comparator.0.fast_mirrored_clone()
    }

    /// Look through every file in the db directory to ensure that all expected table files are
    /// present and determine which `.log` files to recover.
    ///
    /// Returns an unsorted list of all `.log` files which should be recovered.
    fn enumerate_files(
        &self,
        filesystem:   &LDBG::FS,
        vset_builder: &mut VersionSetBuilder<LDBG::Refcounted, LdbWriteFile<LDBG>, false>,
    ) -> Result<Vec<FileNumber>, ()> {
        let mut expected_table_files = vset_builder.expected_table_files();
        let mut log_files = Vec::new();
        let db_files = filesystem.children(&self.db_directory).map_err(|_| ())?;

        for relative_path in db_files.dir_iter() {
            // Note that the relative path should not begin with `/`.
            let relative_path = relative_path.map_err(|_| ())?;
            let Some(maybe_filename) = relative_path.to_str() else { continue };
            let Some(maybe_filename) = LevelDBFileName::parse(maybe_filename) else { continue };

            match maybe_filename {
                LevelDBFileName::Log { file_number } => {
                    if vset_builder.log_should_be_recovered(file_number) {
                        log_files.push(file_number);
                        vset_builder.mark_file_used(file_number).map_err(|_| ())?;
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
            return Err(());
        }

        Ok(log_files)
    }

    fn recover_log_file(
        &mut self,
        filesystem:   &LdbFsCell<LDBG>,
        vset_builder: &mut VersionSetBuilder<LDBG::Refcounted, LdbWriteFile<LDBG>, true>,
        log_number:   FileNumber,
        last_log:     bool,
    ) -> Result<(), ()> {
        // Will be set to `false` below by anything that would prevent reusing the `.log` file
        // from working out.
        let mut try_reuse_log = last_log && self.db_options.try_reuse_write_ahead_log;

        let log_file_path = LevelDBFileName::Log { file_number: log_number }
            .file_path(&self.db_directory);
        let log_file = filesystem.write().open_sequential(&log_file_path).map_err(|_| ())?;

        let error = Cell::new(None);

        let mut log_reader = WriteLogReader::new(log_file, |bytes_dropped, cause| {
            error.set(Some(()));
        });
        let mut memtable = Memtable::new(self.cmp());

        // Morally a while-loop, but with a very complicated condition in the first few lines.
        loop {
            let record = match log_reader.read_record() {
                ReadRecord::Record { data, .. } => data,
                ReadRecord::IncompleteRecord => {
                    // Reusing this log file would risk database corruption.
                    try_reuse_log = false;
                    break;
                }
                ReadRecord::EndOfFile => break,
            };
            if let Some(error) = error.take() {
                return Err(error);
            }

            // TODO: depending on a setting, `continue` instead of erroring out if
            // parsing a write batch fails.
            let parsed_write_batch = parse_write_batch(record)?;
            vset_builder.mark_sequence_used(parsed_write_batch.batch_last_sequence);

            {
                // TODO: create *safe* wrappers for skiplists and memtables that statically ensure
                // that only one copy is written.
                // SAFETY:
                // There's only one handle to this memtable; it was created above, and has not been
                // cloned.
                let mut write_access = unsafe { memtable.externally_synchronized() };

                write_access.insert_write_batch(
                    &parsed_write_batch.batch,
                    parsed_write_batch.last_sequence_before_batch,
                );
            };

            if memtable.allocated_bytes() > self.db_options.memtable_size_limit {
                // Flush the memtable, and reset it for further reads. We can't reuse the log
                // file; the `.log` file is supposed to correspond to a memtable, but we won't
                // have a single memtable corresponding to the whole `.log` file.
                try_reuse_log = false;
                self.flush_memtable(filesystem, vset_builder, &memtable)?;
                memtable = Memtable::new(self.cmp());
            }
        }

        if try_reuse_log {
            let mut fs_lock = filesystem.write();
            if let Some(log) = self.try_reuse_log(&mut *fs_lock, &log_file_path) {
                self.reused_log = Some(ReusedLog {
                    memtable,
                    log,
                    log_number,
                });
                return Ok(());
            }
        }

        // If we get here, we didn't reuse the log.
        self.flush_memtable(filesystem, vset_builder, &memtable)?;
        Ok(())
    }

    fn flush_memtable(
        &self,
        filesystem:   &LdbFsCell<LDBG>,
        vset_builder: &mut VersionSetBuilder<LDBG::Refcounted, LdbWriteFile<LDBG>, true>,
        memtable:     &Memtable<LDBG::Cmp, LDBG::Skiplist>,
    ) -> Result<(), ()> {
        let table_file_number = vset_builder.new_table_file_number().map_err(|_| ())?;

        let table_metadata = build_table::<LDBG>(
            filesystem,
            &self.db_directory,
            &self.table_cache,
            self.table_options.fast_clone(),
            self.db_options.seek_options,
            memtable,
            table_file_number,
        )?;

        // `build_table` returns `Ok(None)` if the memtable is empty, since there's no need
        // to create a table file in that case.
        if let Some(table_metadata) = table_metadata {
            vset_builder.add_new_table_file(table_metadata);
        }

        Ok(())
    }

    fn try_reuse_log(
        &self,
        filesystem:    &mut LDBG::FS,
        log_file_path: &Path,
    ) -> Option<WriteLogWriter<LdbWriteFile<LDBG>>> {
        let log_size = filesystem.size_of(log_file_path).ok()?;
        if log_size >= self.db_options.file_size_limit {
            return None;
        }

        let log_file = filesystem.open_appendable(log_file_path, false)
            .inspect_err(|_err| {
                // TODO: log error
            }).ok()?;

        let log = WriteLogWriter::new_with_offset(log_file, log_size);

        Some(log)
    }

    fn finish(
        self,
        filesystem:       FSGuard<LDBG>,
        mut vset_builder: VersionSetBuilder<LDBG::Refcounted, LdbWriteFile<LDBG>, true>,
    ) -> Result<InnerGenericDB<LDBG, WriteImpl>, ()> {
        let Self {
            db_directory,
            table_cache,
            table_options,
            db_options,
            corruption_handler,
            reused_log,
            info_logger,
            write_impl,
        } = self;
        let mut fs_lock = filesystem.filesystem.write();

        // Get the reused log (and its file number, and its corresponding memtable),
        // or create a new one.
        let (memtable, log, log_number) = if let Some(reused_log) = reused_log {
            (reused_log.memtable, reused_log.log, reused_log.log_number)
        } else {
            let new_log_number = vset_builder.new_log_file_number().map_err(|_| ())?;
            let new_log_path = LevelDBFileName::Log { file_number: new_log_number }
                .file_path(&db_directory);
            let log_file = fs_lock.open_writable(&new_log_path, false).map_err(|_| ())?;
            (
                Memtable::new(table_options.comparator.fast_mirrored_clone().0),
                WriteLogWriter::new_empty(log_file),
                new_log_number,
            )
        };

        let version_set = vset_builder.finish(
            &mut *fs_lock,
            &db_directory,
            &table_options.comparator,
            log_number,
        )?;

        drop(fs_lock);
        Ok(InnerGenericDB::build(BuildGenericDB {
            db_directory,
            filesystem,
            table_cache,
            table_options,
            db_options,
            corruption_handler,
            version_set,
            current_memtable: memtable,
            current_log:      log,
            info_logger,
            write_impl,
        }))
    }
}

fn parse_write_batch(record: &[u8]) -> Result<ParsedWriteBatch, ()> {
    // TODO: make parsing and encoding of write batches MUCH better.

    // Byte manipulation
    let (header, headerless_entries) = record.split_first_chunk::<12>().ok_or(())?;
    let (sequence_number, num_entries) = header.split_first_chunk::<8>().unwrap();

    let sequence_number = u64::from_le_bytes(*sequence_number);
    let num_entries: [u8; 4] = num_entries.try_into().unwrap();
    let num_entries = u32::from_le_bytes(num_entries);

    // Some validation and calculation
    // TODO: depending on a setting, maybe don't do validation.
    let batch = UnvalidatedWriteBatch {
        num_entries,
        // TODO: avoid this clone.
        headerless_entries: headerless_entries.to_owned(),
    }.into_validated().map_err(|_| ())?;

    let batch_first_sequence = SequenceNumber::new_usable(sequence_number).ok_or(())?;

    let last_sequence_before_batch = batch_first_sequence
        .checked_decrement()
        .ok_or(())?;
    let batch_last_sequence = last_sequence_before_batch
        .checked_add_u32(batch.num_entries())
        .map_err(|_| ())?;

    Ok(ParsedWriteBatch {
        batch,
        last_sequence_before_batch,
        batch_last_sequence,
    })
}

struct ParsedWriteBatch {
    // TODO: make a BorrowedWriteBatch type or something, to avoid a clone
    batch:                      WriteBatch,
    last_sequence_before_batch: SequenceNumber,
    batch_last_sequence:        SequenceNumber,
}

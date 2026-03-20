use std::thread;
use std::{path::PathBuf, sync::Arc};

use anchored_vfs::{CreateParentDir, LevelDBFilesystem, SyncParentDir};
use clone_behavior::FastMirroredClone;

use crate::{database_files::LevelDBFileName, file_tracking::FileMetadata};
use crate::{
    all_errors::types::{
        AddTableEntryError, CorruptedVersionError, CorruptionError, FilesystemError,
        NewTableReaderError, OutOfFileNumbers, ReadError, ReadFsError, RwErrorKind, WriteError,
        WriteFsError,
    },
    memtable::Memtable,
    options::{CacheUsage, InternalOptions, InternalOptionsPerRead},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{FileNumber, FileSize, NonZeroLevel},
    sstable::{TableBuilder, TableReader},
    table_caches::{TableCache, TableCacheKey},
    typed_bytes::{EncodedInternalKey, InternalKey, MaybeUserValue},
};


pub(crate) struct TableFileBuilder<'a, FS, Policy, Pool>
where
    FS:     LevelDBFilesystem,
    Policy: FilterPolicy,
    Pool:   BufferPool,
{
    fs:           &'a FS,
    db_directory: PathBuf,
    /// Value is irrelevant if `builder` is inactive.
    file_number:  FileNumber,
    /// Value is irrelevant if `builder` is inactive.
    ///
    /// `None` denotes that this table is being produces from a memtable.
    level:        Option<NonZeroLevel>,
    builder:      TableBuilder<FS::WriteFile, Policy, Pool>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, FS, Policy, Pool> TableFileBuilder<'a, FS, Policy, Pool>
where
    FS:     LevelDBFilesystem,
    Policy: FilterPolicy,
    Pool:   BufferPool,
{
    /// Create a new and initially [inactive] builder. Before [`add_entry`] or [`finish`] is
    /// called on the returned builder, [`start`] must be called on it.
    ///
    /// [inactive]: TableFileBuilder::active
    /// [`start`]: TableFileBuilder::start
    /// [`add_entry`]: TableFileBuilder::add_entry
    /// [`finish`]: TableFileBuilder::finish
    #[inline]
    #[must_use]
    pub fn new<Cmp, Codecs>(
        filesystem:   &'a FS,
        opts:         &InternalOptions<Cmp, Policy, Codecs, Pool>,
    ) -> Self
    where
        Policy: FastMirroredClone,
    {
        Self {
            fs:           filesystem,
            // TODO: Figure out whether this clone is necessary.
            db_directory: opts.db_directory.clone(),
            file_number:  FileNumber(0),
            level:        None,
            builder:      TableBuilder::new(opts),
        }
    }

    /// Writes the entries of the memtable to zero or more table files.
    ///
    /// If the memtable is empty, zero table files are used. Otherwise, table files are split
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
    #[expect(clippy::type_complexity, reason = "it's just `RwErrorKind`'s fault")]
    fn flush_memtable<Cmp, Codecs, F>(
        &mut self,
        opts:                &InternalOptions<Cmp, Policy, Codecs, Pool>,
        table_cache:         &TableCache<FS::RandomAccessFile, Policy, Pool>,
        encoders:            &mut Codecs::Encoders,
        decoders:            &mut Codecs::Decoders,
        memtable:            &Memtable<Cmp>,
        mut get_file_number: F,
    ) -> Result<Vec<FileMetadata>, RwErrorKind<
        FS::Error,
        Cmp::InvalidKeyError,
        Codecs::CompressionError,
        Codecs::DecompressionError,
    >>
    where
        Cmp:        LevelDBComparator,
        Policy:     FastMirroredClone,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
        F:          FnMut() -> Result<FileNumber, OutOfFileNumbers>,
    {
        let mut memtable_iter = memtable.iter();
        let mut created_file_metadata = Vec::new();

        while let Some(mut current) = memtable_iter.next() {
            let table_file_number = get_file_number()
                .map_err(|OutOfFileNumbers {}| RwErrorKind::Write(WriteError::OutOfFileNumbers))?;

            self.start(opts, table_file_number, None).map_err(RwErrorKind::Write)?;

            let smallest_key = current.0;

            // Correctness: the memtable is sorted solely by internal key
            // (in the same way in which `InternalComparator<Cmp>` would sort the internal keys)
            // and does not have any entries with duplicate keys.
            match self.add_entry(current.0, current.1, opts, encoders) {
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
                match self.add_entry(current.0, current.1, opts, encoders) {
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

            created_file_metadata.push(self.finish(
                opts,
                table_cache,
                encoders,
                decoders,
                smallest_key.as_internal_key(),
                largest_key.as_internal_key(),
            )?);
        }

        Ok(created_file_metadata)
    }


    /// Begin writing a table file with the indicated file number. The file is either newly created
    /// or initially truncated to zero bytes.
    ///
    /// The table file's parent directory is **not** synced. (Instead, the parent directory
    /// is synced when the `CURRENT` file is replaced, which must occur when publishing
    /// references to new table files.)
    ///
    /// The builder then becomes [active], and may have [`add_entry`] or [`finish`] called on it.
    ///
    /// Note that if the builder was already active, the previous table file would be closed, but
    /// it would _not_ be properly finished *or* deleted. That file would be an invalid table file
    /// and should eventually be garbage collected by this program.
    ///
    /// # Errors
    /// Returns any error that occurs when opening the table file.
    ///
    /// [active]: TableBuilder::active
    /// [`add_entry`]: TableBuilder::add_entry
    /// [`finish`]: TableBuilder::finish
    pub fn start<Cmp, Codecs, InvalidKey, Compression, Decompression>(
        &mut self,
        opts:              &InternalOptions<Cmp, Policy, Codecs, Pool>,
        table_file_number: FileNumber,
        level:             Option<NonZeroLevel>,
    ) -> Result<(), WriteError<FS::Error, InvalidKey, Compression, Decompression>> {
        let file_number = table_file_number;
        let table_path = LevelDBFileName::Table { file_number }.file_path(&self.db_directory);

        let table_file = self.fs.open_writable(
            &table_path,
            CreateParentDir::False,
            SyncParentDir::False,
        ).map_err(|fs_err| {
            WriteError::Filesystem(
                FilesystemError::FsError(fs_err),
                file_number,
                WriteFsError::OpenWritableTableFile,
            )
        })?;

        // Statically guaranteed to not panic.
        #[expect(
            clippy::indexing_slicing,
            reason = "`usize::from(level.inner().get() - 1 <= 6-1 < NUM_NONZERO_LEVELS_USIZE`",
        )]
        let compressor = if let Some(level) = level {
            opts.table_compressors[usize::from(level.inner().get() - 1)]
        } else {
            opts.memtable_compressor
        };

        self.file_number = file_number;
        self.level = level;
        self.builder.start(table_file, compressor);
        Ok(())
    }

    /// Abandon and delete the previous table file (if any), making the builder [inactive].
    ///
    /// # Errors
    /// Returns any error that occurs when deleting the previous table file.
    ///
    /// [inactive]: TableFileBuilder::active
    pub fn deactivate(&mut self) -> Result<(), FS::Error> {
        if self.builder.active() {
            self.builder.deactivate();

            let file_number = self.file_number;
            let table_path = LevelDBFileName::Table { file_number }.file_path(&self.db_directory);

            self.fs.remove_file(&table_path)
        } else {
            Ok(())
        }
    }

    /// Determines whether the builder has an associated table file.
    ///
    /// A builder is active only while it has an associated in-progress table file, provided in
    /// [`TableFileBuilder::start`] and consumed in [`TableFileBuilder::finish`]. A just-constructed
    /// builder is inactive.
    ///
    /// [`add_entry`] and [`finish`] must only be called on active builders, or else a panic will
    /// occur.
    ///
    /// [`add_entry`]: TableFileBuilder::add_entry
    /// [`finish`]: TableFileBuilder::finish
    #[inline]
    #[must_use]
    pub const fn active(&self) -> bool {
        self.builder.active()
    }

    /// Get the number of entries which have been added to the current table with
    /// [`TableFileBuilder::add_entry`].
    ///
    /// If the builder is not [active], then the value is unspecified, though a panic will not
    /// occur.
    ///
    /// [active]: TableFileBuilder::active
    #[inline]
    #[must_use]
    pub const fn num_entries(&self) -> usize {
        self.builder.num_entries()
    }

    /// Estimates the length that the table file being built would have if `self.finish(..)`
    /// were called now.
    ///
    /// This is a rough estimate that does not take into account:
    /// - compression of the current data block,
    /// - compression of the index block,
    /// - the metaindex block, which contains the name of any filter policy.
    ///
    /// If the builder is not [active], then the value is unspecified, though a panic will not
    /// occur.
    ///
    /// [active]: TableFileBuilder::active
    #[must_use]
    pub fn estimated_finished_file_length(&self) -> FileSize {
        self.builder.estimated_finished_file_length()
    }

    /// Add a new entry to the table.
    ///
    /// With respect to `InternalComparator<LDBG::Cmp>`, wrapping the comparator which was provided
    /// in the options struct when this builder was created, the `key` must compare strictly
    /// greater than any previously-added key. If this requirement is not met, a panic may occur
    /// or an invalid `Table` file may be produced by this builder.
    ///
    /// # Errors
    /// On error, the current table file is abandoned and deleted.
    ///
    /// # Panics
    /// Panics if the builder is not currently [active].
    ///
    /// [active]: TableBuilder::active
    /// [`Table`]: crate::table::Table
    /// [`Policy::append_key_data`]: anchored_sstable::options::TableFilterPolicy::append_key_data
    /// [`FilterBlockBuilder`]: anchored_sstable::table_format::FilterBlockBuilder
    #[expect(clippy::type_complexity, reason = "it's just `RwErrorKind`'s fault")]
    pub fn add_entry<Cmp, Codecs>(
        &mut self,
        key:      EncodedInternalKey<'_>,
        value:    MaybeUserValue<'_>,
        opts:     &InternalOptions<Cmp, Policy, Codecs, Pool>,
        encoders: &mut Codecs::Encoders,
    ) -> Result<(), AddTableEntryError<RwErrorKind<
        FS::Error,
        Cmp::InvalidKeyError,
        Codecs::CompressionError,
        Codecs::DecompressionError,
    >>>
    where
        Cmp:        LevelDBComparator,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
    {
        self.builder.add_entry::<Cmp, Codecs>(key, value, opts, encoders)
            .map_err(|add_entry_err| {
                self.delete_table_file();
                match add_entry_err {
                    AddTableEntryError::AddEntryError    => AddTableEntryError::AddEntryError,
                    AddTableEntryError::Write(write_err) => AddTableEntryError::Write(
                        write_err.into_rw_error(self.level, self.file_number),
                    ),
                }
            })
    }

    /// Finish writing the entire table to the table file and sync the file to persistent storage.
    /// WARNING: the data of the file's parent directory also needs to be synced to persistent
    /// storage in order to ensure crash resilience.
    ///
    /// On success, the total number of bytes written to the table file is returned.
    ///
    /// After this method is called, no other [`TableFileBuilder`] methods should be called other
    /// than [`Self::reuse_as_new`] or [`Self::new_or_reuse`]. See the type-level documentation for
    /// more.
    #[expect(clippy::type_complexity, reason = "it's just `RwErrorKind`'s fault")]
    pub fn finish<Cmp, Codecs>(
        &mut self,
        opts:         &InternalOptions<Cmp, Policy, Codecs, Pool>,
        table_cache:  &TableCache<FS::RandomAccessFile, Policy, Pool>,
        encoders:     &mut Codecs::Encoders,
        decoders:     &mut Codecs::Decoders,
        smallest_key: InternalKey<'_>,
        largest_key:  InternalKey<'_>,
    ) -> Result<FileMetadata, RwErrorKind<
        FS::Error,
        Cmp::InvalidKeyError,
        Codecs::CompressionError,
        Codecs::DecompressionError,
    >>
    where
        Cmp:        LevelDBComparator,
        Policy:     FastMirroredClone,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
    {
        let file_size = self.builder.finish(opts, encoders)
            .map_err(|write_err| {
                self.delete_table_file();
                write_err.into_rw_error(self.level, self.file_number)
            })?;

        // Confirm that the produced table is actually usable
        let read_opts = InternalOptionsPerRead {
            verify_checksums:  true,
            // `read_sstable` only uses the table cache, so this setting is irrelevant.
            block_cache_usage: CacheUsage::ReadAndFill,
            table_cache_usage: CacheUsage::ReadAndFill,
        };

        let _table = read_sstable::<FS, Cmp, Policy, Codecs, Pool>(
            self.file_number,
            file_size,
            opts,
            &read_opts,
            self.fs,
            table_cache,
            decoders,
        ).inspect_err(|_| {
            self.delete_table_file();
        })?;

        Ok(FileMetadata::new(
            self.file_number,
            file_size,
            smallest_key,
            largest_key,
            opts.seek_compactions,
        ))
    }

    /// Should only be called if an error is encountered or if `self` is dropped.
    ///
    /// This calls [`Self::deactivate`] and ignores any error.
    fn delete_table_file(&mut self) {
        #[expect(
            let_underscore_drop,
            clippy::let_underscore_must_use,
            reason = "ignore any error which occurs while handling the root error",
        )]
        let _: Result<_, _> = self.deactivate();
    }
}

impl<FS, Policy, Pool> Drop for TableFileBuilder<'_, FS, Policy, Pool>
where
    FS:     LevelDBFilesystem,
    Policy: FilterPolicy,
    Pool:   BufferPool,
{
    fn drop(&mut self) {
        // When a panic occurs, destructors would still be run if the program starts unwinding.
        // There's no point in causing a double panic (and thus an abort) just to delete
        // an invalid table file which would likely be garbage-collected later.
        // Plus, there's no reason to think that deleting the invalid table file would necessarily
        // succeed if we're already panicking.
        if !thread::panicking() {
            self.delete_table_file();
        }
    }
}

#[expect(clippy::type_complexity, reason = "`RwErrorKind` is still fairly readable")]
pub(crate) fn read_sstable<FS, Cmp, Policy, Codecs, Pool>(
    sstable_file_number: FileNumber,
    sstable_file_size:   FileSize,
    opts:                &InternalOptions<Cmp, Policy, Codecs, Pool>,
    read_opts:           &InternalOptionsPerRead,
    fs:                  &FS,
    table_cache:         &TableCache<FS::RandomAccessFile, Policy, Pool>,
    decoders:            &mut Codecs::Decoders,
) -> Result<Arc<TableReader<FS::RandomAccessFile, Policy, Pool>>, RwErrorKind<
    FS::Error,
    Cmp::InvalidKeyError,
    Codecs::CompressionError,
    Codecs::DecompressionError,
>>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    let table_key = TableCacheKey { table_number: sstable_file_number };

    let mut read_table = || {
        let file_number = sstable_file_number;
        let table_path = LevelDBFileName::Table { file_number }
            .file_path(&opts.db_directory);

        let sstable_file = match fs.open_random_access(&table_path) {
            Ok(file) => file,
            Err(first_error) => {
                let sst_path = LevelDBFileName::TableLegacyExtension { file_number }
                    .file_path(&opts.db_directory);

                // Try opening the legacy path, though if that fails, return the first error.
                fs.open_random_access(&sst_path)
                    .map_err(|_second_error| {
                        RwErrorKind::Read(ReadError::Filesystem(
                            FilesystemError::FsError(first_error),
                            ReadFsError::OpenTableFile(file_number),
                        ))
                    })?
            }
        };

        let table = TableReader::new(
            sstable_file,
            file_number,
            sstable_file_size,
            opts,
            read_opts,
            decoders,
        ).map_err(|new_table_err| {
            match new_table_err {
                NewTableReaderError::BlockUsizeOverflow(handle)
                    => RwErrorKind::Read(ReadError::BlockUsizeOverflow(file_number, handle)),
                NewTableReaderError::BufferAllocErr
                    => RwErrorKind::Read(ReadError::BufferAllocErr),
                NewTableReaderError::FileSizeTooShort
                    => RwErrorKind::Corruption(CorruptionError::CorruptedVersion(
                        CorruptedVersionError::FileSizeTooSmall(file_number),
                    )),
                NewTableReaderError::TableCorruption(corruption)
                    => RwErrorKind::Corruption(CorruptionError::CorruptedTable(
                        file_number,
                        corruption,
                    )),
                NewTableReaderError::Io(io_err)
                    => RwErrorKind::Read(ReadError::Filesystem(
                        FilesystemError::Io(io_err),
                        ReadFsError::ReadTableFile(file_number),
                    )),
            }
        })?;

        Ok(Arc::new(table))
    };

    match read_opts.table_cache_usage {
        CacheUsage::ReadAndFill => table_cache.get_or_insert_with(table_key, read_table),
        CacheUsage::Read => {
            if let Some(cached_table) = table_cache.get(table_key) {
                Ok(cached_table)
            } else {
                read_table()
            }
        }
        CacheUsage::Ignore => read_table(),
    }
}

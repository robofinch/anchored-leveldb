use std::sync::Arc;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_vfs::{CreateParentDir, LevelDBFilesystem, SyncParentDir, WritableFile};
use clone_behavior::FastMirroredClone;

use crate::{
    database_files::LevelDBFileName,
    file_tracking::FileMetadata,
    table_caches::TableCacheKey,
};
use crate::{
    all_errors::{
        aliases::{RwErrorKindAlias, WriteErrorAlias},
        types::{
            AddTableEntryError, CorruptedManifestError, CorruptionError, FilesystemError,
            NewTableReaderError, ReadError, ReadFsError, RwErrorKind, WriteError, WriteFsError,
        },
    },
    options::{
        InternallyMutableOptions, InternalOptions, InternalReadOptions, pub_options::CacheUsage,
    },
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{FileNumber, FileSize, NonZeroLevel},
    sstable::{TableBuilder, TableReader},
    typed_bytes::{EncodedInternalKey, InternalKey, MaybeUserValue},
};


pub(crate) struct TableFileBuilder<File, Policy, Pool: BufferPool> {
    builder:     TableBuilder<File, Policy, Pool>,
    /// Value is irrelevant if `builder` is inactive.
    file_number: FileNumber,
    /// Value is irrelevant if `builder` is inactive.
    ///
    /// `None` denotes that this table is being produces from a memtable.
    level:       Option<NonZeroLevel>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File, Policy, Pool> TableFileBuilder<File, Policy, Pool>
where
    File:   WritableFile,
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
    pub fn new<Cmp, Codecs>(opts: &InternalOptions<Cmp, Policy, Codecs>) -> Self
    where
        Policy: FastMirroredClone,
    {
        Self {
            file_number:  FileNumber(0),
            level:        None,
            builder:      TableBuilder::new(opts),
        }
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
    pub fn start<FS, Cmp, Codecs>(
        &mut self,
        opts:              &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:          &InternallyMutableOptions<FS, Policy, Pool>,
        table_file_number: FileNumber,
        level:             Option<NonZeroLevel>,
    ) -> Result<(), WriteErrorAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<WriteFile = File>,
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
    {
        let file_number = table_file_number;
        let table_path = LevelDBFileName::Table { file_number }.file_path(&opts.db_directory);

        let table_file = mut_opts.filesystem.open_writable(
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

        self.file_number = file_number;
        self.level = level;
        self.builder.start(&mut_opts.dynamic.read(), table_file, level);
        Ok(())
    }

    /// Abandon and delete the previous table file (if any), making the builder [inactive].
    ///
    /// # Errors
    /// Returns any error that occurs when deleting the previous table file.
    ///
    /// [inactive]: TableFileBuilder::active
    pub fn deactivate<FS, Cmp, Codecs>(
        &mut self,
        opts:     &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts: &InternallyMutableOptions<FS, Policy, Pool>,
    ) -> Result<(), FS::Error>
    where
        FS:     LevelDBFilesystem,
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
    {
        if self.builder.active() {
            self.builder.deactivate();

            let file_number = self.file_number;
            let table_path = LevelDBFileName::Table { file_number }.file_path(&opts.db_directory);

            mut_opts.filesystem.remove_file(&table_path)
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
    pub fn add_entry<FS, Cmp, Codecs>(
        &mut self,
        opts:     &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts: &InternallyMutableOptions<FS, Policy, Pool>,
        encoders: &mut Codecs::Encoders,
        key:      EncodedInternalKey<'_>,
        value:    MaybeUserValue<'_>,
    ) -> Result<(), AddTableEntryError<RwErrorKindAlias<FS, Cmp, Codecs>>>
    where
        FS:         LevelDBFilesystem<WriteFile = File>,
        Cmp:        LevelDBComparator,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
    {
        self.builder.add_entry(opts, mut_opts, encoders, key, value)
            .map_err(|add_entry_err| {
                self.delete_table_file(opts, mut_opts);
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
    #[expect(
        clippy::too_many_arguments,
        reason = "the first four arguments can't easily be conglomerated",
    )]
    pub fn finish<FS, Cmp, Codecs>(
        &mut self,
        opts:            &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:        &InternallyMutableOptions<FS, Policy, Pool>,
        encoders:        &mut Codecs::Encoders,
        decoders:        &mut Codecs::Decoders,
        manifest_number: FileNumber,
        smallest_key:    InternalKey<'_>,
        largest_key:     InternalKey<'_>,
    ) -> Result<FileMetadata, RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:         LevelDBFilesystem,
        Cmp:        LevelDBComparator,
        Policy:     FastMirroredClone,
        Codecs:     CompressionCodecs,
        Policy::Eq: CoarserThan<Cmp::Eq>,
    {
        let file_size = self.builder.finish(opts, mut_opts, encoders)
            .map_err(|write_err| {
                self.delete_table_file(opts, mut_opts);
                write_err.into_rw_error(self.level, self.file_number)
            })?;

        // Confirm that the produced table is actually usable
        let read_opts = InternalReadOptions {
            verify_data_checksums:  true,
            verify_index_checksums: true,
            // `read_sstable` only uses the table cache, so this setting is irrelevant.
            block_cache_usage:      CacheUsage::ReadAndFill,
            table_cache_usage:      CacheUsage::ReadAndFill,
        };

        let _table = read_sstable::<FS, Cmp, Policy, Codecs, Pool>(
            opts,
            mut_opts,
            read_opts,
            decoders,
            manifest_number,
            self.file_number,
            file_size,
        ).inspect_err(|_| {
            self.delete_table_file(opts, mut_opts);
        })?;

        Ok(FileMetadata::new(
            self.file_number,
            file_size,
            smallest_key,
            largest_key,
            opts.compaction.seek_compactions,
        ))
    }

    /// Should only be called if an error is encountered or if `self` is dropped.
    ///
    /// This calls [`Self::deactivate`] and ignores any error.
    fn delete_table_file<FS, Cmp, Codecs>(
        &mut self,
        opts:     &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts: &InternallyMutableOptions<FS, Policy, Pool>,
    )
    where
        FS:     LevelDBFilesystem,
        Cmp:    LevelDBComparator,
        Codecs: CompressionCodecs,
    {
        #[expect(
            let_underscore_drop,
            clippy::let_underscore_must_use,
            reason = "ignore any error which occurs while handling the root error",
        )]
        // TODO: would be good to log the error.
        let _: Result<_, _> = self.deactivate(opts, mut_opts);
    }
}

impl<File, Policy, Pool> Debug for TableFileBuilder<File, Policy, Pool>
where
    File:   Debug,
    Policy: Debug,
    Pool:   Debug + BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("TableFileBuilder")
            .field("builder",     &self.builder)
            .field("file_number", &self.file_number)
            .field("level",       &self.level)
            .finish()
    }
}

#[expect(clippy::type_complexity, reason = "the result is still fairly readable")]
pub(crate) fn read_sstable<FS, Cmp, Policy, Codecs, Pool>(
    opts:                &InternalOptions<Cmp, Policy, Codecs>,
    mut_opts:            &InternallyMutableOptions<FS, Policy, Pool>,
    read_opts:           InternalReadOptions,
    decoders:            &mut Codecs::Decoders,
    manifest_number:     FileNumber,
    sstable_file_number: FileNumber,
    sstable_file_size:   FileSize,
) -> Result<
    Arc<TableReader<FS::RandomAccessFile, Policy, Pool>>,
    RwErrorKindAlias<FS, Cmp, Codecs>,
>
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

        let sstable_file = match mut_opts.filesystem.open_random_access(&table_path) {
            Ok(file) => file,
            Err(first_error) => {
                let sst_path = LevelDBFileName::TableLegacyExtension { file_number }
                    .file_path(&opts.db_directory);

                // Try opening the legacy path, though if that fails, return the first error.
                mut_opts.filesystem.open_random_access(&sst_path)
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
            mut_opts,
            read_opts,
            decoders,
        ).map_err(|new_table_err| {
            match new_table_err {
                NewTableReaderError::BlockUsizeOverflow(handle)
                    => RwErrorKind::Read(ReadError::BlockUsizeOverflow(file_number, handle)),
                NewTableReaderError::BufferAllocErr
                    => RwErrorKind::Read(ReadError::BufferAllocErr),
                NewTableReaderError::FileSizeTooShort
                    => RwErrorKind::Corruption(CorruptionError::CorruptedManifest(
                        manifest_number,
                        CorruptedManifestError::FileSizeTooSmall(file_number),
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
        CacheUsage::ReadAndFill => mut_opts.table_cache.get_or_insert_with(table_key, read_table),
        CacheUsage::Read => {
            if let Some(cached_table) = mut_opts.table_cache.get(table_key) {
                Ok(cached_table)
            } else {
                read_table()
            }
        }
        CacheUsage::Ignore => read_table(),
    }
}

use std::thread;
use std::borrow::Borrow;
use std::path::{Path, PathBuf};

use generic_container::FragileTryContainer as _;

use anchored_sstable::{Table, TableBuilder};
use anchored_sstable::options::KVCache as _;
use anchored_vfs::traits::{ReadableFilesystem, WritableFilesystem as _};

use crate::leveldb_generics::{LdbFsCell, LdbRwCell};
use crate::{containers::FragileRwCell as _, memtable::Memtable};
use crate::{
    file_tracking::{FileMetadata, SeeksBetweenCompactionOptions},
    format::{EncodedInternalKey, FileNumber, InternalKey, LevelDBFileName, UserValue},
    leveldb_generics::{
        LevelDBGenerics, LdbReadTableOptions, LdbTableBuilder, LdbTableContainer,
        LdbTableOptions, LdbWriteTableOptions,
    },
};


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableCacheKey {
    table_file_number: u64,
}

// Because this internal struct is transient and implementing `Debug` (or similar) would be tedious,
// `Debug` is not implemented.
pub(crate) struct TableFileBuilder<LDBG: LevelDBGenerics, FS: Borrow<LdbFsCell<LDBG>>> {
    fs:           FS,
    db_directory: PathBuf,
    /// Value is irrelevant if `builder` is inactive.
    file_number:  FileNumber,
    builder:      LdbTableBuilder<LDBG>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, FS: Borrow<LdbFsCell<LDBG>>> TableFileBuilder<LDBG, FS> {
    /// Create a new and initially [inactive] builder. Before [`add_entry`] or [`finish`] is
    /// called on the returned builder, [`start`] must be called on it.
    ///
    /// [inactive]: TableFileBuilder::active
    /// [`start`]: TableFileBuilder::start
    /// [`add_entry`]: TableFileBuilder::add_entry
    /// [`finish`]: TableFileBuilder::finish
    #[inline]
    #[must_use]
    pub fn new(
        filesystem:   FS,
        db_directory: PathBuf,
        write_opts:   LdbWriteTableOptions<LDBG>,
    ) -> Self {
        Self {
            fs:           filesystem,
            db_directory,
            file_number:  FileNumber(0),
            builder:      TableBuilder::new(write_opts),
        }
    }

    /// Begin writing a table file with the indicated file number. The file is either newly created
    /// or initially truncated to zero bytes.
    ///
    /// The builder then becomes [active], and may have [`add_entry`] or [`finish`] called on it.
    ///
    /// Note that if the builder was already active, the previous table file would be closed, but
    /// it would _not_ be properly finished; that file would be an invalid table file.
    ///
    /// # Errors
    /// Returns any error that occurs when opening the table file.
    ///
    /// [active]: TableBuilder::active
    /// [`add_entry`]: TableBuilder::add_entry
    /// [`finish`]: TableBuilder::finish
    pub fn start(
        &mut self,
        table_file_number: FileNumber,
    ) -> Result<(), <LDBG::FS as ReadableFilesystem>::Error> {
        let file_number = table_file_number;
        let table_filename = LevelDBFileName::Table { file_number }.file_name();
        let table_path = self.db_directory.join(table_filename);

        let mut fs_ref = self.fs.borrow().write();
        let fs: &mut LDBG::FS = &mut fs_ref;
        let table_file = fs.open_writable(&table_path, false)?;
        drop(fs_ref);

        self.file_number = file_number;
        self.builder.start(table_file);
        Ok(())
    }

    /// Abandon and delete the previous table file (if any), making the builder [inactive].
    ///
    /// # Errors
    /// Returns any error that occurs when deleting the previous table file.
    ///
    /// [inactive]: TableFileBuilder::active
    pub fn deactivate(&mut self) -> Result<(), <LDBG::FS as ReadableFilesystem>::Error> {
        if self.builder.active() {
            self.builder.deactivate();

            let file_number = self.file_number;
            let table_filename = LevelDBFileName::Table { file_number }.file_name();
            let table_path = self.db_directory.join(table_filename);

            let mut fs_ref = self.fs.borrow().write();
            fs_ref.delete(&table_path)
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
    pub fn estimated_finished_file_length(&self) -> u64 {
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
    /// May panic if `key.len() + value.len() + opts.block_size + 30` exceeds `u32::MAX`, where
    /// `opts` refers to the [`LdbWriteTableOptions`] struct which was provided to [`Self::new`].
    /// More precisely, if the current block's size ends up exceeding `u32::MAX`, a panic would
    /// occur.
    ///
    /// May also panic if adding this entry would result in at least 4 GiB of key data,
    /// produced by [`Policy::append_key_data`], associated with the current block.
    /// Note that the key data is not necessarily equivalent to concatenating the keys together.
    /// Lastly, this function may panic if at least 4 GiB of filters are generated
    /// by `Policy` for this table; such an event would generally only occur if hundreds of millions
    /// of entries were added to a single table. See [`FilterBlockBuilder`] for more.
    ///
    /// [active]: TableBuilder::active
    /// [`Table`]: crate::table::Table
    /// [`Policy::append_key_data`]: anchored_sstable::options::TableFilterPolicy::append_key_data
    /// [`FilterBlockBuilder`]: anchored_sstable::table_format::FilterBlockBuilder
    pub fn add_entry(
        &mut self,
        key:   EncodedInternalKey<'_>,
        value: UserValue<'_>,
    ) -> Result<(), ()> {
        self.builder.add_entry(key.0, value.0).inspect_err(|()| self.delete_table_file())
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
    pub fn finish(
        &mut self,
        table_cache:  &LDBG::TableCache,
        read_opts:    LdbReadTableOptions<LDBG>,
        seek_opts:    SeeksBetweenCompactionOptions,
        smallest_key: InternalKey<'_>,
        largest_key:  InternalKey<'_>,
    ) -> Result<FileMetadata, ()> {
        let file_size = self.builder.finish(true).inspect_err(|()| {
            self.delete_table_file();
        })?;

        // Confirm that the produced table is actually usable
        let _table = get_table::<LDBG>(
            self.fs.borrow(),
            &self.db_directory,
            table_cache,
            read_opts,
            self.file_number,
            file_size,
        ).inspect_err(|()| {
            self.delete_table_file();
        })?;

        // TODO: better SSTable error handling; check that an iterator can be initialized.
        // let _ = Table::new_iter(table)?;

        Ok(FileMetadata::new(
            self.file_number,
            file_size,
            smallest_key,
            largest_key,
            seek_opts,
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

impl<LDBG: LevelDBGenerics, FS: Borrow<LdbFsCell<LDBG>>> Drop for TableFileBuilder<LDBG, FS> {
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

/// If the provided memtable is nonempty, writes the entries of the memtable to a new table file
/// with the indicated file number.
pub(crate) fn build_table<LDBG: LevelDBGenerics>(
    filesystem:        &LdbFsCell<LDBG>,
    db_directory:      PathBuf,
    table_cache:       &LDBG::TableCache,
    table_opts:        LdbTableOptions<LDBG>,
    seek_opts:         SeeksBetweenCompactionOptions,
    memtable:          &Memtable<LDBG::Cmp, LDBG::Skiplist>,
    table_file_number: FileNumber,
) -> Result<Option<FileMetadata>, ()> {
    let mut memtable_iter = memtable.iter();
    let Some(mut current) = memtable_iter.next() else {
        // If the memtable is completely empty, there's no need to create a table file for it.
        return Ok(None);
    };

    let (read_opts, write_opts) = table_opts.split();

    let mut builder = TableFileBuilder::<LDBG, _>::new(
        filesystem,
        db_directory,
        write_opts,
    );
    #[expect(clippy::map_err_ignore, reason = "TODO: return better errors")]
    builder.start(table_file_number).map_err(|_| ())?;

    let smallest_key = current.internal_key();

    let largest_key = loop {
        let (internal_key, user_value) = current.key_and_user_value();
        // Correctness: the memtable is sorted solely by internal key
        // (in the same way in which `InternalComparator<Cmp>` would sort the internal keys)
        // and does not have any entries with duplicate keys.
        builder.add_entry(internal_key, user_value)?;

        if let Some(next) = memtable_iter.next() {
            current = next;
        } else {
            break current.internal_key();
        }
    };

    let metadata = builder.finish(
        table_cache,
        read_opts,
        seek_opts,
        smallest_key,
        largest_key,
    )?;

    Ok(Some(metadata))
}

/// Attempt to open the table file with the indicated file number. The file size must be accurate.
///
/// An error is returned if no such table file exists, among other cases.
pub(crate) fn get_table<LDBG: LevelDBGenerics>(
    filesystem:        &LdbFsCell<LDBG>,
    db_directory:      &Path,
    table_cache:       &LDBG::TableCache,
    read_opts:         LdbReadTableOptions<LDBG>,
    // TODO(opt): `fill_cache` option: should the table be inserted into the cache
    // likewise, TODO(opt) in sstable: support the option to not insert blocks into the block cache.
    // It might be cool to wait until I have a working version, though, so I can see to what
    // extent those options actually help bulk scans.
    table_file_number: FileNumber,
    file_size:         u64,
) -> Result<LdbTableContainer<LDBG>, ()> {
    let cache_key = TableCacheKey { table_file_number: table_file_number.0 };

    if let Some(table_container) = table_cache.get(&cache_key) {
        return Ok(table_container);
    }

    let file_number = table_file_number;
    let table_filename = LevelDBFileName::Table { file_number }.file_name();
    let table_path = db_directory.join(table_filename);

    let fs_ref = filesystem.read();
    let fs: &LDBG::FS = &fs_ref;

    let table_file = match fs.open_random_access(&table_path) {
        Ok(file)         => file,
        Err(_first_error) => {
            // Try opening the legacy path
            let sst_path = LevelDBFileName::TableLegacyExtension { file_number }.file_name();

            if let Ok(file) = fs.open_random_access(&sst_path) {
                file
            } else {
                // TODO: return error based on `_first_error`
                return Err(());
            }
        }
    };
    drop(fs_ref);

    let table = Table::new(read_opts, table_file, file_size, table_file_number.0)?;
    let table_container = LdbTableContainer::<LDBG>::new_container(table);

    table_cache.insert(cache_key, &table_container);

    Ok(table_container)
}

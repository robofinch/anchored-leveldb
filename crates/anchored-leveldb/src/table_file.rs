use std::thread;
use std::borrow::Borrow;
use std::path::{Path, PathBuf};

use generic_container::FragileTryContainer as _;

use anchored_sstable::{Table, TableBuilder};
use anchored_sstable::options::KVCache as _;
use anchored_vfs::traits::{ReadableFilesystem, WritableFilesystem as _};

use crate::{containers::RwCell as _, memtable::Memtable};
use crate::{
    format::{EncodedInternalKey, InternalKey, LevelDBFileName, UserValue},
    leveldb_generics::{
        LevelDBGenerics, LdbReadTableOptions, LdbTableBuilder, LdbTableContainer,
        LdbTableOptions, LdbWriteTableOptions,
    },
    version_utils::{FileMetadata, SeeksBetweenCompactionOptions},
};


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableCacheKey {
    table_file_number: u64,
}

// Lint does not trigger because it's `pub(crate)`
// #[expect(
//     missing_debug_implementations,
//     reason = "too tedious to implement for this type; plus, it's a transient struct",
// )]
pub(crate) struct TableFileBuilder<LDBG: LevelDBGenerics, FS: Borrow<LDBG::FSCell>> {
    fs:          FS,
    file_number: u64,
    table_path:  PathBuf,
    builder:     LdbTableBuilder<LDBG>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, FS: Borrow<LDBG::FSCell>> TableFileBuilder<LDBG, FS> {
    pub fn new_or_reuse<'a>(
        builder:           &'a mut Option<Self>,
        db_directory:      &Path,
        filesystem:        FS,
        write_opts:        LdbWriteTableOptions<LDBG>,
        table_file_number: u64,
    ) -> Result<&'a mut Self, <LDBG::FS as ReadableFilesystem>::Error> {
        Ok(if let Some(builder) = builder {
            builder.reuse_as_new(db_directory, table_file_number)?;
            builder
        } else {
            builder.insert(Self::new(
                db_directory,
                filesystem,
                write_opts,
                table_file_number,
            )?)
        })
    }

    pub fn new(
        db_directory:      &Path,
        filesystem:        FS,
        write_opts:        LdbWriteTableOptions<LDBG>,
        table_file_number: u64,
    ) -> Result<Self, <LDBG::FS as ReadableFilesystem>::Error> {
        let file_number = table_file_number;
        let table_filename = LevelDBFileName::Table { file_number }.file_name();
        let table_path = db_directory.join(table_filename);

        let mut fs_ref = filesystem.borrow().write();
        let fs: &mut LDBG::FS = &mut fs_ref;
        let table_file = fs.open_writable(&table_path, false)?;
        drop(fs_ref);

        let table_builder = TableBuilder::new(write_opts, table_file);

        Ok(Self {
            fs:         filesystem,
            table_path,
            file_number,
            builder:    table_builder,
        })
    }

    pub fn reuse_as_new(
        &mut self,
        db_directory:      &Path,
        table_file_number: u64,
    ) -> Result<(), <LDBG::FS as ReadableFilesystem>::Error> {
        let file_number = table_file_number;
        let table_filename = LevelDBFileName::Table { file_number }.file_name();
        let table_path = db_directory.join(table_filename);

        let mut fs_ref = self.fs.borrow().write();
        let fs: &mut LDBG::FS = &mut fs_ref;
        let table_file = fs.open_writable(&table_path, false)?;
        drop(fs_ref);

        self.table_path  = table_path;
        self.file_number = file_number;
        self.builder.reuse_as_new(table_file);
        Ok(())
    }

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
    pub fn add_entry(
        &mut self,
        key:   EncodedInternalKey<'_>,
        value: UserValue<'_>,
    ) -> Result<(), ()> {
        self.builder.add_entry(key.0, value.0)
    }

    /// Finish writing the entire table to the table file and sync the file to persistent storage.
    ///
    /// On success, the total number of bytes written to the table file is returned.
    ///
    /// After this method is called, no other [`TableFileBuilder`] methods should be called other
    /// than [`Self::reuse_as_new`] or [`Self::new_or_reuse`]. See the type-level documentation for
    /// more.
    pub fn finish(
        &mut self,
        db_directory: &Path,
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
            db_directory,
            self.fs.borrow(),
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

    /// Must only be called during `Self::finish` if an error is encountered, or if `self`
    /// is dropped without being finished.
    fn delete_table_file(&self) {
        // Acquire the FS lock
        let mut fs_ref = self.fs.borrow().write();
        #[expect(
            let_underscore_drop,
            clippy::let_underscore_must_use,
            reason = "ignore any error which occurs while handling the root error",
        )]
        let _: Result<_, _> = fs_ref.delete(&self.table_path);
    }
}

impl<LDBG: LevelDBGenerics, FS: Borrow<LDBG::FSCell>> Drop for TableFileBuilder<LDBG, FS> {
    fn drop(&mut self) {
        if !thread::panicking() {
            self.delete_table_file();
        }
    }
}

pub(crate) fn build_table<LDBG: LevelDBGenerics>(
    db_directory:      &Path,
    filesystem:        &LDBG::FSCell,
    table_cache:       &LDBG::TableCache,
    table_opts:        LdbTableOptions<LDBG>,
    seek_opts:         SeeksBetweenCompactionOptions,
    memtable:          &Memtable<LDBG::Cmp, LDBG::Skiplist>,
    table_file_number: u64,
) -> Result<Option<FileMetadata>, ()> {
    let mut memtable_iter = memtable.iter();
    let Some(mut current) = memtable_iter.next() else {
        // If the memtable is completely empty, there's no need to create a table file for it.
        return Ok(None);
    };

    let (read_opts, write_opts) = table_opts.split();

    #[expect(clippy::map_err_ignore, reason = "temporary")]
    let mut builder = TableFileBuilder::<LDBG, _>::new(
        db_directory,
        filesystem,
        write_opts,
        table_file_number,
    ).map_err(|_| ())?;

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
        db_directory,
        table_cache,
        read_opts,
        seek_opts,
        smallest_key,
        largest_key,
    )?;

    Ok(Some(metadata))
}

pub(crate) fn get_table<LDBG: LevelDBGenerics>(
    db_directory:      &Path,
    filesystem:        &LDBG::FSCell,
    table_cache:       &LDBG::TableCache,
    read_opts:         LdbReadTableOptions<LDBG>,
    table_file_number: u64,
    file_size:         u64,
) -> Result<LdbTableContainer<LDBG>, ()> {
    let cache_key = TableCacheKey { table_file_number };

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

    let table = Table::new(read_opts, table_file, file_size, table_file_number)?;
    let table_container = LdbTableContainer::<LDBG>::new_container(table);

    table_cache.insert(cache_key, &table_container);

    Ok(table_container)
}

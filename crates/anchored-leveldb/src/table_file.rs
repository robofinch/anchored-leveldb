use std::path::Path;

use generic_container::FragileTryContainer as _;

use anchored_sstable::{ReadTableOptions, Table, TableBuilder};
use anchored_sstable::options::KVCache as _;
use anchored_vfs::traits::{ReadableFilesystem as _, WritableFilesystem};

use crate::{containers::RwContainer as _, version_edit::FileMetadata};
use crate::{
    format::{EncodedMemtableEntry, LevelDBFileName},
    leveldb_generics::{LevelDBGenerics, ReadTableOpts, TableContainer, TableOpts},
    memtable::{Memtable, MemtableIter},
};


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableCacheKey {
    table_file_number: u64,
}

pub(crate) fn build_table<LDBG: LevelDBGenerics>(
    db_directory:      &Path,
    filesystem:        &LDBG::FSContainer,
    table_cache:       &LDBG::TableCache,
    table_opts:        TableOpts<LDBG>,
    memtable:          &Memtable<LDBG::Cmp, LDBG::Skiplist>,
    table_file_number: u64,
) -> Result<Option<FileMetadata>, ()> {
    let mut memtable_iter = memtable.iter();
    let Some(current) = memtable_iter.next() else {
        // If the memtable is completely empty, there's no need to create a table file for it.
        return Ok(None);
    };

    let file_number = table_file_number;
    let table_filename = LevelDBFileName::Table { file_number }.file_name();
    let table_path = db_directory.join(table_filename);

    let mut fs_ref = filesystem.write();
    let fs: &mut LDBG::FS = &mut fs_ref;
    let table_file = fs.open_writable(&table_path, false).map_err(|_| ())?;
    drop(fs_ref);

    // We use an inner function below in order to ensure that we perform cleanup if an error
    // is encountered.
    return match inner_build_table::<LDBG>(
        db_directory, filesystem, table_cache, table_opts,
        memtable_iter, current, table_file, file_number,
    ) {
        Ok(file_metadata) => Ok(Some(file_metadata)),
        Err(err) => {
            // Re-acquire the FS lock
            fs_ref = filesystem.write();
            #[expect(
                let_underscore_drop,
                clippy::let_underscore_must_use,
                reason = "ignore any error which occurs while handling the root error",
            )]
            let _: Result<_, _> = fs_ref.delete(&table_path);
            Err(err)
        }
    };

    #[expect(
        clippy::items_after_statements,
        reason = "this is the last item, and is just below the one place it is called",
    )]
    #[expect(clippy::too_many_arguments, reason = "internal function")]
    fn inner_build_table<'a, LDBG: LevelDBGenerics>(
        db_directory:      &Path,
        filesystem:        &LDBG::FSContainer,
        table_cache:       &LDBG::TableCache,
        table_opts:        TableOpts<LDBG>,
        mut memtable_iter: MemtableIter<'a, LDBG::Cmp, LDBG::Skiplist>,
        mut current:       EncodedMemtableEntry<'a>,
        table_file:        <LDBG::FS as WritableFilesystem>::WriteFile,
        file_number:       u64,
    ) -> Result<FileMetadata, ()> {
        let smallest_key = current.internal_key();
        let mut table_builder = TableBuilder::new(table_opts.mirrored_write_opts(), table_file);

        let largest_key = loop {
            let (internal_key, user_value) = current.key_and_user_value();
            // Correctness: the memtable is sorted solely by internal key
            // (in the same way in which `InternalComparator<Cmp>` would sort the internal keys)
            // and does not have any entries with duplicate keys.
            table_builder.add_entry(internal_key.0, user_value.0)?;

            if let Some(next) = memtable_iter.next() {
                current = next;
            } else {
                break current.internal_key();
            }
        };

        let file_size = table_builder.finish(true)?;

        // Confirm that the produced table is actually usable
        let _table = get_table::<LDBG>(
            db_directory,
            filesystem,
            table_cache,
            ReadTableOptions::from(table_opts),
            file_number,
            file_size,
        )?;

        // TODO: better SSTable error handling; check that an iterator can be initialized.
        // let _ = Table::new_iter(table)?;

        Ok(FileMetadata::new(
            file_number,
            file_size,
            smallest_key,
            largest_key,
        ))
    }
}

pub(crate) fn get_table<LDBG: LevelDBGenerics>(
    db_directory:      &Path,
    filesystem:        &LDBG::FSContainer,
    table_cache:       &LDBG::TableCache,
    table_opts:        ReadTableOpts<LDBG>,
    table_file_number: u64,
    file_size:         u64,
) -> Result<TableContainer<LDBG>, ()> {
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

    let table = Table::new(table_opts, table_file, file_size, table_file_number)?;
    let table_container = TableContainer::<LDBG>::new_container(table);

    table_cache.insert(cache_key, &table_container);

    Ok(table_container)
}

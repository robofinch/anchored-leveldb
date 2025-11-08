use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    path::{Path, PathBuf},
};

use clone_behavior::MirroredClone as _;
use generic_container::FragileTryContainer as _;
use seekable_iterator::{CursorLendingIterator as _, Seekable as _};

use anchored_sstable::{perf_options::KVCache as _, Table};
use anchored_vfs::traits::ReadableFilesystem as _;

use crate::{
    database_files::LevelDBFileName,
    leveldb_iter::InternalIterator,
    table_traits::adapters::InternalComparator,
};
use crate::{
    containers::{DebugWrapper, FragileRwCell as _},
    format::{EncodedInternalEntry, EncodedInternalKey, FileNumber, LookupKey},
    leveldb_generics::{
        LdbContainer, LdbFsCell, LdbOptionalTableIter, LdbPooledBuffer, LdbReadTableOptions,
        LdbTableIter, LdbTableContainer, LevelDBGenerics,
    },
};
use super::TableCacheKey;


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
        return Ok(table_container.0);
    }

    let file_number = table_file_number;
    let table_path = LevelDBFileName::Table { file_number }.file_path(db_directory);

    let fs_ref = filesystem.read();
    let fs: &LDBG::FS = &fs_ref;

    let table_file = match fs.open_random_access(&table_path) {
        Ok(file)         => file,
        Err(_first_error) => {
            // Try opening the legacy path
            let sst_path = LevelDBFileName::TableLegacyExtension { file_number }
                .file_path(db_directory);

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

    table_cache.insert(cache_key, DebugWrapper::from_ref(&table_container));

    Ok(table_container)
}

fn to_internal_entry<'a>((key, value): (&'a [u8], &'a [u8])) -> EncodedInternalEntry<'a> {
    // TODO: validate that corruption has not compromised the `key`

    EncodedInternalEntry::new(EncodedInternalKey(key), value)
}

pub(crate) struct InternalTableIter<LDBG: LevelDBGenerics>(LdbTableIter<LDBG>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics> InternalTableIter<LDBG> {
    #[must_use]
    pub fn new(table: LdbTableContainer<LDBG>) -> Self {
        Self(LdbTableIter::<LDBG>::new(table))
    }
}

impl<LDBG: LevelDBGenerics> InternalIterator<LDBG::Cmp> for InternalTableIter<LDBG> {
    fn valid(&self) -> bool {
        self.0.valid()
    }

    fn next(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.0.next().map(to_internal_entry)
    }

    fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.0.current().map(to_internal_entry)
    }

    fn prev(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.0.prev().map(to_internal_entry)
    }

    fn reset(&mut self) {
        self.0.reset();
    }

    fn seek(&mut self, min_bound: LookupKey<'_>) {
        self.0.seek(min_bound.encoded_internal_key().0);
    }

    fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        self.0.seek_before(strict_upper_bound.encoded_internal_key().0);
    }

    fn seek_to_first(&mut self) {
        self.0.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.0.seek_to_last();
    }
}

impl<LDBG> Debug for InternalTableIter<LDBG>
where
    LDBG:                    LevelDBGenerics,
    LDBG::Cmp:               Debug,
    LdbPooledBuffer<LDBG>:   Debug,
    LdbTableContainer<LDBG>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("InternalTableIter").field(&self.0).finish()
    }
}

pub(crate) struct InternalOptionalTableIter<LDBG: LevelDBGenerics> {
    shared_data: LdbContainer<LDBG, (LdbFsCell<LDBG>, PathBuf, LDBG::TableCache, LdbReadTableOptions<LDBG>)>,
    iter:        LdbOptionalTableIter<LDBG>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics> InternalOptionalTableIter<LDBG> {
    #[must_use]
    pub fn new_empty(shared_data: LdbContainer<LDBG, (LdbFsCell<LDBG>, PathBuf, LDBG::TableCache, LdbReadTableOptions<LDBG>)>) -> Self {
        let cmp = shared_data.3.comparator.mirrored_clone();
        Self {
            shared_data,
            iter: LdbOptionalTableIter::<LDBG>::new_empty(cmp),
        }
    }

    #[must_use]
    pub const fn is_set(&self) -> bool {
        self.iter.is_set()
    }

    pub fn clear(&mut self) {
        self.iter.clear();
    }

    pub fn set(&mut self, table_file_number: FileNumber, table_file_size: u64) {
        self.iter.clear();
        let table = get_table::<LDBG>(
            &self.shared_data.0,
            &self.shared_data.1,
            &self.shared_data.2,
            self.shared_data.3.fast_clone(),
            table_file_number,
            table_file_size,
        ).expect("TODO: do proper error handling in iterators");

        self.iter.set(table);
    }

    pub fn comparator(&self) -> &InternalComparator<LDBG::Cmp> {
        &self.shared_data.3.comparator
    }
}

impl<LDBG: LevelDBGenerics> InternalIterator<LDBG::Cmp> for InternalOptionalTableIter<LDBG> {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn next(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.next().map(to_internal_entry)
    }

    fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.current().map(to_internal_entry)
    }

    fn prev(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.iter.prev().map(to_internal_entry)
    }

    fn reset(&mut self) {
        self.iter.reset();
    }

    fn seek(&mut self, min_bound: LookupKey<'_>) {
        self.iter.seek(min_bound.encoded_internal_key().0);
    }

    fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        self.iter.seek_before(strict_upper_bound.encoded_internal_key().0);
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }
}

impl<LDBG> Debug for InternalOptionalTableIter<LDBG>
where
    LDBG:                    LevelDBGenerics,
    LDBG::Cmp:               Debug,
    LdbPooledBuffer<LDBG>:   Debug,
    LdbTableContainer<LDBG>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InternalOptionalTableIter")
            .field("shared_data", &"(shared database options)")
            .field("iter",        &self.iter)
            .finish()
    }
}

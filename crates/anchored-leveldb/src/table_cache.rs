use std::{marker::PhantomData, path::PathBuf};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{AnySpeed, ConstantTime, LogTime, MirroredClone};
use generic_container::FragileContainer;

use anchored_sstable::{ReadTableOptions, Table};
use anchored_sstable::options::{
    BlockCacheKey, BufferPool, CompressorList, KVCache, TableComparator, TableFilterPolicy,
};
use anchored_vfs::traits::{RandomAccess, ReadableFilesystem};

use crate::format::LevelDBFileName;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableCacheKey {
    pub(crate) table_file_number: u64,
}

// TODO: is this truly deserving of its own struct, or should it be a function somewhere else?
// Maybe it should be part of some larger `Shared` struct.
#[derive(Debug, Clone)]
pub(crate) struct TableCache<PathContainer, ReadableFS, InnerTableCache> {
    db_directory_path: PathContainer,
    filesystem:        ReadableFS,
    cache:             InnerTableCache,
}

impl<PathContainer, ReadableFS, InnerTableCache>
    TableCache<PathContainer, ReadableFS, InnerTableCache>
{
    #[inline]
    #[must_use]
    pub fn new(
        db_directory_path: PathContainer,
        filesystem:        ReadableFS,
        cache:             InnerTableCache,
    ) -> Self {
        Self {
            db_directory_path,
            filesystem,
            cache,
        }
    }
}

impl<PathContainer, ReadableFS, InnerTableCache>
    TableCache<PathContainer, ReadableFS, InnerTableCache>
{
    pub fn get_table<TableContainer, CompList, TablePolicy, TableCmp, BlockCache, Pool>(
        &self,
        opts:              ReadTableOptions<CompList, TablePolicy, TableCmp, BlockCache, Pool>,
        table_file_number: u64,
        file_size:         u64,
    ) -> Result<TableContainer, ()>
    where
        PathContainer:   FragileContainer<PathBuf>,
        ReadableFS:      ReadableFilesystem,
        InnerTableCache: KVCache<TableCacheKey, TableContainer>,
        TableContainer:  FragileContainer<
            Table<CompList, TablePolicy, TableCmp, ReadableFS::RandomAccessFile, BlockCache, Pool>,
        >,
        CompList:        FragileContainer<CompressorList>,
        TablePolicy:     TableFilterPolicy,
        TableCmp:        TableComparator + MirroredClone<ConstantTime>,
        BlockCache:      KVCache<BlockCacheKey, Pool::PooledBuffer>,
        Pool:            BufferPool,
    {
        let cache_key = TableCacheKey { table_file_number };

        if let Some(table_container) = self.cache.get(&cache_key) {
            return Ok(table_container);
        }

        let file_number = table_file_number;
        let table_path = LevelDBFileName::Table { file_number }.file_name();

        let table_file = match self.filesystem.open_random_access(&table_path) {
            Ok(file)         => file,
            Err(_first_error) => {
                // Try opening the legacy path
                let table_path = LevelDBFileName::TableLegacyExtension { file_number }.file_name();

                if let Ok(file) = self.filesystem.open_random_access(&table_path) {
                    file
                } else {
                    // TODO: return error based on `_first_error`
                    return Err(());
                }
            }
        };

        let table = Table::new(opts, table_file, file_size, table_file_number)?;
        let table_container = TableContainer::new_container(table);

        self.cache.insert(cache_key, &table_container);

        Ok(table_container)
    }
}

macro_rules! mirrored_clone {
    ($($speed:ident),*) => {
        $(
            impl<PathContainer, ReadableFS, InnerTableCache> MirroredClone<$speed>
            for TableCache<PathContainer, ReadableFS, InnerTableCache>
            where
                PathContainer:    MirroredClone<$speed>,
                ReadableFS:       MirroredClone<$speed>,
                InnerTableCache:  MirroredClone<$speed>,
            {
                fn mirrored_clone(&self) -> Self {
                    Self {
                        db_directory_path: self.db_directory_path.mirrored_clone(),
                        filesystem:        self.filesystem.mirrored_clone(),
                        cache:             self.cache.mirrored_clone(),
                    }
                }
            }
        )*
    };
}

mirrored_clone!(ConstantTime, LogTime, AnySpeed);

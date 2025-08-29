use std::borrow::BorrowMut as _;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator as _, Seekable as _};

use anchored_vfs::traits::RandomAccess;

use crate::block::TableBlock;
use crate::cache::{CacheDebugAdapter, CacheKey, TableBlockCache};
use crate::comparator::{ComparatorAdapter, MetaindexComparator, TableComparator};
use crate::compressors::CompressorList;
use crate::filter::FilterPolicy;
use crate::filter_block::FilterBlockReader;
use crate::pool::BufferPool;
use super::format::{BlockHandle, TableFooter};
use super::iter::TableIter;
use super::read::TableBlockReader;


// TODO: impl Default for Options structs

#[derive(Debug, Clone)]
pub struct ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool> {
    pub compressor_list:  CompList,
    pub policy:           Policy,
    pub comparator:       TableCmp,
    pub verify_checksums: bool,
    pub block_cache:      Option<Cache>,
    pub buffer_pool:      Pool,
}

pub struct Table<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool> {
    compressor_list:  CompList,
    verify_checksums: bool,
    buffer_pool:      Pool,

    file:             File,
    metaindex_offset: u64,

    block_cache:      Option<CacheDebugAdapter<Cache, Pool::PooledBuffer>>,
    #[allow(clippy::struct_field_names, reason = "clarify what the ID identifies")]
    table_id:         u64,

    index_block:      TableBlock<Pool::PooledBuffer, TableCmp>,
    filter_block:     Option<FilterBlockReader<Policy, Pool::PooledBuffer>>,
}

#[expect(
    clippy::result_unit_err, clippy::map_err_ignore,
    reason = "temporary. TODO: return actual errors.",
)]
impl<CompList, Policy, TableCmp, File, Cache, Pool>
    Table<CompList, Policy, TableCmp, File, Cache, Pool>
where
    CompList: FragileContainer<CompressorList>,
    Policy:   FilterPolicy,
    TableCmp: TableComparator + MirroredClone<ConstantTime>,
    File:     RandomAccess,
    Cache:    TableBlockCache<Pool::PooledBuffer>,
    Pool:     BufferPool,
{
    pub fn new(
        opts:      ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>,
        file:      File,
        file_size: u64,
        table_id:  u64,
    ) -> Result<Self, ()> {
        let mut scratch_buffer = opts.buffer_pool.get_buffer();
        let scratch_buffer: &mut Vec<u8> = scratch_buffer.borrow_mut();
        #[expect(clippy::as_conversions, reason = "the constant is far less than `u64::MAX`")]
        file.read_exact_at(
            file_size - TableFooter::ENCODED_LENGTH as u64,
            scratch_buffer,
        ).map_err(|_| ())?;
        let footer = TableFooter::decode_from(&*scratch_buffer)?;
        scratch_buffer.clear();

        let mut block_reader = TableBlockReader {
            file:             &file,
            compressor_list:  &opts.compressor_list,
            verify_checksums: opts.verify_checksums,
            buffer_pool:      &opts.buffer_pool,
            scratch_buffer,
        };

        let mut block_buffer = opts.buffer_pool.get_buffer();
        block_reader.read_table_block(footer.metaindex, block_buffer.borrow_mut())?;
        let metaindex_cmp = ComparatorAdapter(MetaindexComparator::new());
        let metaindex_block = TableBlock::new(block_buffer, metaindex_cmp);

        let filter_block = block_reader.read_filter_block(opts.policy, &metaindex_block)?;

        block_buffer = metaindex_block.contents;
        block_buffer.borrow_mut().clear();

        block_reader.read_table_block(footer.index, block_buffer.borrow_mut())?;
        let index_block = TableBlock::new(
            block_buffer,
            ComparatorAdapter(opts.comparator),
        );

        Ok(Self {
            compressor_list:  opts.compressor_list,
            verify_checksums: opts.verify_checksums,
            buffer_pool:      opts.buffer_pool,
            file,
            metaindex_offset: footer.metaindex.offset,
            block_cache:      opts.block_cache.map(CacheDebugAdapter::new),
            table_id,
            index_block,
            filter_block,
        })
    }

    // pub fn get

    #[expect(clippy::should_implement_trait, reason = "the iterator is a lending iterator")]
    #[inline]
    #[must_use]
    pub fn into_iter(self) -> TableIter<CompList, Policy, TableCmp, File, Cache, Pool, Self> {
        TableIter::new(self)
    }

    #[inline]
    #[must_use]
    pub fn new_iter<TableContainer>(
        table_container: TableContainer,
    ) -> TableIter<CompList, Policy, TableCmp, File, Cache, Pool, TableContainer>
    where
        TableContainer: FragileContainer<Self>,
    {
        TableIter::new(table_container)
    }

    pub fn approximate_offset_of_key(&self, key: &[u8]) -> u64 {
        let mut iter = self.index_block.iter();
        iter.seek(key);

        if let Some((_, block_handle)) = iter.current() {
            if let Ok((block_handle, _)) = BlockHandle::decode_from(block_handle) {
                return block_handle.offset;
            }
        }

        // Either the `key` is greater than the largest key in this table,
        // or the index block is corrupt. The `metaindex_offset` is the offset just after the
        // last data block, so at least if the index block isn't corrupt, the answer is reasonable.
        self.metaindex_offset
    }

    /// Used by [`TableIter`].
    pub(super) const fn comparator(&self) -> &ComparatorAdapter<TableCmp> {
        &self.index_block.cmp
    }

    /// Used by [`TableIter`].
    pub(super) const fn index_block(&self) -> &TableBlock<Pool::PooledBuffer, TableCmp> {
        &self.index_block
    }

    /// Read and cache the block with the given encoded handle, and return the block contents on
    /// success.
    ///
    /// Used by [`TableIter`].
    pub(super) fn read_block_from_encoded_handle(
        &self,
        encoded_handle: &[u8],
    ) -> Result<Pool::PooledBuffer, ()> {
        let (handle, read_len) = BlockHandle::decode_from(encoded_handle)?;
        if read_len == encoded_handle.len() {
            self.read_block(handle)
        } else {
            Err(())
        }
    }

    /// Read and cache the block with the given handle, and return the block contents on success.
    ///
    /// Used by [`TableIter`].
    pub(super) fn read_block(&self, handle: BlockHandle) -> Result<Pool::PooledBuffer, ()> {
        let cache_key = CacheKey {
            table_id:      self.table_id,
            handle_offset: handle.offset,
        };

        if let Some(cache) = self.block_cache.as_ref() {
            if let Some(block) = cache.get(&cache_key) {
                return Ok(block);
            }
        }

        let mut scratch_buffer = self.buffer_pool.get_buffer();
        let mut block_reader = TableBlockReader {
            file:             &self.file,
            compressor_list:  &self.compressor_list,
            verify_checksums: self.verify_checksums,
            buffer_pool:      &self.buffer_pool,
            scratch_buffer:   scratch_buffer.borrow_mut(),
        };

        let mut block_buffer = self.buffer_pool.get_buffer();
        block_reader.read_table_block(handle, block_buffer.borrow_mut())?;

        if let Some(cache) = self.block_cache.as_ref() {
            cache.insert(cache_key, &block_buffer);
        }

        Ok(block_buffer)
    }
}

impl<CompList, Policy, TableCmp, File, Cache, Pool> Debug
for Table<CompList, Policy, TableCmp, File, Cache, Pool>
where
    CompList:           Debug,
    Policy:             Debug,
    TableCmp:           Debug,
    File:               Debug,
    Cache:              TableBlockCache<Pool::PooledBuffer>,
    Pool:               Debug + BufferPool,
    Pool::PooledBuffer: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Table")
            .field("compressor_list",  &self.compressor_list)
            .field("verify_checksums", &self.verify_checksums)
            .field("buffer_pool",      &self.buffer_pool)
            .field("file",             &self.file)
            .field("metaindex_offset", &self.metaindex_offset)
            .field("block_cache",      &self.block_cache)
            .field("table_id",         &self.table_id)
            .field("index_block",      &self.index_block)
            .field("filter_block",     &self.filter_block)
            .finish()
    }
}

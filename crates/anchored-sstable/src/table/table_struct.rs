use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator as _, Seekable as _};

use anchored_vfs::traits::RandomAccess;

use crate::block::{BlockContentsContainer, TableBlock};
use crate::comparator::{ComparatorAdapter, MetaindexComparator, TableComparator};
use crate::compressors::CompressorList;
use crate::filter::FilterPolicy;
use crate::filter_block::FilterBlockReader;
use super::cache::{CacheDebugAdapter, CacheKey, TableBlockCache};
use super::format::{BlockHandle, TableFooter};
// use super::iter::TableIter;
use super::read::TableBlockReader;


// TODO: impl Default for Options structs

#[derive(Debug, Clone)]
pub struct ReadTableOptions<CompList, Policy, TableCmp, Cache> {
    pub compressor_list:  CompList,
    pub policy:           Policy,
    pub comparator:       TableCmp,
    pub verify_checksums: bool,
    pub block_cache:      Option<Cache>,
}

pub struct Table<CompList, Policy, TableCmp, File, Cache, BlockContents> {
    compressor_list:  CompList,
    comparator:       TableCmp,
    verify_checksums: bool,

    file:             File,
    metaindex_offset: u64,

    block_cache:      Option<CacheDebugAdapter<Cache, BlockContents, TableCmp>>,
    #[allow(clippy::struct_field_names, reason = "clarify what the ID identifies")]
    table_id:         u64,

    index_block:      TableBlock<Vec<u8>, TableCmp>,
    filter_block:     Option<FilterBlockReader<Policy>>,
}

#[expect(
    clippy::result_unit_err, clippy::map_err_ignore,
    reason = "temporary. TODO: return actual errors.",
)]
impl<CompList, Policy, TableCmp, File, Cache, BlockContents>
    Table<CompList, Policy, TableCmp, File, Cache, BlockContents>
where
    CompList:       FragileContainer<CompressorList>,
    Policy:         FilterPolicy,
    TableCmp:       TableComparator + MirroredClone<ConstantTime>,
    File:           RandomAccess,
    Cache:          TableBlockCache<BlockContents, TableCmp>,
    BlockContents:  BlockContentsContainer,
{
    pub fn new(
        opts:      ReadTableOptions<CompList, Policy, TableCmp, Cache>,
        file:      File,
        file_size: u64,
        table_id:  u64,
    ) -> Result<Self, ()> {
        let mut scratch_buffer = vec![0; TableFooter::ENCODED_LENGTH];
        #[expect(clippy::as_conversions, reason = "the constant is far less than `u64::MAX`")]
        file.read_exact_at(
            file_size - TableFooter::ENCODED_LENGTH as u64,
            &mut scratch_buffer,
        ).map_err(|_| ())?;
        let footer = TableFooter::decode_from(&scratch_buffer)?;
        scratch_buffer.clear();

        let mut block_reader = TableBlockReader {
            file:             &file,
            compressor_list:  &opts.compressor_list,
            verify_checksums: opts.verify_checksums,
            scratch_buffer:   &mut scratch_buffer,
        };

        let mut block_buffer = Vec::new();
        block_reader.read_table_block(footer.metaindex, &mut block_buffer)?;
        let metaindex_cmp = ComparatorAdapter(MetaindexComparator::new());
        let metaindex_block = TableBlock::new(block_buffer, metaindex_cmp);

        let filter_block = block_reader.read_filter_block(opts.policy, &metaindex_block)?;

        block_buffer = metaindex_block.contents;
        block_buffer.clear();

        block_reader.read_table_block(footer.index, &mut block_buffer)?;
        let index_block = TableBlock::new(
            block_buffer,
            ComparatorAdapter(opts.comparator.mirrored_clone()),
        );

        Ok(Self {
            compressor_list:  opts.compressor_list,
            comparator:       opts.comparator,
            verify_checksums: opts.verify_checksums,
            file,
            metaindex_offset: footer.metaindex.offset,
            block_cache:      opts.block_cache.map(CacheDebugAdapter::new),
            table_id,
            index_block,
            filter_block,
        })
    }

    // #[inline]
    // #[must_use]
    // pub fn new_iter<TableContainer>(
    //     table_container: TableContainer,
    // ) -> TableIter<CompList, Policy, TableCmp, File, Cache, BlockContents, TableContainer>
    // where
    //     TableContainer: FragileContainer<Self> + MirroredClone<ConstantTime>,
    // {
    //     TableIter::new(table_container)
    // }

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

    #[expect(dead_code, reason = "will be used by table iter")]
    pub(super) const fn comparator(&self) -> &TableCmp {
        &self.comparator
    }

    pub(super) const fn index_block(&self) -> &TableBlock<Vec<u8>, TableCmp> {
        &self.index_block
    }

    /// Read and cache the block with the given handle.
    ///
    /// The given `scratch_buffer` must be empty.
    ///
    /// # Errors
    /// If an error is returned, the contents of `scratch_buffer` should be considered unknown.
    #[expect(dead_code, reason = "will be used by table iter")]
    pub(super) fn read_block(
        &self,
        scratch_buffer: &mut Vec<u8>,
        handle:         BlockHandle,
    ) -> Result<TableBlock<BlockContents, TableCmp>, ()> {
        let cache_key = CacheKey {
            table_id:      self.table_id,
            handle_offset: handle.offset,
        };

        if let Some(cache) = self.block_cache.as_ref() {
            if let Some(block) = cache.get(&cache_key) {
                return Ok(block);
            }
        }

        let mut block_reader = TableBlockReader {
            file:             &self.file,
            compressor_list:  &self.compressor_list,
            verify_checksums: self.verify_checksums,
            scratch_buffer,
        };

        let mut block_buffer = Vec::new();
        block_reader.read_table_block(handle, &mut block_buffer)?;

        let block = TableBlock::new(
            BlockContents::new_container(block_buffer),
            ComparatorAdapter(self.comparator.mirrored_clone()),
        );

        if let Some(cache) = self.block_cache.as_ref() {
            cache.insert(cache_key, &block);
        }

        Ok(block)
    }
}

impl<CompList, Policy, TableCmp, File, Cache, BlockContents> Debug
for Table<CompList, Policy, TableCmp, File, Cache, BlockContents>
where
    CompList:       Debug,
    Policy:         Debug,
    TableCmp:       Debug,
    File:           Debug,
    Cache:          TableBlockCache<BlockContents, TableCmp>,
    BlockContents:  Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Table")
            .field("compressor_list",  &self.compressor_list)
            .field("comparator",       &self.comparator)
            .field("verify_checksums", &self.verify_checksums)
            .field("file",             &self.file)
            .field("metaindex_offset", &self.metaindex_offset)
            .field("block_cache",      &self.block_cache)
            .field("table_id",         &self.table_id)
            .field("index_block",      &self.index_block)
            .field("filter_block",     &self.filter_block)
            .finish()
    }
}

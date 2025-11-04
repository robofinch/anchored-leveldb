use std::{
    borrow::{Borrow as _, BorrowMut as _},
    fmt::{Debug, Formatter, Result as FmtResult},
};

use clone_behavior::{Fast, MirroredClone};
use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator as _, Seekable as _};

use anchored_vfs::traits::RandomAccess;

use crate::{
    block::TableBlock,
    compressors::CompressorList,
    filters::TableFilterPolicy,
    filter_block::FilterBlockReader,
    iter::BlockIterImpl,
    option_structs::ReadTableOptions,
    pool::BufferPool,
};
use crate::{
    caches::{BlockCacheKey, CacheDebugAdapter, KVCache},
    comparator::{ComparatorAdapter, MetaindexComparator, TableComparator},
};
use super::{entry::TableEntry, iter::TableIter, read::TableBlockReader};
use super::format::{BlockHandle, TableFooter};


pub struct Table<CompList, Policy, TableCmp, File, Cache, Pool: BufferPool> {
    compressor_list:  CompList,
    verify_checksums: bool,
    buffer_pool:      Pool,

    file:             File,
    file_number:      u64,
    metaindex_offset: u64,

    block_cache:      CacheDebugAdapter<Cache, BlockCacheKey, Pool::PooledBuffer>,
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
    Policy:   TableFilterPolicy,
    TableCmp: TableComparator + MirroredClone<Fast>,
    File:     RandomAccess,
    Cache:    KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:     BufferPool,
{
    // TODO: make sure that if the persistent data is sorted incorrectly, panics cannot occur.
    /// It is not checked that the indicated `TableCmp` is correct. If the wrong [`TableComparator`]
    /// is used for the opened table, then the table might appear to be corrupt, or entries simply
    /// won't be found.
    pub fn new(
        opts:              ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool>,
        file:              File,
        file_size:         u64,
        table_file_number: u64,
    ) -> Result<Self, ()> {
        // We need to read the footer and the index block, at the very least.
        // Additionally, if a `Policy` was selected, then we need to read the metaindex block
        // and filter block.
        // Because we only need the metaindex block temporarily (if at all), we can reuse the
        // buffer for the index block.

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

        let filter_block = if let Some(policy) = opts.filter_policy {
            block_reader.read_table_block(footer.metaindex, block_buffer.borrow_mut())?;
            let metaindex_cmp = ComparatorAdapter(MetaindexComparator);
            let metaindex_block = TableBlock::new(block_buffer, metaindex_cmp);

            let filter_block = block_reader.read_filter_block(policy, &metaindex_block)?;

            block_buffer = metaindex_block.contents;
            block_buffer.borrow_mut().clear();

            filter_block
        } else {
            None
        };

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
            file_number:      table_file_number,
            metaindex_offset: footer.metaindex.offset,
            block_cache:      CacheDebugAdapter::new(opts.block_cache),
            index_block,
            filter_block,
        })
    }

    /// Attempt to get an entry with a key greater than or equal to `min_bound` which is, loosely
    /// speaking, "near" `min_bound`.
    ///
    /// If corruption is encountered or `TableCmp` or `Policy` incorrectly implemented its
    /// respective trait, then any output may be returned from this function. For example,
    /// unnoticed corruption in a filter may result in `Ok(None)` being incorrectly returned.
    /// The remaining description assumes that such an error does not occur; additionally,
    /// all below comparisons refer to the `TableCmp` comparator provided to this `Table`, which
    /// is assumed to be compatible with the `Policy` value (if `Some`).
    ///
    /// # Which entry is returned
    ///
    /// If an entry is returned as `Ok(Some(_))`, then it is the least entry in the `Table` whose
    /// key is greater than or equal to `min_bound`.
    ///
    /// # Will an entry be returned
    ///
    /// If an entry whose key compares equal to `min_bound` is in the `Table`, then it is
    /// returned as `Ok(Some(_))` (barring corruption or similar errors). This fact is implied
    /// by the below exhaustive conditions for `Ok(None)` to be returned.
    ///
    /// This method is guaranteed to return `Ok(None)` if there is no entry with a key greater
    /// than or equal to `min_bound`.
    ///
    /// If a `Policy` was provided to this `Table`, then:
    /// - If a `filter` were to be successfully generated with [`TableFilterPolicy::create_filter`]
    ///   from a list of keys which includes all keys in the `Table` which compare greater than or
    ///   equal to `min_bound`, then this function may or may not return `Ok(None)` if the described
    ///   `filter` would not match `min_bound`.
    /// - For any two adjacent entries in the `Table` with keys `from` and `to` such that
    ///   `from < to`, let `separator` be the result of applying `TableCmp::find_short_separator`
    ///   to `from` and `to`.
    ///   If a `filter` were to be successfully generated with [`TableFilterPolicy::create_filter`]
    ///   from a list of keys which includes all keys in the `Table` which compare greater than or
    ///   equal to `min_bound` and less than or equal to `separator`, then this function may or may
    ///   not return `Ok(None)` if:
    ///   - the described `filter` would not match `min_bound`, and
    ///   - `min_bound <= separator`.
    ///
    /// This method may or may not return `Ok(None)` if there exist adjacent entries in the `Table`
    /// with keys `from` and `to` such that:
    /// - `from < min_bound`,
    /// - `min_bound < to`, and
    /// - where `separator` is the result of applying `TableCmp::find_short_separator` to
    ///   `from` and `to`, it holds that `min_bound <= separator`.
    ///
    /// # Errors
    /// May return `Err(_)` if corruption was encountered.
    ///
    /// # Policy-Comparator Compatibility
    /// The [`TableFilterPolicy`] and [`TableComparator`] of a [`Table`] are required to be
    /// compatible; in particular, if the equivalence relation of the [`TableComparator`] is looser
    /// than strict equality, the [`TableFilterPolicy`] must ensure that generated filters match
    /// not only the exact keys for which the filter was generated, but also any key which compares
    /// equal to a key the filter was generated for.
    pub fn get(&self, min_bound: &[u8]) -> Result<Option<TableEntry<Pool::PooledBuffer>>, ()> {
        let mut index_iter = self.index_block.iter();
        index_iter.seek(min_bound);

        // If this returns `None` in the `else` branch, then `min_bound` is past the last entry.
        if let Some((_, block_handle)) = index_iter.current() {
            let (handle, _) = BlockHandle::decode_from(block_handle)?;

            // In this branch, `[last_key_in_block] <= separator` and `min_bound <= separator`.
            // If there was a previous block, its keys are all strictly less than `min_bound`.
            // Any table entries which are greater than or equal to `min_bound` are thus in
            // this block and any following blocks.
            //
            // Suppose this is the last block. Then, this block contains all entries greater than
            // or equal to `min_bound` (and possibly more), so if the filter for that block does
            // not match and we return `None`, we are within one of the declared cases where
            // `Ok(None)` might be returned.
            //
            // Otherwise, there is a following block, so `separator` was returned from
            // `TableCmp::find_short_separator` applied to `[last_key_in_block]` and an adjacent,
            // strictly-greater `[some_key_in_next_block]`.
            //
            // The block whose filter we check contains all table entries up to and including
            // `separator`, and there are no entries strictly less than `min_bound` which we miss.
            // If the filter for keys including everything between `min_bound` and `separator`,
            // inclusive, does not match `min_bound` and we return `None`, then we are within
            // one of the declared cases where `Ok(None)` might be returned.
            if self.filter_block.as_ref().is_some_and(|filter_block| {
                !filter_block.key_may_match(handle.offset, min_bound)
            }) {
                return Ok(None);
            }

            let block_buffer = self.read_block(handle)?;
            let block_contents = block_buffer.borrow();
            let mut block_iter = BlockIterImpl::new(block_contents);

            block_iter.seek(block_contents, self.comparator(), min_bound);

            // If `TableEntry::new` returns `None`, then:
            // - `[some_key_in_block] < min_bound` for each key in this block, else we'd have
            //   found and returned the entry corresponding to a GEQ key;
            // - if there is not a next block, `min_bound` is strictly past the last entry
            //   in the `Table`;
            // - if there is a next block,
            //   `last_key_in_block < min_bound <= separator < first_key_in_next_block`
            // If this returns `Some`, then it's the first entry which is greater than or equal to
            // `min_bound` in the first block whose keys are not bounded above by an index strictly
            // less than `min_bound`.
            //
            // TLDR: we satisfy the documentation of this `Table::get` method.
            Ok(TableEntry::new(block_buffer, block_iter))
        } else {
            Ok(None)
        }
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
        let (handle, _) = BlockHandle::decode_from(encoded_handle)?;

        self.read_block(handle)
    }

    /// Read and cache the block with the given handle, and return the block contents on success.
    ///
    /// Used by [`TableIter`].
    pub(super) fn read_block(&self, handle: BlockHandle) -> Result<Pool::PooledBuffer, ()> {
        let cache_key = BlockCacheKey {
            table_file_number: self.file_number,
            handle_offset:     handle.offset,
        };

        if let Some(block) = self.block_cache.get(&cache_key) {
            return Ok(block);
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

        self.block_cache.insert(cache_key, &block_buffer);

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
    Cache:              KVCache<BlockCacheKey, Pool::PooledBuffer>,
    Pool:               Debug + BufferPool,
    Pool::PooledBuffer: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Table")
            .field("compressor_list",  &self.compressor_list)
            .field("verify_checksums", &self.verify_checksums)
            .field("buffer_pool",      &self.buffer_pool)
            .field("file",             &self.file)
            .field("file_number",      &self.file_number)
            .field("metaindex_offset", &self.metaindex_offset)
            .field("block_cache",      &self.block_cache)
            .field("index_block",      &self.index_block)
            .field("filter_block",     &self.filter_block)
            .finish()
    }
}

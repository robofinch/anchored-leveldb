use generic_container::FragileContainer;

use anchored_vfs::traits::WritableFile;

use crate::{
    block::BlockBuilder, filters::FilterPolicy,
    filter_block::FilterBlockBuilder, option_structs::WriteTableOptions,
};
use crate::{
    comparator::{ComparatorAdapter, TableComparator},
    compressors::{CompressorList, NO_COMPRESSION},
};
use super::format::{BlockHandle, BLOCK_TRAILER_LEN, FILTER_META_PREFIX, TableFooter};


/// A `TableBuilder` is used to create a [`Table`] from data entries.
///
/// The data entries _must_ be added in the order of their keys under the provided `TableCmp`
/// comparator. The `TableBuilder` does not necessarily validate this behavior, and failing to
/// uphold that requirement may result in panics or an invalid [`Table`] being produced.
///
/// After all data in the table has been written, `self.finish()` should be called.
/// After calling `self.finish()`, the `TableBuilder` should be either dropped or have
/// `reuse_as_new(..)` called on it; all other `TableBuilder` methods should be assumed to be
/// potentially invalid after `self.finish()` is called, unless `reuse_as_new(..)` is called.
///
/// `reuse_as_new(..)` allows internal buffers to be reused, and should be preferred over
/// dropping the `TableBuilder` just to create a new one with the same options.
///
/// [`Table`]: crate::table::Table
#[derive(Debug)]
pub struct TableBuilder<CompList, Policy, TableCmp, File> {
    compressor_list:         CompList,
    selected_compressor:     u8,
    comparator:              TableCmp,

    table_file:              File,
    offset_in_file:          u64,
    num_entries:             usize,

    block_size:              usize,
    data_block:              BlockBuilder<ComparatorAdapter<TableCmp>>,
    index_block:             BlockBuilder<ComparatorAdapter<TableCmp>>,
    filter_block:            Option<FilterBlockBuilder<Policy>>,

    /// Should almost always be `empty()`, except while in direct use.
    short_scratch:           Vec<u8>,
    compression_scratch_buf: Vec<u8>,
}

/// Macro to explode `&mut self` into borrows to several fields.
///
/// Note that this uses `self.compression_scratch_buf`.
macro_rules! builder_write_block {
    ($builder:expr, $block_contents:expr, $compressor_id:expr $(,)?) => {
        Self::write_block(
            // mfw no view types
            &$builder.compressor_list,
            &mut $builder.table_file,
            &mut $builder.offset_in_file,
            &mut $builder.compression_scratch_buf,
            // Actual arguments
            $block_contents,
            $compressor_id,
        )
    };
}

#[expect(
    clippy::result_unit_err, clippy::map_err_ignore,
    reason = "temporary. TODO: return actual errors.",
)]
impl<CompList, Policy, TableCmp, File> TableBuilder<CompList, Policy, TableCmp, File>
where
    CompList: FragileContainer<CompressorList>,
    Policy:   FilterPolicy,
    TableCmp: TableComparator,
    File:     WritableFile,
{
    #[inline]
    #[must_use]
    pub fn new(opts: WriteTableOptions<CompList, Policy, TableCmp>, table_file: File) -> Self {
        let filter_block = opts.filter_policy.map(|policy| {
            let mut filter_block = FilterBlockBuilder::new(policy);
            filter_block.start_block(0);
            filter_block
        });

        Self {
            compressor_list:         opts.compressor_list,
            selected_compressor:     opts.selected_compressor,
            comparator:              opts.comparator,
            table_file,
            offset_in_file:          0,
            num_entries:             0,
            block_size:              opts.block_size,
            data_block:              BlockBuilder::new(opts.block_restart_interval),
            index_block:             BlockBuilder::new(opts.block_restart_interval),
            filter_block,
            short_scratch:           Vec::new(),
            compression_scratch_buf: Vec::new(),
        }
    }

    #[inline]
    #[must_use]
    pub fn reuse_as_new<OtherPolicy: FilterPolicy>(
        mut self,
        table_file: File,
        policy:     Option<OtherPolicy>,
    ) -> TableBuilder<CompList, OtherPolicy, TableCmp, File> {
        self.data_block.reset();
        self.index_block.reset();
        self.short_scratch.clear();
        self.compression_scratch_buf.clear();

        let filter_block = policy.map(|policy| {
            if let Some(filter_block) = self.filter_block {
                let mut filter_block = filter_block.reuse_as_new(policy);
                filter_block.start_block(0);
                filter_block
            } else {
                let mut filter_block = FilterBlockBuilder::new(policy);
                filter_block.start_block(0);
                filter_block
            }
        });

        TableBuilder {
            compressor_list:         self.compressor_list,
            selected_compressor:     self.selected_compressor,
            comparator:              self.comparator,
            table_file,
            offset_in_file:          0,
            num_entries:             0,
            block_size:              self.block_size,
            data_block:              self.data_block,
            index_block:             self.index_block,
            filter_block,
            short_scratch:           self.short_scratch,
            compression_scratch_buf: self.compression_scratch_buf,
        }
    }

    #[must_use]
    pub const fn num_entries(&self) -> usize {
        self.num_entries
    }

    #[must_use]
    pub fn size_estimate(&self) -> usize {
        usize::try_from(self.offset_in_file).unwrap_or(usize::MAX)
            + self.data_block.size_estimate()
            + self.index_block.size_estimate()
            + self.filter_block.as_ref().map(FilterBlockBuilder::size_estimate).unwrap_or(0)
            + TableFooter::ENCODED_LENGTH
    }

    /// Add a new entry to the table.
    ///
    /// With respect to the `TableCmp` comparator that was provided to this builder, the `key` must
    /// compare strictly greater than any previously-added key. If this requirement is not met,
    /// a panic may occur, or an invalid [`Table`] may be produced by this builder.
    ///
    /// [`Table`]: crate::table::Table
    //
    // This function uses `self.short_scratch` and `self.compression_scratch_buf`.
    pub fn add_entry(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        if self.data_block.size_estimate() > self.block_size {
            // `key` will be the first key in the next block, so it's less than or equal to any
            // key in the next block. And the caller asserts it's strictly greater than anything
            // already inserted.
            self.write_data_block(key)?;
        }

        if let Some(filter_block) = &mut self.filter_block {
            // We've called `filter_block.start_block(_)` as appropriate, and the caller
            // asserts that this key is strictly greater than previously inserted keys.
            filter_block.add_key(key);
        }

        // The caller asserts that this key is strictly greater than previously inserted keys.
        self.data_block.add_entry(key, value);
        self.num_entries += 1;

        Ok(())
    }

    /// Finish writing the entire table to the table file.
    ///
    /// On success, the total number of bytes written to the table file is returned.
    ///
    /// After this method is called, no other [`TableBuilder`] methods should be called other than
    /// `self.reuse_as_new(..)`. See the type-level documentation for more.
    //
    // This function uses `self.short_scratch` and `self.compression_scratch_buf`.
    pub fn finish(&mut self) -> Result<u64, ()> {
        // Write any pending data block
        if self.data_block.num_entries() > 0 {
            // Since `self.write_data_block` uses both scratch buffers, create a temporary one.
            let mut successor = Vec::new();

            self.comparator.find_short_successor(
                self.data_block.last_key(),
                &mut successor,
            );
            // There's no next block, so any successor works.
            self.write_data_block(&successor)?;
        }

        // Create metaindex block. We can reuse the data block builder, since this table builder
        // will not be writing any more data blocks until the table is completed.
        // Note that `self.data_block` has already been reset; either it had zero entries,
        // and was thus already in a blank-slate state, or `self.write_data_block(..)`
        // would have called `self.data_block.reset()`.
        //
        // Also, IMPORTANT NOTE:
        // If any more than one entry were to be added to the metaindex block, this approach
        // would not work. Or, it technically would, but I'd want to refactor something.
        // Why? Because the metaindex block, unlike every other block in an SSTable, is *always*
        // sorted by the default bytewise comparator. Technically, the `BlockBuilder` doesn't
        // actually care about its `Cmp` parameter, but still, it'd go against the stated letter
        // of the law to say that `ComparatorAdapter<TableCmp>` can be used as the parameter of
        // a `BlockBuilder` which is actually sorted by `ComparatorAdapter<DefaultComparator>`.
        // Adding one key is fine, since it meets the invariants for _any_ comparator.
        if let Some(filter_block) = &mut self.filter_block {
            self.short_scratch.extend(FILTER_META_PREFIX);
            self.short_scratch.extend(filter_block.policy().name());

            // From this point on, no other methods should be called on `filter_block`,
            // until `self.reuse_as_new(..)` calls `filter_block.reuse_as_new(..)`.
            let filter_block_data = filter_block.finish();

            let filter_handle = builder_write_block!(self, filter_block_data, NO_COMPRESSION)?;

            let mut encoded_handle = [0_u8; BlockHandle::MAX_ENCODED_LENGTH];
            let encoded_len = filter_handle.encode_to(&mut encoded_handle);

            // Reminder: `self.data_block` is currently actually the metaindex block.
            // Also, this is the first entry added, so it's guaranteed to vacuously be strictly
            // greater than any previously-inserted entry.
            #[expect(clippy::indexing_slicing, reason = "`encoded_len <= MAX_ENCODED_LENGTH`")]
            self.data_block.add_entry(
                &self.short_scratch,
                &encoded_handle[..encoded_len],
            );
            self.short_scratch.clear();
        }

        // Write the metaindex and index blocks

        let metaindex_block = self.data_block.finish_block_contents();
        let metaindex = builder_write_block!(self, metaindex_block, self.selected_compressor)?;

        let index_block = self.index_block.finish_block_contents();
        let index = builder_write_block!(self, index_block, self.selected_compressor)?;

        // Write the footer
        self.short_scratch.resize(TableFooter::ENCODED_LENGTH, 0);
        // The only way for it to fail is if the buffer isn't long enough, but we resized it
        // so that it's long enough.
        let _success = (TableFooter { metaindex, index }).encode_to(&mut self.short_scratch);

        self.table_file.write_all(&self.short_scratch).map_err(|_| ())?;
        self.short_scratch.clear();

        {
            #![expect(clippy::as_conversions, reason = "the constant is far less than `u64::MAX`")]
            self.offset_in_file += TableFooter::ENCODED_LENGTH as u64;
        };
        self.table_file.flush().map_err(|_| ())?;

        Ok(self.offset_in_file)
    }

    /// `next_key` should be strictly greater than any key in the current block, and less than
    /// or equal to any key in the next block (if there is one).
    ///
    /// This function uses `self.short_scratch` and `self.compression_scratch_buf`.
    fn write_data_block(&mut self, next_key: &[u8]) -> Result<(), ()> {
        self.comparator.find_short_separator(
            self.data_block.last_key(),
            next_key,
            &mut self.short_scratch,
        );

        let block_contents = self.data_block.finish_block_contents();

        let block_handle = builder_write_block!(self, block_contents, self.selected_compressor)?;
        self.data_block.reset();

        let mut encoded_handle = [0_u8; BlockHandle::MAX_ENCODED_LENGTH];
        let encoded_len = block_handle.encode_to(&mut encoded_handle);

        // First, note that `self.data_block` is necessarily nonempty if we get here.
        // Therefore, `self.data_block.last_key()` refers to the last key which was inserted
        // in this table, which is strictly greater than anything previously inserted.
        // All previous keys in the `index_block` will be strictly less than the first key of
        // what was in `self.data_block`. Therefore, the `self.short_scratch` separator is strictly
        // greater than all previous keys in `index_block`.
        #[expect(clippy::indexing_slicing, reason = "`encoded_len <= MAX_ENCODED_LENGTH`")]
        self.index_block.add_entry(&self.short_scratch, &encoded_handle[..encoded_len]);
        self.short_scratch.clear();

        if let Some(filter_block) = &mut self.filter_block {
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "offset in file cannot exceed `usize::MAX`",
            )]
            filter_block.start_block(self.offset_in_file as usize);
        }

        Ok(())
    }

    fn write_block(
        // mfw no view types; use `builder_write_block!(..)`
        compressor_list:        &CompList,
        table_file:             &mut File,
        offset_in_file:         &mut u64,
        scratch_buffer:         &mut Vec<u8>,
        // Actual arguments
        block_contents:         &[u8],
        compressor_id:          u8,
    ) -> Result<BlockHandle, ()> {
        // Scope for destructor of `compressor_list.get_ref()`
        {
            let compressor_list: &CompressorList = &compressor_list.get_ref();
            let Some(compressor) = compressor_list.get(compressor_id) else {
                return Err(());
            };

            compressor.encode_into(block_contents, scratch_buffer).map_err(|_| ())?;
        };

        let mut digest = crc32c::crc32c(scratch_buffer);
        digest = crc32c::crc32c_append(digest, &[compressor_id]);

        // Write the block: the compressed contents, followed by the table block trailer.
        table_file.write_all(scratch_buffer).map_err(|_| ())?;
        table_file.write_all(&[compressor_id]).map_err(|_| ())?;
        table_file.write_all(&digest.to_le_bytes()).map_err(|_| ())?;

        let block_size = u64::try_from(scratch_buffer.len()).map_err(|_| ())?;
        // We're done with the scratch buffer data.
        scratch_buffer.clear();

        let block_handle = BlockHandle {
            offset:     *offset_in_file,
            block_size,
        };

        {
            #![expect(clippy::as_conversions, reason = "constant is far less than `u64::MAX`")]
            *offset_in_file += block_size + BLOCK_TRAILER_LEN as u64;
        };

        Ok(block_handle)
    }
}

use std::borrow::BorrowMut as _;

use generic_container::FragileContainer;
use seekable_iterator::{CursorLendingIterator as _, Seekable as _};

use anchored_vfs::traits::RandomAccess;

use crate::block::TableBlock;
use crate::comparator::MetaindexComparator;
use crate::compressors::CompressorList;
use crate::filter::FilterPolicy;
use crate::filter_block::FilterBlockReader;
use crate::pool::BufferPool;
use super::format::{BlockHandle, BLOCK_TRAILER_LEN, FILTER_META_PREFIX, unmask_checksum};


/// A short-lived reader for any table block. Contains the data usually consistent across calls
/// by the same [`Table`].
///
/// The contents of the given `scratch_buffer` must be empty.
#[derive(Debug)]
pub struct TableBlockReader<'a, File, CompList, Pool> {
    pub file:             &'a File,
    pub compressor_list:  &'a CompList,
    pub verify_checksums: bool,
    pub buffer_pool:      &'a Pool,
    pub scratch_buffer:   &'a mut Vec<u8>,
}

#[expect(
    clippy::result_unit_err, clippy::map_err_ignore,
    reason = "temporary. TODO: return actual errors.",
)]
impl<File, CompList, Pool> TableBlockReader<'_, File, CompList, Pool>
where
    File:     RandomAccess,
    CompList: FragileContainer<CompressorList>,
    Pool:     BufferPool,
{
    /// Attempts to read the block associated with the `block_handle` from `self.file`,
    /// writing into the given `block_buffer`.
    ///
    /// Returns `Ok(())` if the entire block was successfully read.
    ///
    /// The given `block_buffer` _must_ be empty. The contents of `self.scratch_buffer` are cleared
    /// and remain valid for future calls if this method successfully returns.
    ///
    /// There must be a valid table block associated with the given `block_handle` in `self.file`.
    ///
    /// # Errors
    /// If an error is returned, then the contents of `self.scratch_buffer` and `block_buffer`
    /// should be assumed to be unknown. In particular, `self.scratch_buffer` would then need
    /// to be manually cleared before other methods of `self` may be called.
    // Could only panic if `File` implements `RandomAccess::read_exact_at` incorrectly.
    #[expect(clippy::missing_panics_doc, reason = "false positive (sort of)")]
    pub fn read_table_block(
        &mut self,
        block_handle: BlockHandle,
        block_buffer: &mut Vec<u8>,
    ) -> Result<(), ()> {
        #[expect(clippy::map_err_ignore, reason = "only one way that u64 -> usize can fail")]
        let block_size = usize::try_from(block_handle.block_size).map_err(|_| ())?;

        self.scratch_buffer.resize(block_size + BLOCK_TRAILER_LEN, 0);

        self.file.read_exact_at(block_handle.offset, self.scratch_buffer).map_err(|_| ())?;

        // Note that `self.scratch_buffer.len() >= BLOCK_TRAILER_LEN`, since we resized it
        // to at least that length.
        let (compressed_block, trailer) = self.scratch_buffer
            .split_at(self.scratch_buffer.len() - BLOCK_TRAILER_LEN);

        #[expect(clippy::unwrap_used, reason = "we split at `len - BLOCK_TRAILER_LEN`")]
        let trailer: &[u8; BLOCK_TRAILER_LEN] = trailer.try_into().unwrap();

        let compressor_id = trailer[0];
        let masked_checksum = &trailer[1..];
        #[expect(clippy::unwrap_used, reason = "`BLOCK_TRAILER_LEN == U32_BYTES + 1`")]
        let masked_checksum = u32::from_le_bytes(masked_checksum.try_into().unwrap());

        if self.verify_checksums {
            let unmasked_checksum = unmask_checksum(masked_checksum);
            // Compute the checksum of everything except the checksum that was appended.
            // That is, the checksum of the block and the appended `compressor_id`, but not
            // the `masked_checksum`.
            let checksum_of_block = crc32c::crc32c(compressed_block);
            let actual_checksum = crc32c::crc32c_append(checksum_of_block, &[compressor_id]);

            if unmasked_checksum != actual_checksum {
                return Err(());
            }
        }

        let compressor_list: &CompressorList = &self.compressor_list.get_ref();
        if let Some(compressor) = compressor_list.get(compressor_id) {
            compressor.decode_into(compressed_block, block_buffer).map_err(|_| ())?;

            self.scratch_buffer.clear();
            Ok(())
        } else {
            Err(())
        }
    }

    /// Attempts to read the filter block in `self.file` associated with the given `policy`.
    ///
    /// Returns `Ok(None)` if there is no such filter block - which is valid - and `Ok(Some(_))`
    /// if the filter block was found and successfully read.
    ///
    /// The `self.scratch_buffer` is cleared and remains valid for future calls if this method
    /// successfully returns.
    ///
    /// `self.file` must be a valid table file that contains the given `metaindex_block`.
    ///
    /// # Errors
    /// If an error is returned, then the contents of `self.scratch_buffer`
    /// should be assumed to be unknown. In particular, `self.scratch_buffer` would then need
    /// to be manually cleared before other methods of `self` may be called.
    #[expect(
        clippy::result_unit_err,
        reason = "temporary. TODO: return actual errors.",
    )]
    pub fn read_filter_block<Policy: FilterPolicy>(
        &mut self,
        policy:          Policy,
        metaindex_block: &TableBlock<Pool::PooledBuffer, MetaindexComparator>,
    ) -> Result<Option<FilterBlockReader<Policy, Pool::PooledBuffer>>, ()> {
        self.scratch_buffer.extend(FILTER_META_PREFIX);
        self.scratch_buffer.extend(policy.name());

        let mut metaindex_iter = metaindex_block.iter();
        metaindex_iter.seek(self.scratch_buffer);

        if let Some((meta_key, maybe_filter_handle)) = metaindex_iter.current() {
            if meta_key == self.scratch_buffer {
                self.scratch_buffer.clear();

                // The filter exists, and we found it.
                let (filter_block_handle, _) = BlockHandle::decode_from(maybe_filter_handle)?;

                if filter_block_handle.block_size > 0 {
                    let mut filter_block_buffer = self.buffer_pool.get_buffer();

                    self.read_table_block(filter_block_handle, filter_block_buffer.borrow_mut())?;

                    return Ok(Some(FilterBlockReader::new(policy, filter_block_buffer)));
                }
            }
        }
        self.scratch_buffer.clear();

        // There was no filter block associated with the given filter.
        Ok(None)
    }
}

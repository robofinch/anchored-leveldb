use crate::all_errors::types::BlockHandleCorruption;
use crate::utils::{encode_varint64, ReadVarint as _};

use super::simple_newtypes::{FileOffset, FileSize};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: FileOffset,
    pub size:   FileSize,
}

impl BlockHandle {
    pub(crate) fn decode(mut input: &[u8]) -> Result<(Self, usize), BlockHandleCorruption> {
        let (offset, offset_len) = input
            .read_varint64()
            .map_err(BlockHandleCorruption::offset)?;
        let (size, size_len) = input
            .read_varint64()
            .map_err(BlockHandleCorruption::size)?;

        let this = Self {
            offset: FileOffset(offset),
            size:   FileSize(size),
        };
        let this_len = offset_len + size_len;

        Ok((this, this_len))
    }

    #[expect(clippy::expect_used, reason = "easy to verify that the lengths are correct")]
    #[inline]
    #[must_use]
    pub(crate) fn encode(self, output: &mut [u8; 20]) -> usize {
        let offset_len = encode_varint64(
            output.first_chunk_mut::<10>().expect("`10 <= 20`"),
            self.offset.0,
        );

        // `offset_len` is the length of the above first chunk which was written to, and is
        // therefore at most 10.
        #[expect(clippy::indexing_slicing, reason = "`offset_len <= 10 < output.len()`")]
        let size_len = encode_varint64(
            output[offset_len..].first_chunk_mut::<10>().expect("`offset_len + 10 <= 20`"),
            self.size.0,
        );

        offset_len + size_len
    }
}

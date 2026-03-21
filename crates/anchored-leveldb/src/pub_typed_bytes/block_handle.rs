use crate::all_errors::types::BlockHandleCorruption;
use crate::utils::{encode_varint64, ReadVarint as _};
use super::{min_u32_usize::MinU32Usize, short_slice::ShortSlice};
use super::simple_newtypes::{FileOffset, TableBlockSize};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: FileOffset,
    pub size:   TableBlockSize,
}

impl BlockHandle {
    /// The maximum length of two varint64 values.
    pub(crate) const MAX_ENCODED_LENGTH: usize = 20;

    /// The maximum length of two varint64 values, as a `MinU32Usize`.
    pub(crate) const MAX_ENCODED_LENGTH_MIN_U32_USIZE: MinU32Usize = const {
        MinU32Usize::from_usize(Self::MAX_ENCODED_LENGTH).unwrap()
    };

    pub(crate) fn decode(mut input: &[u8]) -> Result<(Self, usize), BlockHandleCorruption> {
        let (offset, offset_len) = input
            .read_varint64()
            .map_err(BlockHandleCorruption::offset)?;
        let (size, size_len) = input
            .read_varint64()
            .map_err(BlockHandleCorruption::size)?;

        let this = Self {
            offset: FileOffset(offset),
            size:   TableBlockSize(size),
        };
        let this_len = offset_len + size_len;

        Ok((this, this_len))
    }

    #[expect(clippy::expect_used, reason = "easy to verify that the lengths are correct")]
    #[must_use]
    pub(crate) fn encode(self, output: &mut [u8; Self::MAX_ENCODED_LENGTH]) -> usize {
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

    #[must_use]
    pub(crate) fn encode_short(
        self,
        output: &mut [u8; Self::MAX_ENCODED_LENGTH],
    ) -> ShortSlice<'_> {
        let encoded_len = self.encode(output);
        #[expect(
            clippy::indexing_slicing,
            clippy::expect_used,
            reason = "`encoded_len <= MAX_ENCODED_LENGTH < u32::MAX`; cannot panic",
        )]
        ShortSlice::new(&output[..encoded_len])
            .expect("`BlockHandle::MAX_ENCODED_LENGTH < u32::MAX`")
    }
}

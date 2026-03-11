use crate::all_errors::types::Varint64DecodeError;
use crate::utils::{encode_varint64, ReadVarint as _};

use super::simple_newtypes::{FileOffset, FileSize};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: FileOffset,
    pub size:   FileSize,
}

impl BlockHandle {
    pub(crate) fn decode(mut input: &[u8]) -> Result<Self, Varint64DecodeError> {
        let (offset, _) = input.read_varint64()?;
        let (size, _) = input.read_varint64()?;

        Ok(Self {
            offset: FileOffset(offset),
            size:   FileSize(size),
        })
    }

    #[inline]
    #[must_use]
    pub(crate) fn encode(self, output: &mut [u8; 10]) -> usize {
        let offset_len = encode_varint64(
            output.first_chunk_mut().expect("5 <= 10"),
            self.offset.0,
        );

        let size_len = encode_varint64(
            output[offset_len..].first_chunk_mut().expect("offset_len + 5 <= 5 + 5 == 10"),
            self.size.0,
        );

        offset_len + size_len
    }
}

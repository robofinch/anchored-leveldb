use crate::{all_errors::types::TableFooterCorruption, pub_typed_bytes::BlockHandle};


/// The length of the footer at the end of every (compressed) block of an SSTable.
pub(super) const BLOCK_FOOTER_LEN: usize = const {
    let footer_len: usize = 5;

    assert!(
        footer_len == size_of::<u8>() + size_of::<u32>(),
        "Show why it's the magic value 5",
    );

    footer_len
};

/// The hardcoded `filter.` prefix used before a filter's name (in metaindex block entries
/// corresponding to filters).
pub(super) const FILTER_META_PREFIX: &[u8] = b"filter.";


#[derive(Debug, Clone, Copy)]
pub(super) struct TableFooter {
    pub metaindex: BlockHandle,
    pub index:     BlockHandle,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl TableFooter {
    /// The exact length of the table footer (when encoded).
    pub const ENCODED_LENGTH:    usize   = 48;
    /// The exact length of the table footer (when encoded), as a `u8`.
    pub const ENCODED_LENGTH_U8: u8      = 48;
    pub const MAGIC:             u64     = 0x_db47_7524_8b80_fb57;
    pub const ENCODED_MAGIC:     [u8; 8] = Self::MAGIC.to_le_bytes();

    pub fn decode_from(input: &[u8; Self::ENCODED_LENGTH]) -> Result<Self, TableFooterCorruption> {
        #[expect(clippy::unwrap_used, reason = "8 <= 48")]
        let magic: &[u8; 8] = input.last_chunk().unwrap();
        if magic != &Self::ENCODED_MAGIC {
            return Err(TableFooterCorruption::BadTableMagic(*magic));
        }

        let (metaindex, metaindex_size) = BlockHandle::decode(input)
            .map_err(TableFooterCorruption::Metaindex)?;

        // Encoded block handles consist of two varint64's, each of which can be up to 10 bytes
        // long.
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "overflow/wrap impossible; `metaindex_size <= 20 < u8::MAX`",
        )]
        let metaindex_size_u8 = metaindex_size as u8;

        #[expect(clippy::indexing_slicing, reason = "`metaindex_size + 20 <= 40 < input.len()`")]
        let (index, _) = BlockHandle::decode(&input[metaindex_size..])
            .map_err(|err| TableFooterCorruption::Index(metaindex_size_u8, err))?;

        Ok(Self {
            metaindex,
            index,
        })
    }

    pub fn encode_to(&self, output: &mut [u8; Self::ENCODED_LENGTH]) {
        #[expect(clippy::unwrap_used, reason = "`20 < Self::ENCODED_LENGTH == 48`")]
        let metaindex_len = self.metaindex.encode(output.first_chunk_mut::<20>().unwrap());

        #[expect(clippy::indexing_slicing, reason = "`metaindex_len <= 20 < 48`")]
        let remaining = &mut output[metaindex_len..];
        #[expect(
            clippy::unwrap_used,
            reason = "`metaindex_len + 20 <= 40 < 48`",
        )]
        let _ignore_index_len = self.index.encode(remaining.first_chunk_mut::<20>().unwrap());

        #[expect(clippy::unwrap_used, reason = "`8 < 48`")]
        {
            *output.last_chunk_mut::<8>().unwrap() = Self::ENCODED_MAGIC;
        };
    }
}


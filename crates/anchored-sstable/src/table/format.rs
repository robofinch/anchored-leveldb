use integer_encoding::VarInt as _;


/// One byte to indicate the compression type, and 4 bytes for a checksum.
pub const BLOCK_TRAILER_LEN:  usize = 5;
/// The hardcoded `filter.` prefix used before a filter's name, in meta index block entries
/// corresponding to filters.
pub const FILTER_META_PREFIX: &[u8] = b"filter.";


/// The offset and size of a block within a table (SSTable). Can be converted to and from
/// varints.
#[derive(Debug, Clone, Copy)]
pub struct BlockHandle {
    pub offset:     u64,
    pub block_size: u64,
}

impl BlockHandle {
    /// Each varint64 could take up to 10 bytes.
    pub const MAX_ENCODED_LENGTH: usize = 20;

    /// Attempts to decode a block handle and return how many bytes were read.
    ///
    /// Returns `None` if `input` was too short or did not have two valid varint encodings.
    #[expect(
        clippy::result_unit_err,
        reason = "temporary. TODO: return actual errors.",
    )]
    pub fn decode_from(input: &[u8]) -> Result<(Self, usize), ()> {
        let (offset, offset_size) = u64::decode_var(input).ok_or(())?;
        #[expect(
            clippy::indexing_slicing,
            reason = "byte len that `decode_var` read must be less than `input.len()`",
        )]
        let (size, size_size) = u64::decode_var(&input[offset_size..]).ok_or(())?;

        Ok((
            Self {
                offset,
                block_size: size,
            },
            offset_size + size_size,
        ))
    }

    /// Returns the number of bytes written.
    ///
    /// If `output` is too short to write both varints, nothing is written.
    #[must_use]
    pub fn encode_to(self, output: &mut [u8]) -> usize {
        if output.len() < self.offset.required_space() + self.block_size.required_space() {
            0
        } else {
            let mut read_len = self.offset.encode_var(output);
            {
                #![expect(
                    clippy::indexing_slicing,
                    reason = "byte len that `encode_var` wrote must be less than `output.len()`",
                )]
                read_len += self.block_size.encode_var(&mut output[read_len..]);
            };
            read_len
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TableFooter {
    pub metaindex: BlockHandle,
    pub index:     BlockHandle,
}

#[expect(clippy::result_unit_err, reason = "temporary. TODO: return actual errors.")]
impl TableFooter {
    /// The exact length of the table footer (when encoded).
    pub const ENCODED_LENGTH: usize   = 2 * BlockHandle::MAX_ENCODED_LENGTH + 8;
    pub const MAGIC:          u64     = 0x_db47_7524_8b80_fb57;
    pub const ENCODED_MAGIC:  [u8; 8] = Self::MAGIC.to_le_bytes();

    pub fn decode_from(input: &[u8]) -> Result<Self, ()> {
        if input.len() < Self::ENCODED_LENGTH {
            return Err(());
        }

        // We checked the input length.
        Self::read_magic(input)?;

        let (metaindex, metaindex_size) = BlockHandle::decode_from(input)?;
        #[expect(clippy::indexing_slicing, reason = "we check that `input` is long enough")]
        let (index, _) = BlockHandle::decode_from(&input[metaindex_size..])?;

        Ok(Self {
            metaindex,
            index,
        })
    }

    /// Returns whether encoding was successful or not.
    #[must_use]
    pub fn encode_to(&self, output: &mut [u8]) -> bool {
        if output.len() < Self::ENCODED_LENGTH {
            return false;
        }

        let read_len = self.metaindex.encode_to(output);
        debug_assert!(read_len > 0, "encoding should only fail if output is too short");

        #[expect(clippy::indexing_slicing, reason = "we check if output is long enough")]
        let read_len = self.index.encode_to(&mut output[read_len..]);
        debug_assert!(read_len > 0,  "encoding should only fail if output is too short");

        // This won't panic, we checked `output.len()`.
        Self::write_magic(output);

        true
    }

    /// # Panics
    /// Panics if `input` is not at least [`Self::ENCODED_LENGTH`] bytes in length.
    fn read_magic(input: &[u8]) -> Result<(), ()> {
        #[expect(clippy::indexing_slicing, reason = "we declare the possible panic")]
        let magic: &[u8] = &input[
            Self::ENCODED_LENGTH - Self::ENCODED_MAGIC.len()
            ..Self::ENCODED_LENGTH
        ];

        if Self::ENCODED_MAGIC == magic {
            Ok(())
        } else {
            Err(())
        }
    }

    /// # Panics
    /// Panics if `output` is not at least [`Self::ENCODED_LENGTH`] bytes in length.
    fn write_magic(output: &mut [u8]) {
        #[expect(clippy::indexing_slicing, reason = "we declare the possible panic")]
        let magic: &mut [u8] = &mut output[
            Self::ENCODED_LENGTH - Self::ENCODED_MAGIC.len()
            ..Self::ENCODED_LENGTH
        ];

        magic.copy_from_slice(&Self::ENCODED_MAGIC);
    }
}

const CHECKSUM_MASK_DELTA: u32 = 0x_a282_ead8;

#[inline]
#[must_use]
pub const fn mask_checksum(unmasked: u32) -> u32 {
    unmasked.rotate_right(15).wrapping_add(CHECKSUM_MASK_DELTA)
}

#[inline]
#[must_use]
pub const fn unmask_checksum(masked: u32) -> u32 {
    masked.wrapping_sub(CHECKSUM_MASK_DELTA).rotate_left(15)
}

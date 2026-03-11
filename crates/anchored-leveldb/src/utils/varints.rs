use crate::all_errors::types::{Varint32DecodeError, Varint64DecodeError};


pub(crate) fn decode_varint32(input: &[u8]) -> Result<(u32, usize), Varint32DecodeError> {
    let mut varint: u32 = 0;
    let mut bytes_read: usize = 0;
    let mut shift: u8 = 0;
    let mut finished = false;

    for _ in 0..4_u8 {
        let byte = *input.get(bytes_read).ok_or(Varint32DecodeError::Truncated)?;
        let data = byte & 0b0111_1111;
        finished = byte & 0b1000_0000 == 0;

        varint |= u32::from(data) << shift;
        bytes_read += 1;
        shift += 7;

        if finished {
            break;
        }
    }

    if !finished {
        // We've processed the first 4*7 = 28 bits, so 4 bits should remain.
        // Bits 5, 6, 7, and 8 should be zero, else we have overflow.
        let byte = *input.get(bytes_read).ok_or(Varint32DecodeError::Truncated)?;
        if byte & 0b1111_0000 != 0 {
            return Err(Varint32DecodeError::Overflowing);
        }
        let data = byte; // & 0b0000_1111; This is a no-op, since the upper bits are zero.

        varint |= u32::from(data) << shift;
        bytes_read += 1;
    }

    Ok((varint, bytes_read))
}

pub(crate) fn decode_varint64(input: &[u8]) -> Result<(u64, usize), Varint64DecodeError> {
    let mut varint: u64 = 0;
    let mut bytes_read: usize = 0;
    let mut shift: u8 = 0;
    let mut finished = false;

    for _ in 0..9_u8 {
        let byte = *input.get(bytes_read).ok_or(Varint64DecodeError::Truncated)?;
        let data = byte & 0b0111_1111;
        finished = byte & 0b1000_0000 == 0;

        varint |= u64::from(data) << shift;
        bytes_read += 1;
        shift += 7;

        if finished {
            break;
        }
    }

    if !finished {
        // We've processed the first 9*7 = 63 bits, so 1 bit should remain.
        // Bits 2-8 should be zero, else we have overflow.
        let byte = *input.get(bytes_read).ok_or(Varint64DecodeError::Truncated)?;
        if byte & 0b1111_1110 != 0 {
            return Err(Varint64DecodeError::Overflowing);
        }
        let data = byte; // & 0b0000_0001; This is a no-op, since the upper bits are zero.

        varint |= u64::from(data) << shift;
        bytes_read += 1;
    }

    Ok((varint, bytes_read))
}

pub(crate) trait ReadVarint {
    fn read_varint32(&mut self) -> Result<(u32, usize), Varint32DecodeError>;

    fn read_varint64(&mut self) -> Result<(u64, usize), Varint64DecodeError>;
}

impl ReadVarint for &[u8] {
    fn read_varint32(&mut self) -> Result<(u32, usize), Varint32DecodeError> {
        let (varint, varint_len) = decode_varint32(self)?;
        #[expect(clippy::indexing_slicing, reason = "the varint's len is at most the input's len")]
        {
            *self = &self[varint_len..];
        };
        Ok((varint, varint_len))
    }

    fn read_varint64(&mut self) -> Result<(u64, usize), Varint64DecodeError> {
        let (varint, varint_len) = decode_varint64(self)?;
        #[expect(clippy::indexing_slicing, reason = "the varint's len is at most the input's len")]
        {
            *self = &self[varint_len..];
        };
        Ok((varint, varint_len))
    }
}

#[inline]
#[must_use]
pub(crate) const fn encode_varint32(output: &mut [u8; 5], mut num: u32) -> usize {
    let mut bytes_written = 0;

    // Once `num` has been shifted right four times, it has been shifted right `28` bits,
    // meaning that its maximum possible value is `0b1111`. Therefore, in this loop,
    // `bytes_written < 4`, so the access is in-bounds.
    #[expect(clippy::indexing_slicing, reason = "(size_of::<u32> * 8).div_ceil(7) == output.len()")]
    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        reason = "truncation is intentional",
    )]
    while num >= 0b1000_0000 {
        output[bytes_written] = (num as u8) | 0b1000_0000;
        num >>= 7_u8;
        bytes_written += 1;
    }
    // `num` can be shifted right at most four times in the above loop. In that case,
    // `bytes_written == 4 == output.len() - 1`, so the access is in-bounds.
    #[expect(clippy::indexing_slicing, reason = "(size_of::<u32> * 8).div_ceil(7) == output.len()")]
    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        reason = "truncation is intentional (and, in this case, the leading bytes are zero anyway)",
    )]
    {
        output[bytes_written] = num as u8;
    };
    bytes_written += 1;

    bytes_written
}

#[inline]
#[must_use]
pub(crate) const fn encode_varint64(output: &mut [u8; 10], mut num: u64) -> usize {
    let mut bytes_written = 0;

    // Once `num` has been shifted right nine times, it has been shifted right `63` bits,
    // meaning that its maximum possible value is `0b0001`. Therefore, in this loop,
    // `bytes_written < 9`, so the access is in-bounds.
    #[expect(clippy::indexing_slicing, reason = "(size_of::<u64> * 8).div_ceil(7) == output.len()")]
    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        reason = "truncation is intentional",
    )]
    while num >= 0b1000_0000 {
        output[bytes_written] = (num as u8) | 0b1000_0000;
        num >>= 7_u8;
        bytes_written += 1;
    }
    // `num` can be shifted right at most four times in the above loop. In that case,
    // `bytes_written == 9 == output.len() - 1`, so the access is in-bounds.
    #[expect(clippy::indexing_slicing, reason = "(size_of::<u64> * 8).div_ceil(7) == output.len()")]
    #[expect(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        reason = "truncation is intentional (and, in this case, the leading bytes are zero anyway)",
    )]
    {
        output[bytes_written] = num as u8;
    };
    bytes_written += 1;

    bytes_written
}

pub(crate) trait WriteVarint {
    fn write_varint32(&mut self, num: u32);

    fn write_varint64(&mut self, num: u64);
}

impl WriteVarint for Vec<u8> {
    #[inline]
    fn write_varint32(&mut self, mut num: u32) {
        while num >= 0b1000_0000 {
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "truncation is intentional",
            )]
            self.push((num as u8) | 0b1000_0000);
            num >>= 7_u8;
        }
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "truncation is intentional",
        )]
        self.push(num as u8);
    }

    #[inline]
    fn write_varint64(&mut self, mut num: u64) {
        while num >= 0b1000_0000 {
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "truncation is intentional",
            )]
            self.push((num as u8) | 0b1000_0000);
            num >>= 7_u8;
        }
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "truncation is intentional",
        )]
        self.push(num as u8);
    }
}

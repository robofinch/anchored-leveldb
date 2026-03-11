use std::fmt::{Debug, Formatter, Result as FmtResult};

use zlib_rs::{Deflate, DeflateFlush, Inflate, InflateFlush, Status};


use crate::utils::get_buffer;
use crate::pub_traits::pool::{BufferPool, ByteBuffer as _};
use super::codec_trait::{CodecCompressionError, CodecDecompressionError, CompressionCodec};


pub use zlib_rs::{DeflateError as ZlibDeflateError, InflateError as ZlibInflateError};


/// The default number of window bits, used by `flate2` and recommended by `zlib-rs` as the
/// most common choice.
const DEFAULT_WINDOW_BITS: u8 = 15;


/// Zlib compression via [`zlib_rs`].
///
/// No dictionary is used, and the default 15 window bits are used.
#[derive(Debug, Clone, Copy)]
pub struct ZlibCodec {
    /// The level of compression to perform when encoding/compressing data. Has no effect on
    /// decoding/decompressing data.
    pub compression_level: u8,
    /// Whether the output data of encoding/compression should have a zlib header, and whether
    /// the input data of decoding/decompression should be expected to have a zlib header.
    pub zlib_header:       bool,
}

impl ZlibCodec {
    #[inline]
    #[must_use]
    pub const fn with_default_compression(zlib_header: bool) -> Self {
        Self {
            compression_level: 6,
            zlib_header,
        }
    }
}

impl CompressionCodec for ZlibCodec {
    type Encoder = ZlibEncoder;
    type Decoder = ZlibDecoder;
    type CompressionError   = ZlibDeflateError;
    type DecompressionError = ZlibInflateError;

    fn init_encoder(&self) -> Self::Encoder {
        let compression_level = i32::from(self.compression_level);

        ZlibEncoder(Deflate::new(compression_level, self.zlib_header, DEFAULT_WINDOW_BITS))
    }

    fn encode<Pool: BufferPool>(
        encoder:          &mut Self::Encoder,
        src:              &[u8],
        compression_goal: usize,
        pool:             &Pool,
        existing_buf:     &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecCompressionError<Self::CompressionError>> {
        // We will *not* need to reallocate the buffer. We do, however, need to allocate one
        // extra byte to properly distinguish EOF from "buffer too small".
        // We can use `saturating_add(1)`, since it's impossible to allocate a buffer of size
        // `usize::MAX`.
        let compression_goal_plus_one = compression_goal.saturating_add(1);
        // Note that only the buffer's capacity, not length, matters (up until we return it).
        let mut output_buf = get_buffer(pool, existing_buf, compression_goal_plus_one)?;

        let mut old_total_out = 0;
        encoder.0.reset();

        loop {
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "no overflow; a single allocation's length cannot exceed `usize::MAX`",
            )]
            // Since we use a single `src` slice for the entire input, `total_in` should not
            // exceed `usize::MAX`.
            let old_total_in = encoder.0.total_in() as usize;

            #[expect(
                clippy::indexing_slicing,
                reason = "`old_total_in` is at most the length of input, which is `src.len()`",
            )]
            let next_input = &src[old_total_in..];
            #[expect(
                clippy::indexing_slicing,
                reason = "`old_total_out` is at most the length of output, which is \
                          `compression_goal_plus_one`, which is `<= output_buf.capacity()`",
            )]
            let next_output = &mut output_buf
                .as_entire_capacity_slice_mut()[old_total_out..compression_goal_plus_one];

            let end_of_input = next_input.is_empty();
            let flush = if end_of_input {
                DeflateFlush::Finish
            } else {
                DeflateFlush::NoFlush
            };

            let status = match encoder.0.compress(next_input, next_output, flush) {
                Ok(status) => status,
                Err(err) => {
                    *existing_buf = Some(output_buf);
                    return Err(CodecCompressionError::Custom(err));
                }
            };

            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "no overflow; a single allocation's length cannot exceed `usize::MAX`",
            )]
            let new_total_out = encoder.0.total_out() as usize;

            let read_zero = old_total_out == new_total_out;
            old_total_out = new_total_out;

            match status {
                Status::Ok | Status::BufError => {
                    if next_output.is_empty() {
                        // `old_total_out` equals `compression_goal_plus_one`, implying that
                        // the compression goal has been exceeded, so the data is incompressible.
                        return Err(CodecCompressionError::Incompressible);
                    }

                    // Only if we have reached the end of input should we treat a zero read as the
                    // end of the stream.
                    if read_zero && end_of_input {
                        output_buf.set_len(new_total_out);
                        return Ok(output_buf);
                    }

                    // Continue.
                }
                Status::StreamEnd => {
                    output_buf.set_len(new_total_out);
                    return Ok(output_buf);
                }
            }
        }
    }

    fn init_decoder(&self) -> Self::Decoder {
        ZlibDecoder(Inflate::new(self.zlib_header, DEFAULT_WINDOW_BITS), self.zlib_header)
    }

    fn decode<Pool: BufferPool>(
        decoder:      &mut Self::Decoder,
        src:          &[u8],
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecDecompressionError<Self::DecompressionError>> {
        // `src.len()` is an estimated lower bound for the length.
        let mut output_buf = get_buffer(pool, existing_buf, src.len())?;
        // Have the length record what's actually been written.
        output_buf.clear();
        decoder.0.reset(decoder.1);

        loop {
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "no overflow; a single allocation's length cannot exceed `usize::MAX`",
            )]
            // Since we use a single `src` slice for the entire input, `total_in` should not
            // exceed `usize::MAX`.
            let old_total_in = decoder.0.total_in() as usize;

            #[expect(
                clippy::indexing_slicing,
                reason = "`old_total_in` is at most the length of input, which is `src.len()`",
            )]
            let next_input = &src[old_total_in..];
            let next_output = output_buf.as_remaining_capacity_slice_mut();

            let end_of_input = next_input.is_empty();
            let flush = if end_of_input {
                InflateFlush::Finish
            } else {
                InflateFlush::NoFlush
            };

            let status = match decoder.0.decompress(next_input, next_output, flush) {
                Ok(status) => status,
                Err(err) => {
                    *existing_buf = Some(output_buf);
                    return Err(CodecDecompressionError::Custom(err));
                }
            };

            let old_total_out = output_buf.len();

            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "`total_out` should not exceed the capacity of the output buffer",
            )]
            {
                // Should not panic, since we use a single output buffer for the entire output.
                output_buf.set_len(decoder.0.total_out() as usize);
            };

            let read_zero = old_total_out == output_buf.len();

            match status {
                Status::Ok | Status::BufError => {
                    if output_buf.is_full() {
                        // The buffer is full, need more space. Double the buffer's capacity.
                        if let Err(err) = pool.try_grow_amortized(&mut output_buf) {
                            *existing_buf = Some(output_buf);
                            return Err(CodecDecompressionError::from(err));
                        }
                    } else if read_zero && end_of_input {
                        // Only if we have reached the end of input should we treat a zero read as
                        // the end of the stream.
                        return Ok(output_buf);
                    } else {
                        // Continue.
                    }
                }
                Status::StreamEnd => return Ok(output_buf),
            }
        }
    }
}

pub struct ZlibEncoder(Deflate);

impl Debug for ZlibEncoder {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ZlibEncoder")
            .field("inner",     &"internal `zlib-rs` `DeflateStream` state")
            .field("total_in",  &self.0.total_in())
            .field("total_out", &self.0.total_out())
            .finish()
    }
}

pub struct ZlibDecoder(Inflate, bool);

impl ZlibDecoder {
    /// Select whether encoded/compressed data read by this decoder should be expected to have a
    /// zlib header.
    #[inline]
    pub const fn set_zlib_header(&mut self, require_zlib_header: bool) {
        self.1 = require_zlib_header;
    }
}

impl Debug for ZlibDecoder {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ZlibDecoder")
            .field("inner",       &"internal `zlib-rs` `InflateStream` state")
            .field("total_in",    &self.0.total_in())
            .field("total_out",   &self.0.total_out())
            .field("zlib_header", &self.1)
            .finish()
    }
}

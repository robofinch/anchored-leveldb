use std::convert::Infallible;

use crate::utils::get_buffer;
use crate::pub_traits::pool::{BufferPool, ByteBuffer as _};
use super::codec_trait::{CodecCompressionError, CodecDecompressionError, CompressionCodec};


/// A "compression codec" that leaves data uncompressed. That is, encoding and decoding data
/// is just a `memcpy`.
#[derive(Debug, Clone, Copy)]
pub struct NoCompressionCodec;

impl CompressionCodec for NoCompressionCodec {
    type Encoder = Self;
    type Decoder = Self;
    type CompressionError   = Infallible;
    type DecompressionError = Infallible;

    #[inline]
    fn init_encoder(&self) -> Self::Encoder {
        Self
    }

    fn encode<Pool: BufferPool>(
        _encoder:         &mut Self::Encoder,
        src:              &[u8],
        compression_goal: usize,
        pool:             &Pool,
        existing_buf:     &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecCompressionError<Infallible>> {
        if src.len() > compression_goal {
            return Err(CodecCompressionError::Incompressible);
        }

        let mut buf = get_buffer(pool, existing_buf, src.len())?;

        // `buf` should have length exactly `src.len()`, so this should not panic.
        buf.as_mut_slice().copy_from_slice(src);

        Ok(buf)
    }

    #[inline]
    fn init_decoder(&self) -> Self::Decoder {
        Self
    }

    fn decode<Pool: BufferPool>(
        _decoder:     &mut Self::Decoder,
        src:          &[u8],
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecDecompressionError<Infallible>> {
        let mut buf = get_buffer(pool, existing_buf, src.len())?;

        // `buf` should have length exactly `src.len()`, so this should not panic.
        buf.as_mut_slice().copy_from_slice(src);

        Ok(buf)
    }
}

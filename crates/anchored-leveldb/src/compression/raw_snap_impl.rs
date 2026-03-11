use snap::raw::{Decoder, Encoder, decompress_len, max_compress_len};

use crate::utils::get_buffer;
use crate::pub_traits::pool::{BufferPool, ByteBuffer as _};
use super::codec_trait::{CodecCompressionError, CodecDecompressionError, CompressionCodec};


pub use snap::Error as SnappyError;


/// Raw snappy compression via [`snap`].
#[derive(Default, Debug, Clone, Copy)]
pub struct SnappyCodec;

impl CompressionCodec for SnappyCodec {
    type Encoder = SnappyEncoder;
    type Decoder = SnappyDecoder;
    type CompressionError   = SnappyError;
    type DecompressionError = SnappyError;

    fn init_encoder(&self) -> Self::Encoder {
        SnappyEncoder(Encoder::new())
    }

    #[expect(clippy::panic_in_result_fn, reason = "the `assert!` could only fail due to a bug")]
    fn encode<Pool: BufferPool>(
        encoder:          &mut Self::Encoder,
        src:              &[u8],
        compression_goal: usize,
        pool:             &Pool,
        existing_buf:     &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecCompressionError<Self::CompressionError>> {
        // `snap::raw` (and, to be fair, upstream snappy does the same) requires that the given
        // buffer have the maximum possible length, which is approximately 7/6 of `src.len()`.
        // In other words, if `compression_goal` is `7/8` times `src.len()`, this wastes ~29% of
        // `src.len()` of buffer capacity. If `compression_goal` is exceeded, up to a quarter of the
        // compression work done could be avoided (though, either way, the first three quarters of
        // the compression work would probably be wasted).
        // In other words, it sure would be nice to optimize this, but that'd require that I write
        // my own snappy implementation, which is severely out-of-scope for this project.
        let dst_len = max_compress_len(src.len());

        let mut buf = get_buffer(pool, existing_buf, dst_len)?;

        match encoder.0.compress(src, buf.as_mut_slice()) {
            Ok(written_bytes) => {
                if written_bytes > compression_goal {
                    *existing_buf = Some(buf);
                    return Err(CodecCompressionError::Incompressible);
                }

                assert!(written_bytes <= buf.len(), "`snap::raw::Encoder` shouldn't be buggy");
                // Note: `written_bytes` should be less than `buf.len()`, so this should not
                // panic.
                buf.set_len(written_bytes);
                Ok(buf)
            }
            Err(err) => {
                *existing_buf = Some(buf);
                Err(CodecCompressionError::Custom(err))
            }
        }
    }

    fn init_decoder(&self) -> Self::Decoder {
        SnappyDecoder(Decoder::new())
    }

    #[expect(clippy::panic_in_result_fn, reason = "the `assert!` could only fail due to a bug")]
    fn decode<Pool: BufferPool>(
        decoder:      &mut Self::Decoder,
        src:          &[u8],
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecDecompressionError<Self::DecompressionError>> {
        let dst_len = decompress_len(src).map_err(CodecDecompressionError::Custom)?;

        let mut buf = get_buffer(pool, existing_buf, dst_len)?;

        match decoder.0.decompress(src, buf.as_mut_slice()) {
            Ok(written_bytes) => {
                // If the decompressed bytes are not length `decompress_len(src)`,
                // then `snap` should return an error.
                assert!(written_bytes == dst_len, "`snap::raw::decompress_len` shouldn't be buggy");
                Ok(buf)
            }
            Err(err) => {
                *existing_buf = Some(buf);
                Err(CodecDecompressionError::Custom(err))
            }
        }
    }
}

#[derive(Debug)]
pub struct SnappyEncoder(Encoder);

#[derive(Debug)]
pub struct SnappyDecoder(Decoder);

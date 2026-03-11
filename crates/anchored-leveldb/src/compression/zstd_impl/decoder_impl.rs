use zstd_safe::{ContentSizeError, DCtx, get_frame_content_size};

use crate::utils::get_buffer;
use crate::pub_traits::pool::{BufferPool, ByteBuffer as _};
use super::super::codec_trait::CodecDecompressionError;
use super::{ZstdDecompressionError, ZstdErrorCode};


pub(super) struct ZstdDecoderImpl(DCtx<'static>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl ZstdDecoderImpl {
    #[must_use]
    pub fn new() -> Self {
        Self(DCtx::create())
    }

    pub fn decode<Pool: BufferPool>(
        &mut self,
        src:          &[u8],
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecDecompressionError<ZstdDecompressionError>> {
        let decompressed_size = match get_frame_content_size(src) {
            Ok(Some(decompressed_size)) => decompressed_size,
            Ok(None) => {
                return Err(CodecDecompressionError::Custom(
                    ZstdDecompressionError::MissingContentSize,
                ));
            }
            Err(ContentSizeError {}) => {
                return Err(CodecDecompressionError::Custom(
                    ZstdDecompressionError::ContentSizeError,
                ));
            }
        };

        let Ok(decompressed_size) = usize::try_from(decompressed_size) else {
            // Basically no different than if `decompressed_size` were `usize::MAX`.
            return Err(CodecDecompressionError::BufferAlloc);
        };

        let mut buf = get_buffer(pool, existing_buf, decompressed_size)?;

        match self.0.decompress(buf.as_mut_slice(), src) {
            Ok(bytes_written) => {
                // Should not panic, since assuming Zstd is not buggy,
                // `bytes_written <= buf.as_mut_slice().len() <= buf.capacity()`.
                buf.set_len(bytes_written);

                Ok(buf)
            }
            Err(error_code) => {
                *existing_buf = Some(buf);
                Err(CodecDecompressionError::Custom(ZstdDecompressionError::ErrorCode(ZstdErrorCode(
                    error_code,
                ))))
            }
        }
    }
}

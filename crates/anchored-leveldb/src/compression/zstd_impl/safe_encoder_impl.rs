use zstd_safe::{CCtx, CParameter, get_error_name, max_c_level, min_c_level};

use crate::pub_traits::pool::ByteBuffer;
use super::super::codec_trait::CodecCompressionError;
use super::{DST_FULL, DST_TOO_SMALL, ZstdCompressionError, ZstdErrorCode};


pub(super) struct ZstdEncoderImpl(CCtx<'static>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl ZstdEncoderImpl {
    /// # Panics
    /// Panics if `ZSTD_createCCtx` returns null, or if setting the compression level (which is
    /// first clamped to be within the valid range) fails.
    ///
    /// (Presumably, the former could happen due to OOM, for instance. It's unlikely that a panic
    /// could be caused by the latter, since this function clamps the given `compression_level`.)
    #[must_use]
    pub fn new(compression_level: i32) -> Self {
        let compression_level = compression_level.clamp(min_c_level(), max_c_level());

        // Panics if allocation fails
        let mut cctx = CCtx::create();

        #[expect(
            clippy::panic,
            reason = "this panic shouldn't happen, since we clamp `compression_level`",
        )]
        if let Err(code) = cctx.set_parameter(CParameter::CompressionLevel(compression_level)) {
            panic!("`ZSTD_CCtx` creation failed: {}", get_error_name(code));
        }

        Self(cctx)
    }

    pub fn encode<Buffer: ByteBuffer>(
        &mut self,
        src: &[u8],
        dst: &mut Buffer,
    ) -> Result<(), CodecCompressionError<ZstdCompressionError>> {
        match self.0.compress2(dst.as_mut_slice(), src) {
            Ok(bytes_written) => {
                dst.set_len(bytes_written);
                Ok(())
            }
            Err(DST_TOO_SMALL | DST_FULL) => {
                Err(CodecCompressionError::Incompressible)
            }
            Err(other_code) => {
                Err(CodecCompressionError::Custom(ZstdCompressionError::ErrorCode(ZstdErrorCode(
                    other_code,
                ))))
            }
        }
    }
}

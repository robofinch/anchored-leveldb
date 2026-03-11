#![expect(unsafe_code, reason = "Use FFI not yet exposed in `zstd_safe`")]

use std::ptr::NonNull;
use std::ffi::{c_int, c_ulonglong, c_void};

use zstd_safe::{max_c_level, min_c_level};
use zstd_safe::zstd_sys::{
    ZSTD_CCtx, ZSTD_CCtx_setCParams, ZSTD_compress2, ZSTD_createCCtx, ZSTD_getCParams, ZSTD_isError,
};

use crate::pub_traits::pool::ByteBuffer;
use super::super::codec_trait::CodecCompressionError;
use super::{DST_FULL, DST_TOO_SMALL, ZstdCompressionError, ZstdErrorCode};


pub(super) struct ZstdEncoderImpl {
    raw_cctx:                  NonNull<ZSTD_CCtx>,
    clamped_compression_level: c_int,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl ZstdEncoderImpl {
    /// # Panics
    /// Panics if `ZSTD_createCCtx` returns null. (Presumably, this could happen due to OOM, for
    /// instance.)
    #[must_use]
    pub fn new(compression_level: i32) -> Self {
        // Safety: "Just FFI", quoting `zstd_safe::CCtx::try_create`.
        // In other words, `zstd_sys` probably could/should mark this safe.
        let raw_cctx = unsafe { ZSTD_createCCtx() };
        // Can fail due to e.g. OOM. Doesn't seem like this error case should be considered
        // common / normal to handle.
        #[expect(clippy::expect_used, reason = "Not an error that the user should need to handle")]
        let raw_cctx = NonNull::new(raw_cctx).expect("`ZSTD_createCCtx` failed");

        let clamped_compression_level = compression_level.clamp(min_c_level(), max_c_level());
        // LMAO, this lint doesn't normally trigger, since `c_int` is `i32` on most machines.
        #[allow(clippy::expect_used, reason = "the bounds are, originally, `c_int`s")]
        let clamped_compression_level = c_int::try_from(clamped_compression_level)
            .expect("values in `min_c_level(), max_c_level()` should fit in `c_int`");

        Self {
            raw_cctx,
            clamped_compression_level,
        }
    }

    pub fn encode<Buffer: ByteBuffer>(
        &mut self,
        src: &[u8],
        dst: &mut Buffer,
    ) -> Result<(), CodecCompressionError<ZstdCompressionError>> {
        let estimated_src_size = c_ulonglong::try_from(src.len()).unwrap_or(c_ulonglong::MAX);

        // SAFETY: Seems that Zstd places no preconditions on this function. This is just `unsafe`
        // because it's FFI that `zstd_sys` didn't bother to mark safe.
        let compression_params = unsafe {
            ZSTD_getCParams(self.clamped_compression_level, estimated_src_size, 0)
        };

        // SAFETY: `self.raw_cctx.as_ptr()` is a valid Zstd compression context that we have
        // exclusive read/write access to (during the body of this function). This function does
        // not stash a reference to the context somewhere else (or, at least not in a way
        // that can cause unsoundness).
        let result = unsafe {
            ZSTD_CCtx_setCParams(self.raw_cctx.as_ptr(), compression_params)
        };

        // SAFETY: "Just FFI", quoting `zstd-safe`'s internal `is_error` function.
        // Again, `zstd-sys` should probably mark this safe.
        let result_is_error = unsafe { ZSTD_isError(result) != 0 };

        if result_is_error {
            return Err(CodecCompressionError::Custom(ZstdCompressionError::ErrorCode(ZstdErrorCode(
                result,
            ))));
        }

        let dst_slice = dst.as_mut_slice();

        let dst_len: usize = dst_slice.len();
        let dst_ptr: *mut c_void = dst_slice.as_mut_ptr().cast();
        let src_len: usize = src.len();
        let src_ptr: *const c_void = src.as_ptr().cast();

        // SAFETY: Safety preconditions not explicitly elaborated, but:
        // - `self.raw_cctx.as_ptr()` is a valid Zstd compression context
        // - It is sound to write initialized bytes to offsets `0..dst_len` of `dst_ptr`.
        // - It is sound to read offsets `0..src_len` of `src_ptr` as initialized bytes.
        let compress_result = unsafe {
            ZSTD_compress2(self.raw_cctx.as_ptr(), dst_ptr, dst_len, src_ptr, src_len)
        };

        // SAFETY: "Just FFI", quoting `zstd-safe`'s internal `is_error` function.
        // Again, `zstd-sys` should probably mark this safe.
        let compress_result_is_error = unsafe { ZSTD_isError(compress_result) != 0 };

        if compress_result_is_error {
            match compress_result {
                DST_TOO_SMALL | DST_FULL => {
                    Err(CodecCompressionError::Incompressible)
                }
                other_code => {
                    Err(CodecCompressionError::Custom(ZstdCompressionError::ErrorCode(ZstdErrorCode(
                        other_code,
                    ))))
                }
            }
        } else {
            // Since `ZSTD_compress2` was successful, its return value is the number of bytes
            // written (which is at most `dst_len`, which should equal `dst.len()` if the `Buffer`
            // implementation is sane).
            let bytes_written = result;
            dst.set_len(bytes_written);
            Ok(())
        }
    }
}

#[cfg(not(feature = "zstd-experimental-compression"))]
mod safe_encoder_impl;
/// Uses Zstd features that are considered experimental (and which require static linking).
///
/// (This implementation is closer to what Google's leveldb uses.)
#[cfg(feature = "zstd-experimental-compression")]
mod experimental_encoder_impl;
mod decoder_impl;


use std::fmt::{Debug, Formatter, Result as FmtResult};

use zstd_safe::{CLEVEL_DEFAULT, zstd_sys::ZSTD_ErrorCode};

use crate::utils::get_buffer;
use crate::pub_traits::pool::{BufferPool, ByteBuffer as _};
use super::codec_trait::{CodecCompressionError, CodecDecompressionError, CompressionCodec};

#[cfg(not(feature = "zstd-experimental-compression"))]
use self::safe_encoder_impl::ZstdEncoderImpl;
#[cfg(feature = "zstd-experimental-compression")]
use self::experimental_encoder_impl::ZstdEncoderImpl;
use self::decoder_impl::ZstdDecoderImpl;


#[expect(
    clippy::as_conversions,
    reason = "constant is under 100 (which means that Zstd considers it stable)",
)]
const DST_TOO_SMALL: usize = ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall as usize;
#[expect(
    clippy::as_conversions,
    reason = "constant is under 100 (which implies that Zstd considers it stable)",
)]
const DST_FULL: usize = ZSTD_ErrorCode::ZSTD_error_noForwardProgress_destFull as usize;


/// Zstandard compression via bindings.
///
/// No dictionary is used, and compressed data is required to indicate the uncompressed size in
/// its header.
#[derive(Debug, Clone, Copy)]
pub struct ZstdCodec {
    /// The level of compression to perform when encoding/compressing data. Has no effect on
    /// decoding/decompressing data.
    pub compression_level: i32,
}

impl Default for ZstdCodec {
    #[inline]
    fn default() -> Self {
        Self {
            compression_level: CLEVEL_DEFAULT, // Currently 3.
        }
    }
}

impl CompressionCodec for ZstdCodec {
    type Encoder = ZstdEncoder;
    type Decoder = ZstdDecoder;
    type CompressionError   = ZstdCompressionError;
    type DecompressionError = ZstdDecompressionError;

    fn init_encoder(&self) -> Self::Encoder {
       ZstdEncoder(ZstdEncoderImpl::new(self.compression_level))
    }

    fn encode<Pool: BufferPool>(
        encoder:          &mut Self::Encoder,
        src:              &[u8],
        compression_goal: usize,
        pool:             &Pool,
        existing_buf:     &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecCompressionError<Self::CompressionError>> {
        let mut buf = get_buffer(pool, existing_buf, compression_goal)?;
        // Should not panic, since `buf.capacity()` should be at least `compression_goal`.
        buf.set_len(compression_goal);

        if let Err(err) = encoder.0.encode(src, &mut buf) {
            *existing_buf = Some(buf);
            Err(err)
        } else {
            Ok(buf)
        }
    }

    fn init_decoder(&self) -> Self::Decoder {
        ZstdDecoder(ZstdDecoderImpl::new())
    }

    fn decode<Pool: BufferPool>(
        decoder:      &mut Self::Decoder,
        src:          &[u8],
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecDecompressionError<Self::DecompressionError>> {
        decoder.0.decode(src, pool, existing_buf)
    }
}

pub struct ZstdEncoder(ZstdEncoderImpl);

impl Debug for ZstdEncoder {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "ZstdEncoder(..)")
    }
}

pub struct ZstdDecoder(ZstdDecoderImpl);

impl Debug for ZstdDecoder {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "ZstdDecoder(..)")
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ZstdCompressionError {
    ErrorCode(ZstdErrorCode),
}

#[derive(Debug, Clone, Copy)]
pub enum ZstdDecompressionError {
    ErrorCode(ZstdErrorCode),
    ContentSizeError,
    MissingContentSize,
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct ZstdErrorCode(pub usize);

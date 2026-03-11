use std::num::NonZeroU8;

use crate::{compression::CompressionCodec as _, pub_traits::pool::BufferPool};
use crate::pub_traits::compression::{
    CodecsCompressionError, CodecsDecompressionError, CompressionCodecs, CompressorId,
};

use super::{
    raw_snap_impl::{SnappyCodec, SnappyDecoder, SnappyEncoder, SnappyError},
    zstd_impl::{ZstdCodec, ZstdCompressionError, ZstdDecoder, ZstdDecompressionError, ZstdEncoder},
};


#[allow(clippy::unwrap_used, reason = "It's confirmed at comptime that 1 != 0")]
const SNAPPY_COMPRESSION: CompressorId = CompressorId(NonZeroU8::new(1).unwrap());

#[allow(clippy::unwrap_used, reason = "It's confirmed at comptime that 2 != 0")]
const ZSTD_COMPRESSION: CompressorId = CompressorId(NonZeroU8::new(2).unwrap());


#[derive(Debug, Clone, Copy)]
pub struct GoogleLevelDBCodecs {
    pub zstd_compression_level: i32,
}

impl CompressionCodecs for GoogleLevelDBCodecs {
    type Encoders = GoogleLevelDBEncoders;
    type Decoders = GoogleLevelDBDecoders;
    type CompressionError   = SnappyOrZstdCompressionError;
    type DecompressionError = SnappyOrZstdDecompressionError;

    fn init_encoders(&self) -> Self::Encoders {
        let zstd_codec = ZstdCodec {
            compression_level: self.zstd_compression_level,
        };

        GoogleLevelDBEncoders(SnappyCodec.init_encoder(), zstd_codec.init_encoder())
    }

    fn encode<Pool: BufferPool>(
        encoders:         &mut Self::Encoders,
        src:              &[u8],
        id:               CompressorId,
        compression_goal: usize,
        pool:             &Pool,
        existing_buf:     &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecsCompressionError<Self::CompressionError>> {
        match id {
            SNAPPY_COMPRESSION => {
                SnappyCodec::encode(&mut encoders.0, src, compression_goal, pool, existing_buf)
                    .map_err(|err| err.map_custom(SnappyOrZstdCompressionError::Snappy))
                    .map_err(Into::into)
            }
            ZSTD_COMPRESSION => {
                ZstdCodec::encode(&mut encoders.1, src, compression_goal, pool, existing_buf)
                    .map_err(|err| err.map_custom(SnappyOrZstdCompressionError::Zstd))
                    .map_err(Into::into)
            }
            _ => Err(CodecsCompressionError::Unsupported),
        }
    }

    fn init_decoders(&self) -> Self::Decoders {
        let zstd_codec = ZstdCodec {
            compression_level: self.zstd_compression_level,
        };

        GoogleLevelDBDecoders(SnappyCodec.init_decoder(), zstd_codec.init_decoder())
    }

    fn decode<Pool: BufferPool>(
        decoders:     &mut Self::Decoders,
        src:          &[u8],
        id:           CompressorId,
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecsDecompressionError<Self::DecompressionError>> {
        match id {
            SNAPPY_COMPRESSION => {
                SnappyCodec::decode(&mut decoders.0, src, pool, existing_buf)
                    .map_err(|err| err.map_custom(SnappyOrZstdDecompressionError::Snappy))
                    .map_err(Into::into)
            }
            ZSTD_COMPRESSION => {
                ZstdCodec::decode(&mut decoders.1, src, pool, existing_buf)
                    .map_err(|err| err.map_custom(SnappyOrZstdDecompressionError::Zstd))
                    .map_err(Into::into)
            }
            _ => Err(CodecsDecompressionError::Unsupported),
        }
    }
}

#[derive(Debug)]
pub struct GoogleLevelDBEncoders(SnappyEncoder, ZstdEncoder);

#[derive(Debug)]
pub struct GoogleLevelDBDecoders(SnappyDecoder, ZstdDecoder);

#[derive(Debug, Clone)]
pub enum SnappyOrZstdCompressionError {
    Snappy(SnappyError),
    Zstd(ZstdCompressionError),
}

#[derive(Debug, Clone)]
pub enum SnappyOrZstdDecompressionError {
    Snappy(SnappyError),
    Zstd(ZstdDecompressionError),
}

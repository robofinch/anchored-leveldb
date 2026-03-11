use std::num::NonZeroU8;

use crate::{compression::CompressionCodec as _, pub_traits::pool::BufferPool};
use crate::pub_traits::compression::{
    CodecsCompressionError, CodecsDecompressionError, CompressionCodecs, CompressorId,
};
use super::zlib_impl::{ZlibCodec, ZlibDecoder, ZlibDeflateError, ZlibEncoder, ZlibInflateError};


#[allow(clippy::unwrap_used, reason = "It's confirmed at comptime that 2 != 0")]
const ZLIB_COMPRESSION: CompressorId = CompressorId(NonZeroU8::new(2).unwrap());

#[allow(clippy::unwrap_used, reason = "It's confirmed at comptime that 4 != 0")]
const RAW_ZLIB_COMPRESSION: CompressorId = CompressorId(NonZeroU8::new(4).unwrap());


#[derive(Debug, Clone, Copy)]
pub struct MojangLevelDBCodecs;

impl CompressionCodecs for MojangLevelDBCodecs {
    type Encoders = MojangLevelDBCompressors;
    type Decoders = MojangLevelDBDecompressors;
    type CompressionError = ZlibDeflateError;
    type DecompressionError = ZlibInflateError;

    fn init_encoders(&self) -> Self::Encoders {
        MojangLevelDBCompressors {
            with_header:    None,
            without_header: ZlibCodec::with_default_compression(false).init_encoder(),
        }
    }

    #[inline]
    fn encode<Pool: BufferPool>(
        encoders:         &mut Self::Encoders,
        src:              &[u8],
        id:               CompressorId,
        compression_goal: usize,
        pool:             &Pool,
        existing_buf:     &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecsCompressionError<Self::CompressionError>> {
        match id {
            ZLIB_COMPRESSION => {
                let encoder = encoders.with_header.get_or_insert_with(|| {
                    ZlibCodec::with_default_compression(true).init_encoder()
                });

                ZlibCodec::encode(encoder, src, compression_goal, pool, existing_buf)
                    .map_err(Into::into)
            }
            RAW_ZLIB_COMPRESSION => {
                let encoder = &mut encoders.without_header;

                ZlibCodec::encode(encoder, src, compression_goal, pool, existing_buf)
                    .map_err(Into::into)
            }
            _ => Err(CodecsCompressionError::Unsupported),
        }
    }

    fn init_decoders(&self) -> Self::Decoders {
        MojangLevelDBDecompressors(ZlibCodec::with_default_compression(false).init_decoder())
    }

    #[inline]
    fn decode<Pool: BufferPool>(
        decoders:     &mut Self::Decoders,
        src:          &[u8],
        id:           CompressorId,
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecsDecompressionError<Self::DecompressionError>> {
        let zlib_header = match id {
            ZLIB_COMPRESSION     => true,
            RAW_ZLIB_COMPRESSION => false,
            _                    => return Err(CodecsDecompressionError::Unsupported),
        };

        decoders.0.set_zlib_header(zlib_header);

        ZlibCodec::decode(&mut decoders.0, src, pool, existing_buf).map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct MojangLevelDBCompressors {
    /// Mojang no longer uses this style of compression, but old Minecraft worlds *could* still
    /// use it. Don't waste the ~3000 bytes of memory on initializing the stream unless necessary.
    with_header:    Option<ZlibEncoder>,
    without_header: ZlibEncoder,
}

#[derive(Debug)]
pub struct MojangLevelDBDecompressors(ZlibDecoder);

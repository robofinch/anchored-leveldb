use std::num::NonZeroU8;

use super::pool::BufferPool;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompressorId(pub NonZeroU8);

pub trait CompressionCodecs {
    type Encoders;
    type Decoders;
    type CompressionError;
    type DecompressionError;

    #[must_use]
    fn init_encoders(&self) -> Self::Encoders;

    fn encode<Pool: BufferPool>(
        encoders:     &mut Self::Encoders,
        src:          &[u8],
        id:           CompressorId,
        pool:         &Pool,
        existing_buf: Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, Self::CompressionError>;

    #[must_use]
    fn init_decoders(&self) -> Self::Decoders;

    fn decode<Pool: BufferPool>(
        decoders:     &mut Self::Decoders,
        src:          &[u8],
        id:           CompressorId,
        pool:         &Pool,
        existing_buf: Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, Self::DecompressionError>;
}

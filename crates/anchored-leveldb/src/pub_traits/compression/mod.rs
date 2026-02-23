use std::num::NonZeroU8;

use super::pool::BufferPool;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompressorId(pub NonZeroU8);

pub trait CompressionCodecs {
    type Encoders;
    type Decoders;
    type CompressionError: CompressionCodecError;
    type DecompressionError: CompressionCodecError;

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

pub trait CompressionCodecError {
    /// The error indicates that a [`CompressorId`] is unsupported.
    ///
    /// Note that, if this function returns `true`, this error may be discarded in favor of simply
    /// noting that the relevant [`CompressorId`] is unsupported.
    #[must_use]
    fn is_unsupported_compressor_id(&self) -> bool;

    /// The error indicates that [`BufferPool::try_get_buffer`] returned [`BufferAllocError`].
    ///
    /// Note that, if this function returns `true`, this error may be discarded in favor of simply
    /// noting that a [`BufferAllocError`] occurred.
    ///
    /// [`BufferPool::try_get_buffer`]: super::pool::BufferPool::try_get_buffer
    /// [`BufferAllocError`]: super::pool::BufferAllocError
    #[must_use]
    fn is_buffer_alloc_err(&self) -> bool;
}

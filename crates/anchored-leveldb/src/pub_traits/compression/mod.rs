use std::num::NonZeroU8;

use super::pool::{BufferPool, BufferAllocError};


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
        encoders:         &mut Self::Encoders,
        src:              &[u8],
        id:               CompressorId,
        compression_goal: usize,
        pool:             &Pool,
        existing_buf:     &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecsCompressionError<Self::CompressionError>>;

    #[must_use]
    fn init_decoders(&self) -> Self::Decoders;

    fn decode<Pool: BufferPool>(
        decoders:     &mut Self::Decoders,
        src:          &[u8],
        id:           CompressorId,
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecsDecompressionError<Self::DecompressionError>>;
}

#[derive(Debug, Clone, Copy)]
pub enum CodecsCompressionError<E> {
    /// The given [`CompressorId`] is unsupported.
    Unsupported,
    /// The error indicates that [`BufferPool::try_get_buffer`] returned [`BufferAllocError`].
    ///
    /// [`BufferPool::try_get_buffer`]: crate::pub_traits::pool::BufferPool::try_get_buffer
    /// [`BufferAllocError`]: crate::pub_traits::pool::BufferAllocError
    BufferAlloc,
    /// The source data could not be compressed to under `compression_goal` bytes.
    Incompressible,
    Custom(E),
}

impl<E> From<BufferAllocError> for CodecsCompressionError<E> {
    #[inline]
    fn from(error: BufferAllocError) -> Self {
        let BufferAllocError {} = error;
        Self::BufferAlloc
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CodecsDecompressionError<E> {
    /// The given [`CompressorId`] is unsupported.
    Unsupported,
    /// The error indicates that [`BufferPool::try_get_buffer`] returned [`BufferAllocError`].
    ///
    /// [`BufferPool::try_get_buffer`]: crate::pub_traits::pool::BufferPool::try_get_buffer
    /// [`BufferAllocError`]: crate::pub_traits::pool::BufferAllocError
    BufferAlloc,
    Custom(E),
}

impl<E> From<BufferAllocError> for CodecsDecompressionError<E> {
    #[inline]
    fn from(error: BufferAllocError) -> Self {
        let BufferAllocError {} = error;
        Self::BufferAlloc
    }
}

use crate::pub_traits::{
    compression::{CodecsCompressionError, CodecsDecompressionError},
    pool::{BufferAllocError, BufferPool},
};


pub trait CompressionCodec {
    type Encoder;
    type Decoder;
    type CompressionError;
    type DecompressionError;

    #[must_use]
    fn init_encoder(&self) -> Self::Encoder;

    fn encode<Pool: BufferPool>(
        encoder:          &mut Self::Encoder,
        src:              &[u8],
        compression_goal: usize,
        pool:             &Pool,
        existing_buf:     &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecCompressionError<Self::CompressionError>>;

    #[must_use]
    fn init_decoder(&self) -> Self::Decoder;

    fn decode<Pool: BufferPool>(
        decoder:      &mut Self::Decoder,
        src:          &[u8],
        pool:         &Pool,
        existing_buf: &mut Option<Pool::PooledBuffer>,
    ) -> Result<Pool::PooledBuffer, CodecDecompressionError<Self::DecompressionError>>;
}

#[derive(Debug, Clone, Copy)]
pub enum CodecCompressionError<E> {
    /// The error indicates that [`BufferPool::try_get_buffer`] returned [`BufferAllocError`].
    ///
    /// [`BufferPool::try_get_buffer`]: crate::pub_traits::pool::BufferPool::try_get_buffer
    /// [`BufferAllocError`]: crate::pub_traits::pool::BufferAllocError
    BufferAlloc,
    /// The source data could not be compressed to under `compression_goal` bytes.
    Incompressible,
    Custom(E),
}

impl<E> CodecCompressionError<E> {
    #[inline]
    #[must_use]
    pub fn map_custom<F, O: FnOnce(E) -> F>(self, op: O) -> CodecCompressionError<F> {
        match self {
            Self::BufferAlloc    => CodecCompressionError::BufferAlloc,
            Self::Incompressible => CodecCompressionError::Incompressible,
            Self::Custom(custom) => CodecCompressionError::Custom(op(custom)),
        }
    }
}

impl<E> From<BufferAllocError> for CodecCompressionError<E> {
    #[inline]
    fn from(error: BufferAllocError) -> Self {
        let BufferAllocError {} = error;
        Self::BufferAlloc
    }
}

impl<E> From<CodecCompressionError<E>> for CodecsCompressionError<E> {
    #[inline]
    fn from(error: CodecCompressionError<E>) -> Self {
        match error {
            CodecCompressionError::BufferAlloc    => Self::BufferAlloc,
            CodecCompressionError::Incompressible => Self::Incompressible,
            CodecCompressionError::Custom(custom) => Self::Custom(custom),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CodecDecompressionError<E> {
    /// The error indicates that [`BufferPool::try_get_buffer`] returned [`BufferAllocError`].
    ///
    /// [`BufferPool::try_get_buffer`]: crate::pub_traits::pool::BufferPool::try_get_buffer
    /// [`BufferAllocError`]: crate::pub_traits::pool::BufferAllocError
    BufferAlloc,
    Custom(E),
}

impl<E> CodecDecompressionError<E> {
    #[inline]
    #[must_use]
    pub fn map_custom<F, O: FnOnce(E) -> F>(self, op: O) -> CodecDecompressionError<F> {
        match self {
            Self::BufferAlloc    => CodecDecompressionError::BufferAlloc,
            Self::Custom(custom) => CodecDecompressionError::Custom(op(custom)),
        }
    }
}

impl<E> From<BufferAllocError> for CodecDecompressionError<E> {
    #[inline]
    fn from(error: BufferAllocError) -> Self {
        let BufferAllocError {} = error;
        Self::BufferAlloc
    }
}

impl<E> From<CodecDecompressionError<E>> for CodecsDecompressionError<E> {
    #[inline]
    fn from(error: CodecDecompressionError<E>) -> Self {
        match error {
            CodecDecompressionError::BufferAlloc    => Self::BufferAlloc,
            CodecDecompressionError::Custom(custom) => Self::Custom(custom),
        }
    }
}

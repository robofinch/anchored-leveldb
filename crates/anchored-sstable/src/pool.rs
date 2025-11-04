use std::borrow::BorrowMut;
use anchored_pool::{PooledBuffer, SharedUnboundedBufferPool, UnboundedBufferPool};


pub trait BufferPool {
    /// A handle to a buffer. When dropped, the buffer is returned to the pool.
    type PooledBuffer: BorrowMut<Vec<u8>>;

    /// Get a buffer from the pool, returning a new empty buffer if none were available in the pool.
    fn get_buffer(&self) -> Self::PooledBuffer;
}

impl BufferPool for UnboundedBufferPool {
    type PooledBuffer = PooledBuffer<Self>;

    fn get_buffer(&self) -> Self::PooledBuffer {
        self.get()
    }
}

impl BufferPool for SharedUnboundedBufferPool {
    type PooledBuffer = PooledBuffer<Self>;

    fn get_buffer(&self) -> Self::PooledBuffer {
        self.get()
    }
}

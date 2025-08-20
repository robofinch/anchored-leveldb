#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::{
    bounded::BoundedPool,
    shared_bounded::SharedBoundedPool,
    shared_unbounded::SharedUnboundedPool,
    unbounded::UnboundedPool,
};
use crate::{
    other_utils::{OutOfBuffers, ResetBuffer, ResourcePoolEmpty},
    pooled_resource::{PooledBuffer, SealedBufferPool},
};


#[derive(Debug, Clone)]
pub struct BoundedBufferPool(BoundedPool<Vec<u8>, ResetBuffer>);

impl SealedBufferPool for BoundedBufferPool {
    type InnerPool = BoundedPool<Vec<u8>, ResetBuffer>;
}

impl BoundedBufferPool {
    /// Create a new `BoundedBufferPool` which has the indicated, fixed number of `Vec<u8>` buffers.
    ///
    /// Whenever a buffer is returned to the pool, if its capacity is at most `max_buffer_capacity`,
    /// then [`Vec::clear`] is run on it; otherwise, it is replaced with a new empty `Vec<u8>`.
    #[inline]
    #[must_use]
    pub fn new(pool_size: usize, max_buffer_capacity: usize) -> Self {
        Self(BoundedPool::new_default(pool_size, ResetBuffer::new(max_buffer_capacity)))
    }

    /// Get a buffer from the pool, if any are available.
    pub fn try_get(&self) -> Result<PooledBuffer<Self>, OutOfBuffers> {
        self.0.try_get()
            .map(PooledBuffer::new)
            .map_err(|ResourcePoolEmpty| OutOfBuffers)
    }

    /// Get a buffer from the pool.
    ///
    /// # Panics
    /// Panics if no buffers are currently available. As `BoundedBufferPool` is `!Send + !Sync`, no
    /// buffer could ever become available while in the body of this function.
    #[must_use]
    pub fn get(&self) -> PooledBuffer<Self> {
        PooledBuffer::new(self.0.get())
    }

    /// Get the total number of buffers in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
       self.0.pool_size()
    }

    /// Get the number of buffers in the pool which are not currently being used.
    #[must_use]
    pub fn available_buffers(&self) -> usize {
        self.0.available_resources()
    }
}

#[cfg(feature = "clone-behavior")]
impl<S: Speed> MirroredClone<S> for BoundedBufferPool {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(MirroredClone::<S>::mirrored_clone(&self.0))
    }
}

#[derive(Debug, Clone)]
pub struct SharedBoundedBufferPool(SharedBoundedPool<Vec<u8>, ResetBuffer>);

impl SealedBufferPool for SharedBoundedBufferPool {
    type InnerPool = SharedBoundedPool<Vec<u8>, ResetBuffer>;
}

impl SharedBoundedBufferPool {
    /// Create a new `SharedBoundedPool` which has the indicated, fixed number of `Vec<u8>` buffers.
    ///
    /// Whenever a buffer is returned to the pool, if its capacity is at most `max_buffer_capacity`,
    /// then [`Vec::clear`] is run on it; otherwise, it is replaced with a new empty `Vec<u8>`.
    #[inline]
    #[must_use]
    pub fn new(pool_size: usize, max_buffer_capacity: usize) -> Self {
        Self(SharedBoundedPool::new_default(pool_size, ResetBuffer::new(max_buffer_capacity)))
    }

    /// Get a buffer from the pool, if any are available.
    pub fn try_get(&self) -> Result<PooledBuffer<Self>, OutOfBuffers> {
        self.0.try_get()
            .map(PooledBuffer::new)
            .map_err(|ResourcePoolEmpty| OutOfBuffers)
    }

    /// Get a buffer from the pool.
    ///
    /// May need to wait for a buffer to become available.
    ///
    /// # Potential Panics or Deadlocks
    /// If `self.pool_size() == 0`, then this method panics.
    /// This method may also cause a deadlock if no buffers are currently available, and the
    /// current thread needs to make progress in order to release a buffer.
    #[must_use]
    pub fn get(&self) -> PooledBuffer<Self> {
        PooledBuffer::new(self.0.get())
    }

    /// Get the total number of buffers in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
       self.0.pool_size()
    }

    /// Get the number of buffers in the pool which are not currently being used.
    #[must_use]
    pub fn available_buffers(&self) -> usize {
        self.0.available_resources()
    }
}

#[cfg(feature = "clone-behavior")]
impl<S: Speed> MirroredClone<S> for SharedBoundedBufferPool {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(MirroredClone::<S>::mirrored_clone(&self.0))
    }
}

#[derive(Debug, Clone)]
pub struct UnboundedBufferPool(UnboundedPool<Vec<u8>, ResetBuffer>);

impl SealedBufferPool for UnboundedBufferPool {
    type InnerPool = UnboundedPool<Vec<u8>, ResetBuffer>;
}

impl UnboundedBufferPool {
    /// Create a new `UnboundedBufferPool`, which initially has zero `Vec<u8>` buffers.
    ///
    /// Whenever a buffer is returned to the pool, if its capacity is at most `max_buffer_capacity`,
    /// then [`Vec::clear`] is run on it; otherwise, it is replaced with a new empty `Vec<u8>`.
    #[inline]
    #[must_use]
    pub fn new(max_buffer_capacity: usize) -> Self {
        Self(UnboundedPool::new(ResetBuffer::new(max_buffer_capacity)))
    }

    /// Get a buffer from the pool, returning a new empty buffer if none were available in the pool.
    #[must_use]
    pub fn get(&self) -> PooledBuffer<Self> {
        PooledBuffer::new(self.0.get_default())
    }

    /// Get the total number of buffers in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
       self.0.pool_size()
    }

    /// Get the number of buffers in the pool which are not currently being used.
    #[must_use]
    pub fn available_buffers(&self) -> usize {
        self.0.available_resources()
    }

    /// Discard extra unused buffers, keeping only the first `max_unused` unused buffers.
    pub fn trim_unused(&self, max_unused: usize) {
        self.0.trim_unused(max_unused);
    }
}

#[cfg(feature = "clone-behavior")]
impl<S: Speed> MirroredClone<S> for UnboundedBufferPool {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(MirroredClone::<S>::mirrored_clone(&self.0))
    }
}

#[derive(Debug, Clone)]
pub struct SharedUnboundedBufferPool(SharedUnboundedPool<Vec<u8>, ResetBuffer>);

impl SealedBufferPool for SharedUnboundedBufferPool {
    type InnerPool = SharedUnboundedPool<Vec<u8>, ResetBuffer>;
}

impl SharedUnboundedBufferPool {
    /// Create a new `SharedUnboundedBufferPool`, which initially has zero `Vec<u8>` buffers.
    ///
    /// Whenever a buffer is returned to the pool, if its capacity is at most `max_buffer_capacity`,
    /// then [`Vec::clear`] is run on it; otherwise, it is replaced with a new empty `Vec<u8>`.
    #[inline]
    #[must_use]
    pub fn new(max_buffer_capacity: usize) -> Self {
        Self(SharedUnboundedPool::new(ResetBuffer::new(max_buffer_capacity)))
    }

    /// Get a buffer from the pool, returning a new empty buffer if none were available in the pool.
    #[must_use]
    pub fn get(&self) -> PooledBuffer<Self> {
        PooledBuffer::new(self.0.get_default())
    }

    /// Get the total number of buffers in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
       self.0.pool_size()
    }

    /// Get the number of buffers in the pool which are not currently being used.
    #[must_use]
    pub fn available_buffers(&self) -> usize {
        self.0.available_resources()
    }

    /// Discard extra unused buffers, keeping only the first `max_unused` unused buffers.
    pub fn trim_unused(&self, max_unused: usize) {
        self.0.trim_unused(max_unused);
    }
}

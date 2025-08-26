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


/// A pool with a fixed number of `Vec<u8>` buffers.
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

/// A threadsafe pool with a fixed number of `Vec<u8>` buffers.
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

/// A pool with a growable number of `Vec<u8>` buffers.
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

/// A threadsafe pool with a growable number of `Vec<u8>` buffers.
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


#[cfg(test)]
mod bounded_tests {
    use std::{array, iter};
    use super::*;


    #[test]
    fn zero_capacity() {
        let pool = BoundedBufferPool::new(0, 0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_buffers(), 0);
        assert!(pool.try_get().is_err());
    }

    #[test]
    #[should_panic]
    fn zero_capacity_fail() {
        let pool = BoundedBufferPool::new(0, 0);
        let unreachable = pool.get();
        let _: &Vec<u8> = &*unreachable;
    }

    #[test]
    fn one_capacity() {
        let pool = BoundedBufferPool::new(1, 1);
        let buffer = pool.get();
        assert_eq!(pool.available_buffers(), 0);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        drop(buffer);
        assert_eq!(pool.available_buffers(), 1);
        let mut buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        buffer.reserve(2);
        assert_eq!(pool.available_buffers(), 0);
        drop(buffer);
        assert_eq!(pool.available_buffers(), 1);
        // It got reset, since 2 capacity exceeds 1
        let mut buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);

        buffer.reserve_exact(1);
        buffer.push(1);
        // `reserve_exact` could theoretically allocate extra elements
        if buffer.capacity() == 1 {
            drop(buffer);
            // The buffer got cleared, but is still the same buffer
            let buffer = pool.get();
            assert_eq!(buffer.len(), 0);
            assert_eq!(buffer.capacity(), 1);
        }
    }

    #[test]
    #[should_panic]
    fn one_capacity_fail() {
        let pool = BoundedBufferPool::new(1, 1);
        let _buf = pool.get();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_buffers(), 0);
        let _unreachable = pool.get();
    }

    #[test]
    fn init_and_reset() {
        const POOL_CAPACITY: usize = 10;
        const BUF_CAPACITY: usize = 4096;

        let pool = BoundedBufferPool::new(POOL_CAPACITY, BUF_CAPACITY);
        let buffers: [_; POOL_CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, mut buffer) in buffers.into_iter().enumerate() {
            buffer.reserve_exact(idx);
            buffer.extend(iter::repeat_n(0, idx));
            assert_eq!(buffer.len(), idx);
            // If the allocator allocated more than 4096 bytes in response to a request for
            // at most 10... just exit the test early.
            if buffer.capacity() > BUF_CAPACITY {
                return;
            }
        }

        // Their lengths but not capacities have been reset.
        // NOTE: users should not rely on the order.
        let buffers: [_; POOL_CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, buffer) in buffers.into_iter().enumerate() {
            assert_eq!(buffer.len(), 0);
            // Technically need not be equal.
            assert!(buffer.capacity() >= idx);
        }
    }
}

#[cfg(test)]
mod shared_bounded_tests {
    use std::{array, iter, sync::mpsc, thread};
    use super::*;


    #[test]
    fn zero_capacity() {
        let pool = SharedBoundedBufferPool::new(0, 0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_buffers(), 0);
        assert!(pool.try_get().is_err());
    }

    #[test]
    fn one_capacity() {
        let pool = SharedBoundedBufferPool::new(1, 1);
        let buffer = pool.get();
        assert_eq!(pool.available_buffers(), 0);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        drop(buffer);
        assert_eq!(pool.available_buffers(), 1);
        let mut buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        buffer.reserve(2);
        assert_eq!(pool.available_buffers(), 0);
        drop(buffer);
        assert_eq!(pool.available_buffers(), 1);
        // It got reset, since 2 capacity exceeds 1
        let mut buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);

        buffer.reserve_exact(1);
        buffer.push(1);
        // `reserve_exact` could theoretically allocate extra elements
        if buffer.capacity() == 1 {
            drop(buffer);
            // The buffer got cleared, but is still the same buffer
            let buffer = pool.get();
            assert_eq!(buffer.len(), 0);
            assert_eq!(buffer.capacity(), 1);
        }
    }

    #[test]
    fn init_and_reset() {
        const POOL_CAPACITY: usize = 10;
        const BUF_CAPACITY: usize = 4096;

        let pool = SharedBoundedBufferPool::new(POOL_CAPACITY, BUF_CAPACITY);
        let buffers: [_; POOL_CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, mut buffer) in buffers.into_iter().enumerate() {
            buffer.reserve_exact(idx);
            buffer.extend(iter::repeat_n(0, idx));
            assert_eq!(buffer.len(), idx);
            // If the allocator allocated more than 4096 bytes in response to a request for
            // at most 10... just exit the test early.
            if buffer.capacity() > BUF_CAPACITY {
                return;
            }
        }

        // Their lengths but not capacities have been reset
        // NOTE: users should not rely on the order.
        let buffers: [_; POOL_CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, buffer) in buffers.into_iter().enumerate() {
            assert_eq!(buffer.len(), 0);
            // Technically need not be equal.
            assert!(buffer.capacity() >= idx);
        }
    }

    #[test]
    fn multithreaded_one_capacity() {
        const BUF_CAPACITY: usize = 4096;

        let pool = SharedBoundedBufferPool::new(1, BUF_CAPACITY);

        let cloned_pool = pool.clone();

        assert_eq!(pool.available_buffers(), 1);

        let (signal_main, wait_for_thread) = mpsc::channel();
        let (signal_thread, wait_for_main) = mpsc::channel();

        thread::spawn(move || {
            let mut buffer = cloned_pool.get();
            signal_main.send(()).unwrap();
            wait_for_main.recv().unwrap();
            // This shouldn't allocate 4096 bytes, but technically could
            assert_eq!(buffer.len(), 0);
            if buffer.capacity() > BUF_CAPACITY {
                // This drops `signal_main` and still causes a test failure, but in a noticeably
                // different way.
                // But again, this should not happen unless the global allocator is set to
                // something truly strange.
                return;
            }
            buffer.push(42);
            drop(buffer);
            signal_main.send(()).unwrap();
        });

        wait_for_thread.recv().unwrap();
        assert_eq!(pool.available_buffers(), 0);
        signal_thread.send(()).unwrap();
        wait_for_thread.recv().unwrap();
        assert_eq!(pool.available_buffers(), 1);
        let buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.capacity() > 0);
    }
}


#[cfg(test)]
mod unbounded_tests {
    use std::{array, iter};
    use super::*;


    #[test]
    fn zero_or_one_size() {
        let pool = UnboundedBufferPool::new(0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_buffers(), 0);

        let buffer = pool.get();
        let _: &Vec<u8> = &buffer;
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_buffers(), 0);

        drop(buffer);
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_buffers(), 1);

        pool.trim_unused(0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_buffers(), 0);
    }

    #[test]
    fn one_capacity() {
        let pool = UnboundedBufferPool::new(1);
        let buffer = pool.get();
        assert_eq!(pool.available_buffers(), 0);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        drop(buffer);
        assert_eq!(pool.available_buffers(), 1);
        let mut buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        buffer.reserve(2);
        assert_eq!(pool.available_buffers(), 0);
        drop(buffer);
        assert_eq!(pool.available_buffers(), 1);
        // It got reset, since 2 capacity exceeds 1
        let mut buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);

        buffer.reserve_exact(1);
        buffer.push(1);
        // `reserve_exact` could theoretically allocate extra elements
        if buffer.capacity() == 1 {
            drop(buffer);
            // The buffer got cleared, but is still the same buffer
            let buffer = pool.get();
            assert_eq!(buffer.len(), 0);
            assert_eq!(buffer.capacity(), 1);
        }
    }

    #[test]
    fn init_and_reset() {
        const POOL_SIZE: usize = 10;
        const BUF_CAPACITY: usize = 4096;

        let pool = UnboundedBufferPool::new(BUF_CAPACITY);
        let buffers: [_; POOL_SIZE] = array::from_fn(|_| pool.get());
        for (idx, mut buffer) in buffers.into_iter().enumerate() {
            buffer.reserve_exact(idx);
            buffer.extend(iter::repeat_n(0, idx));
            assert_eq!(buffer.len(), idx);
            // If the allocator allocated more than 4096 bytes in response to a request for
            // at most 10... just exit the test early.
            if buffer.capacity() > BUF_CAPACITY {
                return;
            }
        }

        // Their lengths but not capacities have been reset.
        // NOTE: users should not rely on the order.
        let buffers: [_; POOL_SIZE] = array::from_fn(|_| pool.get());
        // This one is new.
        assert_eq!(pool.get().capacity(), 0);

        for (idx, buffer) in buffers.into_iter().rev().enumerate() {
            assert_eq!(buffer.len(), 0);
            // Technically need not be equal.
            assert!(buffer.capacity() >= idx);
        }
    }
}

#[cfg(test)]
mod shared_unbounded_tests {
    use std::{array, iter, sync::mpsc, thread};
    use super::*;


    #[test]
    fn zero_or_one_size() {
        let pool: SharedUnboundedBufferPool = SharedUnboundedBufferPool::new(0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_buffers(), 0);

        let buffer = pool.get();
        let _: &Vec<u8> = &buffer;
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_buffers(), 0);

        drop(buffer);
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_buffers(), 1);

        pool.trim_unused(0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_buffers(), 0);
    }

    #[test]
    fn one_capacity() {
        let pool = SharedUnboundedBufferPool::new(1);
        let buffer = pool.get();
        assert_eq!(pool.available_buffers(), 0);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        drop(buffer);
        assert_eq!(pool.available_buffers(), 1);
        let mut buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        buffer.reserve(2);
        assert_eq!(pool.available_buffers(), 0);
        drop(buffer);
        assert_eq!(pool.available_buffers(), 1);
        // It got reset, since 2 capacity exceeds 1
        let mut buffer = pool.get();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);

        buffer.reserve_exact(1);
        buffer.push(1);
        // `reserve_exact` could theoretically allocate extra elements
        if buffer.capacity() == 1 {
            drop(buffer);
            // The buffer got cleared, but is still the same buffer
            let buffer = pool.get();
            assert_eq!(buffer.len(), 0);
            assert_eq!(buffer.capacity(), 1);
        }
    }

    #[test]
    fn init_and_reset() {
        const POOL_SIZE: usize = 10;
        const BUF_CAPACITY: usize = 4096;

        let pool = SharedUnboundedBufferPool::new(BUF_CAPACITY);
        let buffers: [_; POOL_SIZE] = array::from_fn(|_| pool.get());
        for (idx, mut buffer) in buffers.into_iter().enumerate() {
            buffer.reserve_exact(idx);
            buffer.extend(iter::repeat_n(0, idx));
            assert_eq!(buffer.len(), idx);
            // If the allocator allocated more than 4096 bytes in response to a request for
            // at most 10... just exit the test early.
            if buffer.capacity() > BUF_CAPACITY {
                return;
            }
        }

        // Their lengths but not capacities have been reset.
        // NOTE: users should not rely on the order.
        let buffers: [_; POOL_SIZE] = array::from_fn(|_| pool.get());
        // This one is new.
        assert_eq!(pool.get().capacity(), 0);

        for (idx, buffer) in buffers.into_iter().rev().enumerate() {
            assert_eq!(buffer.len(), 0);
            // Technically need not be equal.
            assert!(buffer.capacity() >= idx);
        }
    }

    #[test]
    fn multithreaded_one_capacity() {
        const BUF_CAPACITY: usize = 4096;

        let pool = SharedUnboundedBufferPool::new(BUF_CAPACITY);

        let cloned_pool = pool.clone();

        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_buffers(), 0);

        let (signal_main, wait_for_thread) = mpsc::channel();
        let (signal_thread, wait_for_main) = mpsc::channel();

        thread::spawn(move || {
            let mut buffer = cloned_pool.get();
            signal_main.send(()).unwrap();
            wait_for_main.recv().unwrap();
            // This shouldn't allocate 4096 bytes, but technically could
            assert_eq!(buffer.len(), 0);
            if buffer.capacity() > BUF_CAPACITY {
                // This drops `signal_main` and still causes a test failure, but in a noticeably
                // different way.
                // But again, this should not happen unless the global allocator is set to
                // something truly strange.
                return;
            }
            buffer.push(42);
            drop(buffer);
            signal_main.send(()).unwrap();
        });

        wait_for_thread.recv().unwrap();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_buffers(), 0);

        signal_thread.send(()).unwrap();
        wait_for_thread.recv().unwrap();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_buffers(), 1);

        let buffer = pool.get();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_buffers(), 0);

        assert_eq!(buffer.len(), 0);
        assert!(buffer.capacity() > 0);
    }
}

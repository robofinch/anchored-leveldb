use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};


/// Returned if a buffer with the requested capacity could not be allocated.
#[derive(Debug, Default, Clone, Copy)]
pub struct BufferAllocError;

impl Display for BufferAllocError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "memory allocation for an anchored-leveldb PooledBuffer failed")
    }
}

impl Error for BufferAllocError {}

/// A pool of byte buffers.
///
/// Implementors are strongly encouraged, though not strictly required, to reuse the pooled buffers.
/// That is, the pooled buffers should be returned to the pool when they are dropped, and
/// [`get_buffer`] and [`try_get_buffer`] should be able to return previously-used buffers.
///
/// [`get_buffer`]: BufferPool::get_buffer
/// [`try_get_buffer`]: BufferPool::try_get_buffer
pub trait BufferPool {
    type PooledBuffer: ByteBuffer;

    /// Get a byte buffer with length `0` and at least the requested minimum capacity.
    ///
    /// # Panics or Aborts
    /// If a buffer with the requested capacity cannot be obtained, then this function will
    /// not terminate. In particular, it may panic or abort the process.
    #[must_use]
    fn get_buffer(&self, min_capacity: usize) -> Self::PooledBuffer;

    /// Get a byte buffer with length `0` and at least the requested minimum capacity.
    fn try_get_buffer(&self, min_capacity: usize) -> Result<Self::PooledBuffer, BufferAllocError>;

    /// Expand the capacity of `buffer`, doubling its capacity (or expanding its capacity to 32
    /// bytes, if the previous capacity was less than 16 bytes).
    ///
    /// # Panics or Aborts
    /// If a buffer with the requested capacity cannot be obtained, then this function will
    /// not terminate. In particular, it may panic or abort the process.
    fn grow_amortized(&self, buffer: &mut Self::PooledBuffer) {
        // The new capacity should be twice the previous capacity, or 32, whichever is larger.
        let new_capacity = buffer.capacity().saturating_mul(2).max(32);
        let mut new_buffer = self.get_buffer(new_capacity);

        new_buffer.set_len(buffer.len());
        // Since `new_buffer` and `buffer` should have the same length, this should not panic.
        new_buffer.as_mut_slice().copy_from_slice(buffer.as_slice());

        // This drops the old `buffer`, returning it to the pool.
        *buffer = new_buffer;
    }

    /// Expand the capacity of `buffer`, doubling its capacity (or expanding its capacity to 32
    /// bytes, if the previous capacity was less than 16 bytes).
    fn try_grow_amortized(
        &self,
        buffer: &mut Self::PooledBuffer,
    ) -> Result<(), BufferAllocError> {
        // The new capacity should be twice the previous capacity, or 32, whichever is larger.
        let new_capacity = buffer.capacity().saturating_mul(2).max(32);
        let mut new_buffer = self.try_get_buffer(new_capacity)?;

        new_buffer.set_len(buffer.len());
        // Since `new_buffer` and `buffer` should have the same length, this should not panic.
        new_buffer.as_mut_slice().copy_from_slice(buffer.as_slice());

        // This drops the old `buffer`, returning it to the pool.
        *buffer = new_buffer;
        Ok(())
    }
}

/// A buffer of bytes whose spare capacity must be initialized.
pub trait ByteBuffer {
    /// Returns a shared reference to the buffer's data slice.
    ///
    /// The returned slice should have length `self.len()`, and this method should not change the
    /// length or capacity of the buffer.
    #[must_use]
    fn as_slice(&self) -> &[u8];

    /// Returns an exclusive reference to the buffer's data slice.
    ///
    /// The returned slice should have length `self.len()`, and this method should not change the
    /// length or capacity of the buffer.
    #[must_use]
    fn as_mut_slice(&mut self) -> &mut [u8];

    /// Returns the length of the byte buffer, akin to [`Vec::len`].
    ///
    /// `self.len()` should be at most `self.capacity()`. This method should not change the length
    /// or capacity of the buffer.
    #[must_use]
    fn len(&self) -> usize;

    /// Set the buffer's length to the indicated value.
    ///
    /// This method should not change the capacity of the buffer.
    ///
    /// # Panics
    /// Panics if `new_len > self.capacity()`.
    fn set_len(&mut self, new_len: usize);

    /// Checks whether the length of the buffer is `0`.
    ///
    /// This method should not change the length or capacity of the buffer.
    #[inline]
    #[must_use]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set the buffer's length to `0`.
    ///
    /// This method should not change the capacity of the buffer.
    #[inline]
    fn clear(&mut self) {
        self.set_len(0);
    }

    /// Returns a shared reference to a slice of the entire buffer.
    ///
    /// The returned slice should have length `self.capacity()`, and this method should not change
    /// the length or capacity of the buffer.
    fn as_entire_capacity_slice(&self) -> &[u8];

    /// Returns an exclusive reference to a slice of the entire buffer.
    ///
    /// The returned slice should have length `self.capacity()`, and this method should not change
    /// the length or capacity of the buffer.
    fn as_entire_capacity_slice_mut(&mut self) -> &mut [u8];

    /// Returns the number of bytes which the buffer's data slice could hold.
    ///
    /// This method should not change the length or capacity of the buffer.
    #[must_use]
    fn capacity(&self) -> usize;

    /// Returns a shared reference to offsets `self.len()..self.capacity()` of the buffer.
    ///
    /// The returned slice should have length `self.remaining_capacity()`, and this method should
    /// not change the length or capacity of the buffer.
    #[must_use]
    fn as_remaining_capacity_slice(&self) -> &[u8];

    /// Returns an exclusive reference to offsets `self.len()..self.capacity()` of the buffer.
    ///
    /// The returned slice should have length `self.remaining_capacity()`, and this method should
    /// not change the length or capacity of the buffer.
    #[must_use]
    fn as_remaining_capacity_slice_mut(&mut self) -> &mut [u8];

    /// Returns the number of additional bytes, beyond the bytes already known to be initialized,
    /// which the buffer's data slice could hold.
    ///
    /// `self.remaining_capacity()` should be equal to `self.capacity() - self.len()` (which should
    /// not underflow). This method should not change the length or capacity of the buffer.
    #[must_use]
    fn remaining_capacity(&self) -> usize;

    /// Checks whether the data slice takes up the full capacity of the buffer.
    ///
    /// That is, this method checks whether the length of the buffer is equal to its capacity.
    ///
    /// This method should never change the length or capacity of the buffer.
    #[inline]
    #[must_use]
    fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }
}

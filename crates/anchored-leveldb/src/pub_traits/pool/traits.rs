#![expect(unsafe_code, reason = "make a byte buffer trait that might have uninit spare capacity")]

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};


/// Returned if a buffer with the requested capacity could not be allocated.
#[derive(Debug, Default, Clone, Copy)]
pub struct BufferAllocErr;

impl Display for BufferAllocErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "memory allocation for an anchored-leveldb PooledBuffer failed")
    }
}

impl Error for BufferAllocErr {}

/// A pool of byte buffers.
///
/// Implementors are strongly encouraged, though not strictly required, to reuse the pooled buffers.
/// That is, the pooled buffers should be returned to the pool when they are dropped, and
/// [`get_buffer`] and [`try_get_buffer`] should be able to return previously-used buffers.
///
/// [`get_buffer`]: BufferPool::get_buffer
/// [`try_get_buffer`]: BufferPool::try_get_buffer
pub trait BufferPool {
    type PooledBuffer: PooledBuffer;

    /// Get a byte buffer with at least the requested minimum capacity.
    ///
    /// # Panics or Aborts
    /// If a buffer with the requested capacity cannot be obtained, then this function will
    /// not terminate. In particular, it may panic or abort the process.
    #[must_use]
    fn get_buffer(&self, min_capacity: usize) -> Self::PooledBuffer;

    fn try_get_buffer(&self, min_capacity: usize) -> Result<Self::PooledBuffer, BufferAllocErr>;
}

/// A buffer of bytes. The spare capacity may or may not be initialized.
///
/// # Safety
/// Implementors must ensure that each method is implemented correctly, as described by their
/// documentation.
///
/// Additionally, the buffer must be aliasable, in the since that *unlike* `&mut [u8]` or
/// (currently) `Box<[u8]>`, moving the source buffer must not invalidate pointers to that buffer's
/// contents.
///
/// `unsafe` code is allowed to rely on the correctness of an arbitrary [`PooledBuffer`]
/// implementation.
pub unsafe trait PooledBuffer {
    /// Returns a raw pointer to the buffer's data slice.
    ///
    /// The memory that the pointer points to must not be written to (except inside an `UnsafeCell`)
    /// using this pointer or any pointer derived from it. The returned pointer may be invalidated
    /// if the buffer is mutated, or if other code materializes a mutable reference to part of
    /// the buffer's slice, among other possible causes.
    ///
    /// Note that the returned pointer may be a non-null dangling pointer (valid for zero-sized
    /// reads) if the buffer has capacity `0`.
    ///
    /// # Guarantees
    /// The returned pointer is non-null and dereferenceable for `self.capacity()` bytes.
    /// It is initialized for `self.len()` bytes, so reading those bytes as `u8`s is permitted.
    ///
    /// Moving the source buffer does not invalidate the returned pointer.
    ///
    /// This method never changes the length or capacity of the buffer.
    #[must_use]
    fn as_ptr(&self) -> *const u8;

    /// Returns a raw mutable pointer to the buffer's data slice.
    ///
    /// Writing initialized `u8` values into the data slice (that is, to any offset in
    /// `0..self.capacity()`) is permitted; writing *uninitialized* bytes, even into the spare
    /// capacity beyond `self.len()`, is not permitted. (Note also that the buffer need not
    /// guarantee the soundness of putting pointer provenance into the buffer's data slice.)
    ///
    /// The returned pointer may be invalidated if the buffer is accessed (except by moving it),
    /// in particular by calling a method (whether mutable or not) on it. Other code may also
    /// invalidate the pointer by materializing a reference to part of the buffer's slice. This
    /// list of ways to invalidate the returned pointer is not exhaustive.
    ///
    /// Note that the returned pointer may be a non-null dangling pointer (valid for zero-sized
    /// accesses) if the buffer has capacity `0`.
    ///
    /// # Guarantees
    /// The returned pointer is non-null and dereferenceable for `self.capacity()` bytes.
    /// It is initialized for `self.len()` bytes, so reading or writing those bytes as `u8`s is
    /// permitted.
    ///
    /// Writing initialized `u8` values into the buffer's data (that is, to any offset in
    /// `0..self.capacity()`) is permitted.
    ///
    /// Moving the source buffer does not invalidate the returned pointer.
    ///
    /// This method never changes the length or capacity of the buffer.
    #[must_use]
    fn as_mut_ptr(&mut self) -> *mut u8;

    /// Returns a shared reference to the buffer's data slice.
    ///
    /// # Guarantees
    /// The returned slice has length `self.len()`.
    ///
    /// Moving the source buffer does not invalidate the returned reference.
    ///
    /// This method never changes the length or capacity of the buffer.
    #[must_use]
    fn as_slice(&self) -> &[u8];

    /// Returns an exclusive reference to the buffer's data slice.
    ///
    /// # Guarantees
    /// The returned slice has length `self.len()`.
    ///
    /// Moving the source buffer does not invalidate the returned reference.
    ///
    /// This method never changes the length or capacity of the buffer.
    #[must_use]
    fn as_mut_slice(&mut self) -> &mut [u8];

    /// Returns the number of bytes in the buffer which are known to be initialized.
    ///
    /// # Guarantees
    /// `self.len()` is at most `self.capacity()`. Other methods also make various guarantees that
    /// depend on the buffer's length.
    ///
    /// This method never changes the length or capacity of the buffer.
    #[must_use]
    fn len(&self) -> usize;

    /// Checks whether the length of the buffer is `0`.
    ///
    /// # Guarantees
    /// This method never changes the length or capacity of the buffer.
    #[inline]
    #[must_use]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set the buffer's length to `0`.
    ///
    /// # Guarantees
    /// This method never changes the capacity of the buffer.
    fn clear(&mut self);

    /// Set the buffer's length to the indicated value.
    ///
    /// # Safety
    /// - `new_len` must be less than or equal to `self.capacity()`.
    /// - The bytes at indices `0..new_len` of the buffer's data slice must be initialized to
    ///   values of type `u8`.
    ///
    /// # Guarantees
    /// This method never changes the capacity of the buffer.
    unsafe fn set_len(&mut self, new_len: usize);

    /// Returns the number of bytes which the buffer's data slice could hold.
    ///
    /// Not all of those bytes are guaranteed to be initialized.
    ///
    /// # Guarantees
    /// Various methods make guarantees that depend on the buffer's capacity.
    ///
    /// This method never changes the length or capacity of the buffer.
    #[must_use]
    fn capacity(&self) -> usize;

    /// Returns the number of additional bytes, beyond the bytes already known to be initialized,
    /// which the buffer's data slice could hold.
    ///
    /// # Guarantees
    /// `self.remaining_capacity()` is equal to `self.capacity() - self.len()` (which is guaranteed
    /// to not underflow).
    ///
    /// This method never changes the length or capacity of the buffer.
    #[must_use]
    fn remaining_capacity(&self) -> usize;

    /// Checks whether the full capacity of the buffer is known to be initialized.
    ///
    /// That is, this method checks whether the length of the buffer is equal to its capacity.
    ///
    /// # Guarantees
    /// This method never changes the length or capacity of the buffer.
    #[inline]
    #[must_use]
    fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }
}

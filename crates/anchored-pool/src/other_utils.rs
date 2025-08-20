use std::mem;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

#[cfg(feature = "clone-behavior")]
use clone_behavior::{IndependentClone, MirroredClone, Speed};


/// An error that may, instead of waiting for a `Resource` to become available, be returned if no
/// `Resource`s were available in a bounded pool.
#[derive(Debug, Clone, Copy)]
pub struct ResourcePoolEmpty;

impl Display for ResourcePoolEmpty {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "a bounded resource pool operation was not performed \
             because there were no `Resource`s available",
        )
    }
}

impl Error for ResourcePoolEmpty {}

/// An error that may, instead of waiting for a buffer to become available, be returned if no
/// buffers were available in a bounded pool.
#[derive(Debug, Clone, Copy)]
pub struct OutOfBuffers;

impl Display for OutOfBuffers {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
         write!(
            f,
            "a bounded buffer pool operation was not performed \
             because there were no available buffers",
        )
    }
}

impl Error for OutOfBuffers {}

/// Trait for the operation performed when a `Resource` is returned to a pool.
pub trait ResetResource<Resource> {
    /// Operation performed when a `Resource` is returned to a pool.
    ///
    /// Intended for resetting the `Resource` to some blank state for future use.
    fn reset(&self, resource: &mut Resource);
}

impl<Resource, F: Fn(&mut Resource)> ResetResource<Resource> for F {
    #[inline]
    fn reset(&self, resource: &mut Resource) {
        self(resource);
    }
}

/// Implements [`ResetResource`] with a no-op. Can be used when a `Resource` does not need to be
/// reset when it is returned to a pool.
#[derive(Default, Debug, Clone, Copy)]
pub struct ResetNothing;

impl<Resource> ResetResource<Resource> for ResetNothing {
    /// Do nothing to reset the resource.
    fn reset(&self, _resource: &mut Resource) {}
}

#[cfg(feature = "clone-behavior")]
impl<S: Speed> IndependentClone<S> for ResetNothing {
    #[inline]
    fn independent_clone(&self) -> Self {
        *self
    }
}

#[cfg(feature = "clone-behavior")]
impl<S: Speed> MirroredClone<S> for ResetNothing {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

/// An implementation of <code>[ResetResource]<Vec\<u8\>></code>.
///
/// `ResetBuffer` resets a `Vec<u8>` by either clearing it without reducing its capacity, or by
/// resetting it to a new zero-capacity `Vec<u8>` if its capacity exceeds a chosen maximum
/// capacity.
#[derive(Debug, Clone, Copy)]
pub struct ResetBuffer {
    /// Must not be mutated after construction, in order to satisfy the semantics
    /// of our implementation of `MirroredClone`.
    max_buffer_capacity: usize,
}

impl ResetBuffer {
    /// Get an implementation of <code>[ResetResource]<Vec\<u8\>></code>.
    ///
    /// The created `ResetBuffer` resets a `Vec<u8>` by either clearing it without reducing its
    /// capacity, or by resetting it to a new zero-capacity `Vec<u8>` if its capacity exceeds
    /// `max_buffer_capacity`.
    #[inline]
    #[must_use]
    pub const fn new(max_buffer_capacity: usize) -> Self {
        Self {
            max_buffer_capacity,
        }
    }
}

impl ResetResource<Vec<u8>> for ResetBuffer {
    #[inline]
    fn reset(&self, resource: &mut Vec<u8>) {
        if resource.len() > self.max_buffer_capacity {
            // Take and drop the large buffer
            let _large_buf = mem::take(resource);
        } else {
            resource.clear();
        }
    }
}

#[cfg(feature = "clone-behavior")]
impl<S: Speed> IndependentClone<S> for ResetBuffer {
    #[inline]
    fn independent_clone(&self) -> Self {
        *self
    }
}

#[cfg(feature = "clone-behavior")]
impl<S: Speed> MirroredClone<S> for ResetBuffer {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        *self
    }
}

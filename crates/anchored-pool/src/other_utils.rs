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

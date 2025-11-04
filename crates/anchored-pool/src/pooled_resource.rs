#![expect(unsafe_code, reason = "let unsafe code in Pools rely on PooledResource Drop impl")]

use std::mem::ManuallyDrop;
use std::{
    borrow::{Borrow, BorrowMut},
    fmt::{Debug, Formatter, Result as FmtResult},
    ops::{Deref, DerefMut},
};


pub(crate) trait SealedPool<Resource> {
    type Returner;

    /// Used by [`PooledResource`] to return a `Resource` to a pool.
    ///
    /// # Safety
    /// Must be called at most once in the `Drop` impl of a `PooledResource` constructed
    /// via `PooledResource::new`, where `*returner` must be the `returner` value passed to
    /// `PooledResource::new`.
    unsafe fn return_resource(returner: &Self::Returner, resource: Resource);
}

pub(crate) trait SealedBufferPool {
    type InnerPool: SealedPool<Vec<u8>>;
}

/// A handle to `Resource` in a pool, which returns the `Resource` back to the pool when dropped.
#[expect(private_bounds, reason = "sealed")]
#[derive(Debug)]
pub struct PooledResource<Pool: SealedPool<Resource>, Resource> {
    /// Must not be mutated after construction
    returner:   Pool::Returner,
    resource:   ManuallyDrop<Resource>,
}

#[expect(private_bounds, reason = "sealed")]
impl<Pool: SealedPool<Resource>, Resource> PooledResource<Pool, Resource> {
    /// Create a new `PooledResource` that provides mutable access to a `Resource`, and returns
    /// that `Resource` to a `Pool` once dropped.
    ///
    /// # Safety
    /// It must be safe to call `pool.return_resource(pool_slot, any_resource)` one time in the
    /// `Drop` impl of this `PooledResource` struct, where `pool` and `pool_slot` are the values
    /// passed here, and `any_resource` is any `Resource` value.
    #[expect(clippy::missing_const_for_fn, reason = "no reason to promise const-ness")]
    #[inline]
    #[must_use]
    pub(crate) unsafe fn new(returner: Pool::Returner, resource: Resource) -> Self {
        Self {
            returner,
            resource: ManuallyDrop::new(resource),
        }
    }
}

impl<Pool: SealedPool<Resource>, Resource> Drop for PooledResource<Pool, Resource> {
    fn drop(&mut self) {
        // SAFETY:
        // We must never again use the `ManuallyDrop` value. This is the destructor of the type,
        // and the pool has no reference to the internal data of the `PooledResource`, so nothing
        // can touch `self.resource` after this line.
        let resource = unsafe { ManuallyDrop::take(&mut self.resource) };
        // SAFETY:
        // We call the method at most once in the way described by `Self::new`, which is the only
        // way to construct this type. By the safety contract of `Self::new`, this is safe.
        unsafe { Pool::return_resource(&self.returner, resource); }
    }
}

impl<Pool: SealedPool<Resource>, Resource> Deref for PooledResource<Pool, Resource> {
    type Target = Resource;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.resource
    }
}

impl<Pool: SealedPool<Resource>, Resource> DerefMut for PooledResource<Pool, Resource> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.resource
    }
}

impl<Pool: SealedPool<Resource>, Resource> Borrow<Resource> for PooledResource<Pool, Resource> {
    #[inline]
    fn borrow(&self) -> &Resource {
        self
    }
}

impl<Pool: SealedPool<Resource>, Resource> BorrowMut<Resource> for PooledResource<Pool, Resource> {
    #[inline]
    fn borrow_mut(&mut self) -> &mut Resource {
        self
    }
}

impl<Pool: SealedPool<Resource>, Resource> AsRef<Resource> for PooledResource<Pool, Resource> {
    #[inline]
    fn as_ref(&self) -> &Resource {
        self
    }
}

impl<Pool: SealedPool<Resource>, Resource> AsMut<Resource> for PooledResource<Pool, Resource> {
    #[inline]
    fn as_mut(&mut self) -> &mut Resource {
        self
    }
}

/// A handle to a buffer in a pool, which returns the buffer back to the pool when dropped.
#[expect(private_bounds, reason = "sealed")]
pub struct PooledBuffer<Pool: SealedBufferPool>(PooledResource<Pool::InnerPool, Vec<u8>>);

#[expect(private_bounds, reason = "sealed")]
impl<Pool: SealedBufferPool> PooledBuffer<Pool> {
    #[inline]
    #[must_use]
    pub(crate) const fn new(inner: PooledResource<Pool::InnerPool, Vec<u8>>) -> Self {
        Self(inner)
    }
}

impl<Pool: SealedBufferPool> Debug for PooledBuffer<Pool> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("PooledBuffer")
            .field(&format!("<buffer of length {} and capacity {}>", self.len(), self.capacity()))
            .finish()
    }
}

impl<Pool: SealedBufferPool> Deref for PooledBuffer<Pool> {
    type Target = Vec<u8>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Pool: SealedBufferPool> DerefMut for PooledBuffer<Pool> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<Pool: SealedBufferPool> Borrow<Vec<u8>> for PooledBuffer<Pool> {
    #[inline]
    fn borrow(&self) -> &Vec<u8> {
        self
    }
}

impl<Pool: SealedBufferPool> BorrowMut<Vec<u8>> for PooledBuffer<Pool> {
    #[inline]
    fn borrow_mut(&mut self) -> &mut Vec<u8> {
        self
    }
}

impl<Pool: SealedBufferPool> AsRef<Vec<u8>> for PooledBuffer<Pool> {
    #[inline]
    fn as_ref(&self) -> &Vec<u8> {
        self
    }
}

impl<Pool: SealedBufferPool> AsMut<Vec<u8>> for PooledBuffer<Pool> {
    #[inline]
    fn as_mut(&mut self) -> &mut Vec<u8> {
        self
    }
}

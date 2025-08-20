#![expect(unsafe_code, reason = "let unsafe code in Pools rely on PooledResource Drop impl")]

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::{
    pooled_resource::{PooledResource, SealedPool},
    other_utils::{ResetNothing, ResetResource},
};


/// A threadsafe resource pool with a growable number of `Resource`s.
#[derive(Debug)]
pub struct SharedUnboundedPool<Resource, Reset> {
    pool: Arc<Mutex<(
        // Pool contents
        Vec<Resource>,
        // The number of resources currently in-use
        usize,
    )>>,
    reset_resource: Reset,
}

impl<Resource, Reset> SharedUnboundedPool<Resource, Reset> {
    /// Create a new `SharedUnboundedPool`, which initially has zero `Resource`s.
    ///
    /// Whenever a `Resource` is returned to the pool, `reset_resource` is run on it first.
    #[inline]
    #[must_use]
    pub fn new(reset_resource: Reset) -> Self
    where
        Reset: ResetResource<Resource> + Clone,
    {
        Self {
            pool: Arc::new(Mutex::new((Vec::new(), 0))),
            reset_resource,
        }
    }
}

impl<Resource> SharedUnboundedPool<Resource, ResetNothing> {
    /// Create a new `SharedUnboundedPool`, which initially has zero `Resource`s.
    ///
    /// When a `Resource` is returned to the pool, it is not reset in any way.
    #[inline]
    #[must_use]
    pub fn new_without_reset() -> Self {
        Self::new(ResetNothing)
    }
}

impl<Resource, Reset> SharedUnboundedPool<Resource, Reset> {
    /// Lock the contents and number of in-use resources of the pool.
    #[inline]
    fn lock(&self) -> MutexGuard<'_, (Vec<Resource>, usize)> {
        let lock_result: Result<_, PoisonError<_>> = self.pool.lock();
        #[expect(clippy::unwrap_used, reason = "Unwrapping Mutex poison")]
        lock_result.unwrap()
    }
}

impl<Resource, Reset> SharedUnboundedPool<Resource, Reset>
where
    Resource: Default,
    Reset:    ResetResource<Resource> + Clone,
{
    /// Get a `Resource` from the pool, returning a default `Resource` if none were already
    /// available in the pool.
    ///
    /// Note that `Resource`s are not cleared when they are returned to the pool, so it may
    /// be necessary to clear the `Resource` of previous data.
    #[inline]
    #[must_use]
    pub fn get_default(&self) -> PooledResource<Self, Resource> {
        self.get(Resource::default)
    }
}

impl<Resource, Reset: ResetResource<Resource> + Clone> SharedUnboundedPool<Resource, Reset> {
    /// Get a `Resource` from the pool.
    ///
    /// Note that `Resource`s are not cleared when they are returned to the pool, so it may
    /// be necessary to clear the `Resource` of previous data.
    ///
    /// # Potential Panics or Deadlocks
    /// `init_resource` must not call any method on `self` or a `Clone` or `MirroredClone`
    /// associated with `self`; otherwise, a panic or deadlock may occur.
    ///
    /// Ideally, an `init_resource` closure should not capture any [`SharedUnboundedPool`].
    #[must_use]
    pub fn get<F>(&self, init_resource: F) -> PooledResource<Self, Resource>
    where
        F: FnOnce() -> Resource,
    {
        let mut guard = self.lock();
        let resource = guard.0.pop().unwrap_or_else(init_resource);
        let pool = self.clone();
        guard.1 += 1;

        // SAFETY:
        // It's safe for the `PooledResource` to call `return_resource` however it
        // likes, actually, and thus safe in the restricted guaranteed scenario.
        unsafe { PooledResource::new(pool, resource, ()) }
    }

    /// Get the total number of `Resource`s in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
        let guard = self.lock();
        guard.0.len() + guard.1
    }

    /// Get the number of `Resource`s in the pool which are not currently being used.
    #[must_use]
    pub fn available_resources(&self) -> usize {
        self.lock().0.len()
    }

    /// Discard extra unused `Resource`s, keeping only the first `max_unused` unused `Resource`s.
    pub fn trim_unused(&self, max_unused: usize) {
        self.lock().0.truncate(max_unused);
    }
}

impl<Resource, Reset> SealedPool<Resource> for SharedUnboundedPool<Resource, Reset>
where
    Reset: ResetResource<Resource> + Clone,
{
    type ExtraData = ();

    /// Used by [`PooledResource`] to return a `Resource` to a pool.
    ///
    /// # Safety
    /// Must be called at most once in the `Drop` impl of a `PooledResource` constructed
    /// via `PooledResource::new`, where `*self` and `extra_data` must be the `pool` and
    /// `extra_data` values passed to `PooledResource::new`.
    unsafe fn return_resource(&self, mut resource: Resource, _extra_data: Self::ExtraData) {
        self.reset_resource.reset(&mut resource);
        self.lock().0.push(resource);
    }
}

impl<Resource, Reset> Default for SharedUnboundedPool<Resource, Reset>
where
    Reset: ResetResource<Resource> + Clone + Default,
{
    #[inline]
    fn default() -> Self {
        Self::new(Reset::default())
    }
}

impl<Resource, ResetResource: Clone> Clone for SharedUnboundedPool<Resource, ResetResource> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            pool:           Arc::clone(&self.pool),
            reset_resource: self.reset_resource.clone()
        }
    }

    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.pool.clone_from(&source.pool);
        self.reset_resource.clone_from(&source.reset_resource);
    }
}

#[cfg(feature = "clone-behavior")]
impl<Resource, ResetResource, S> MirroredClone<S> for SharedUnboundedPool<Resource, ResetResource>
where
    ResetResource: MirroredClone<S>,
    S:             Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            pool:           Arc::clone(&self.pool),
            reset_resource: self.reset_resource.mirrored_clone()
        }
    }
}

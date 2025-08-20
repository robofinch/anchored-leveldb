#![expect(unsafe_code, reason = "let unsafe code in Pools rely on PooledResource Drop impl")]

use std::iter;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::{
    other_utils::{ResetNothing, ResetResource, ResourcePoolEmpty},
    pooled_resource::{PooledResource, SealedPool},
};


/// A threadsafe resource pool with a fixed number of `Resource`s.
#[derive(Debug)]
pub struct SharedBoundedPool<Resource, Reset> {
    pool: Arc<(
        // Pool contents
        Mutex<Vec<Option<Resource>>>,
        // Notified when a `Resource` is returned.
        Condvar,
    )>,
    reset_resource: Reset,
}

impl<Resource, Reset> SharedBoundedPool<Resource, Reset> {
    /// Create a new `SharedBoundedPool` which has the indicated, fixed number of `Resource`s.
    ///
    /// Each `Resource` is immediately initialized, using the provided function.
    ///
    /// Whenever a `Resource` is returned to the pool, `reset_resource` is run on it first.
    #[inline]
    #[must_use]
    pub fn new<F>(pool_size: usize, mut init_resource: F, reset_resource: Reset) -> Self
    where
        F: FnMut() -> Resource,
        Reset: ResetResource<Resource> + Clone,
    {
        let mut pool_contents = Vec::new();
        pool_contents.reserve_exact(pool_size);
        pool_contents.extend(
            iter::repeat_with(|| Some(init_resource())).take(pool_size)
        );
        Self {
            pool: Arc::new((
                Mutex::new(pool_contents),
                Condvar::new(),
            )),
            reset_resource,
        }
    }
}

impl<Resource: Default, Reset> SharedBoundedPool<Resource, Reset> {
    /// Create a new `SharedBoundedPool` which has the indicated, fixed number of `Resource`s.
    ///
    /// Each `Resource` is immediately initialized to its default value.
    ///
    /// Whenever a `Resource` is returned to the pool, `reset_resource` is run on it first.
    #[inline]
    #[must_use]
    pub fn new_default(pool_size: usize, reset_resource: Reset) -> Self
    where
        Reset: ResetResource<Resource> + Clone,
    {
        Self::new(pool_size, Resource::default, reset_resource)
    }
}

impl<Resource> SharedBoundedPool<Resource, ResetNothing> {
    /// Create a new `SharedBoundedPool` which has the indicated, fixed number of `Resource`s.
    ///
    /// Each `Resource` is immediately initialized, using the provided function.
    ///
    /// When a `Resource` is returned to the pool, it is not reset in any way.
    #[inline]
    #[must_use]
    pub fn new_without_reset<F>(pool_size: usize, init_resource: F) -> Self
    where
        F: FnMut() -> Resource,
    {
        Self::new(pool_size, init_resource, ResetNothing)
    }
}

impl<Resource: Default> SharedBoundedPool<Resource, ResetNothing> {
    /// Create a new `SharedBoundedPool` which has the indicated, fixed number of `Resource`s.
    ///
    /// Each `Resource` is immediately initialized to its default value.
    ///
    /// When a `Resource` is returned to the pool, it is not reset in any way.
    #[inline]
    #[must_use]
    pub fn new_default_without_reset(pool_size: usize) -> Self {
        Self::new(pool_size, Resource::default, ResetNothing)
    }
}

impl<Resource, Reset: ResetResource<Resource> + Clone> SharedBoundedPool<Resource, Reset> {
    /// Lock the contents of the pool.
    #[inline]
    fn pool_contents(&self) -> MutexGuard<'_, Vec<Option<Resource>>> {
        let lock_result: Result<_, PoisonError<_>> = self.pool.0.lock();
        #[expect(clippy::unwrap_used, reason = "Unwrapping Mutex poison")]
        lock_result.unwrap()
    }

    /// Try to get a resource from the pool.
    #[must_use]
    pub fn inner_try_get(
        &self,
        pool_contents: &mut MutexGuard<'_, Vec<Option<Resource>>>,
    ) -> Option<PooledResource<Self, Resource>> {
        pool_contents.iter_mut()
            .enumerate()
            .find_map(|(slot_idx, slot)| {
                slot.take().map(|resource| {
                    let pool = self.clone();
                    // SAFETY:
                    // It's safe for the `PooledResource` to call `return_resource` however it
                    // likes, actually, and thus safe in the restricted guaranteed scenario.
                    unsafe { PooledResource::new(pool, resource, slot_idx) }
                })
            })
    }

    /// Fallback for [`Self::get`] in the event that a resource is not immediately available.
    #[inline(never)]
    #[must_use]
    fn get_fallback(
        &self,
        mut pool_contents: MutexGuard<'_, Vec<Option<Resource>>>,
    ) -> PooledResource<Self, Resource> {
        loop {
            let poison_result: Result<_, PoisonError<_>> = self.pool.1.wait(pool_contents);

            #[expect(clippy::unwrap_used, reason = "only unwrapping Mutex poison")]
            {
                pool_contents = poison_result.unwrap();
            };

            if let Some(resource) = self.inner_try_get(&mut pool_contents) {
                return resource;
            }
        }
    }
}

impl<Resource, Reset: ResetResource<Resource> + Clone> SharedBoundedPool<Resource, Reset> {
    /// Get a `Resource` from the pool, if any are available.
    ///
    /// Note that `Resource`s are not cleared when they are returned to the pool, so it may
    /// be necessary to clear the `Resource` of previous data.
    pub fn try_get(&self) -> Result<PooledResource<Self, Resource>, ResourcePoolEmpty> {
        self.inner_try_get(&mut self.pool_contents()).ok_or(ResourcePoolEmpty)
    }

    /// Get a `Resource` from the pool.
    ///
    /// Note that `Resource`s are not cleared when they are returned to the pool, so it may
    /// be necessary to clear the `Resource` of previous data.
    ///
    /// May need to wait for a resource to become available.
    ///
    /// # Potential Panics or Deadlocks
    /// If `self.pool_size() == 0`, then this method panics.
    /// This method may also cause a deadlock if no `Resource`s are currently available, and the
    /// current thread needs to make progress in order to release a `Resource`.
    #[expect(clippy::missing_panics_doc, reason = "false positive")]
    #[must_use]
    pub fn get(&self) -> PooledResource<Self, Resource> {
        let mut pool_contents = self.pool_contents();

        if let Some(resource) = self.inner_try_get(&mut pool_contents) {
            return resource;
        }

        assert_ne!(
            pool_contents.len(), 0,
            "A SharedBoundedPool with a size of zero `Resource`s had `get` called on it, which can \
             never succeed",
        );

        self.get_fallback(pool_contents)
    }

    /// Get the total number of `Resource`s in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
        self.pool_contents().len()
    }

    /// Get the number of `Resource`s in the pool which are not currently being used.
    #[must_use]
    pub fn available_resources(&self) -> usize {
        self.pool_contents()
            .iter()
            .map(|slot| {
                if slot.is_some() {
                    // The resource is available to be taken
                    1_usize
                } else {
                    // The resource is in use
                    0_usize
                }
            })
            .sum()
    }
}

impl<Resource, Reset> SealedPool<Resource> for SharedBoundedPool<Resource, Reset>
where
    Reset: ResetResource<Resource> + Clone,
{
    type ExtraData = usize;

    /// Used by [`PooledResource`] to return a `Resource` to a pool.
    ///
    /// # Safety
    /// Must be called at most once in the `Drop` impl of a `PooledResource` constructed
    /// via `PooledResource::new`, where `*self` and `extra_data` must be the `pool` and
    /// `extra_data` values passed to `PooledResource::new`.
    unsafe fn return_resource(&self, mut resource: Resource, extra_data: Self::ExtraData) {
        self.reset_resource.reset(&mut resource);

        let slot_idx: usize = extra_data;

        let mut pool_contents = self.pool_contents();

        #[expect(
            clippy::indexing_slicing,
            reason = "the pool Vec's length is never changed after construction, and `slot_idx` \
                      was a valid index into the Vec when the `PooledResource` was made",
        )]
        {
            // Correctness:
            // `slot_contents` is necessarily `None` right now, we called `Option::take` on the slot
            // at index `slot_idx` to get a resource. We're putting a resource back into a slot
            // which was in-use (namely, by the `PooledResource` calling this method).
            pool_contents[slot_idx] = Some(resource);
        };

        // Notify the condvar that we returned a `Resource`
        self.pool.1.notify_one();
    }
}

impl<Resource, ResetResource: Clone> Clone for SharedBoundedPool<Resource, ResetResource> {
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
impl<Resource, ResetResource, S> MirroredClone<S> for SharedBoundedPool<Resource, ResetResource>
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

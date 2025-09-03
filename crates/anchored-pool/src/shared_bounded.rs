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
            iter::repeat_with(|| Some(init_resource())).take(pool_size),
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
    pub fn try_get(&self) -> Result<PooledResource<Self, Resource>, ResourcePoolEmpty> {
        self.inner_try_get(&mut self.pool_contents()).ok_or(ResourcePoolEmpty)
    }

    /// Get a `Resource` from the pool.
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


#[cfg(all(test, not(tests_with_leaks)))]
mod tests {
    use std::{array, sync::mpsc, thread};
    use super::*;


    #[test]
    fn zero_capacity() {
        let pool: SharedBoundedPool<(), _> = SharedBoundedPool::new_default_without_reset(0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_resources(), 0);
        assert!(pool.try_get().is_err());
    }

    #[test]
    #[should_panic]
    fn zero_capacity_fail() {
        let pool: SharedBoundedPool<(), _> = SharedBoundedPool::new_default_without_reset(0);
        let unreachable = pool.get();
        let _: &() = &*unreachable;
    }

    #[test]
    fn one_capacity() {
        let pool: SharedBoundedPool<(), _> = SharedBoundedPool::new_default_without_reset(1);
        let unit = pool.get();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_resources(), 0);
        assert!(pool.try_get().is_err());
        drop(unit);
        assert_eq!(pool.available_resources(), 1);
    }

    #[test]
    fn init_and_reset() {
        const CAPACITY: usize = 10;

        let pool = SharedBoundedPool::new(CAPACITY, || 1_usize, |int: &mut usize| *int = 1);
        let integers: [_; CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, mut integer) in integers.into_iter().enumerate() {
            assert_eq!(*integer, 1);
            *integer = idx;
            assert_eq!(*integer, idx);
        }

        // They've been reset to 1
        let integers: [_; CAPACITY] = array::from_fn(|_| pool.get());
        for integer in integers {
            assert_eq!(*integer, 1);
        }
    }

    #[test]
    fn no_reset() {
        const CAPACITY: usize = 10;

        let pool = SharedBoundedPool::new(CAPACITY, || 1_usize, ResetNothing);
        let integers: [_; CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, mut integer) in integers.into_iter().enumerate() {
            assert_eq!(*integer, 1);
            *integer = idx;
            assert_eq!(*integer, idx);
        }

        // They haven't been reset.
        // NOTE: users should not rely on the order.
        let integers: [_; CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, integer) in integers.into_iter().enumerate() {
            assert_eq!(*integer, idx);
        }
    }

    /// This test has unspecified behavior that a user should not rely on.
    #[test]
    fn init_and_reset_disagreeing() {
        let pool = SharedBoundedPool::new(2, || 1, |int: &mut i32| *int = 2);
        let first_int = pool.get();
        assert_eq!(*first_int, 1);
        drop(first_int);
        let mut reset_first_int = pool.get();
        assert_eq!(*reset_first_int, 2);
        let second_int = pool.get();
        assert_eq!(*second_int, 1);
        *reset_first_int = 3;
        assert_eq!(*reset_first_int, 3);
        drop(reset_first_int);
        let re_reset_first_int = pool.get();
        assert_eq!(*re_reset_first_int, 2);
    }

    #[test]
    fn multithreaded_one_capacity() {
        let pool: SharedBoundedPool<i32, _> = SharedBoundedPool::new_default_without_reset(1);

        let cloned_pool = pool.clone();

        assert_eq!(pool.available_resources(), 1);

        let (signal_main, wait_for_thread) = mpsc::channel();
        let (signal_thread, wait_for_main) = mpsc::channel();

        thread::spawn(move || {
            let mut int = cloned_pool.get();
            signal_main.send(()).unwrap();
            wait_for_main.recv().unwrap();
            assert_eq!(*int, 0);
            *int = 1;
            drop(int);
            signal_main.send(()).unwrap();
        });

        wait_for_thread.recv().unwrap();
        assert_eq!(pool.available_resources(), 0);
        signal_thread.send(()).unwrap();
        wait_for_thread.recv().unwrap();
        assert_eq!(pool.available_resources(), 1);
        assert_eq!(*pool.get(), 1);
    }
}

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
    ///
    /// # Potential Panics or Deadlocks
    /// `reset_resource` must not call any method on the returned pool or any `Clone` or
    /// `MirroredClone` of it; otherwise, a panic or deadlock may occur.
    ///
    /// Ideally, an `reset_resource` closure should not capture any [`SharedUnboundedPool`].
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
    #[inline]
    #[must_use]
    pub fn get_default(&self) -> PooledResource<Self, Resource> {
        self.get(Resource::default)
    }
}

impl<Resource, Reset: ResetResource<Resource> + Clone> SharedUnboundedPool<Resource, Reset> {
    /// Get a `Resource` from the pool.
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
        let mut guard = self.lock();
        guard.1 -= 1;
        guard.0.push(resource);
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


#[cfg(all(test, not(tests_with_leaks)))]
mod tests {
    use std::{array, sync::mpsc, thread};
    use super::*;


    #[test]
    fn zero_or_one_size() {
        let pool: SharedUnboundedPool<(), ResetNothing> = SharedUnboundedPool::new_without_reset();
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_resources(), 0);

        let unit = pool.get_default();
        let _: &() = &unit;
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_resources(), 0);

        drop(unit);
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_resources(), 1);

        pool.trim_unused(0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_resources(), 0);
    }

    #[test]
    fn init_and_reset() {
        const SIZE: usize = 10;

        let pool = SharedUnboundedPool::new(|int: &mut usize| *int = 1);
        let integers: [_; SIZE] = array::from_fn(|_| pool.get(|| 1_usize));

        for (idx, mut integer) in integers.into_iter().enumerate() {
            assert_eq!(*integer, 1);
            *integer = idx;
            assert_eq!(*integer, idx);
        }

        // They've been reset to 1, and the new constructor is not used.
        let integers: [_; SIZE] = array::from_fn(|_| pool.get(|| 2_usize));
        for integer in integers {
            assert_eq!(*integer, 1);
        }
    }

    #[test]
    fn no_reset() {
        const SIZE: usize = 10;

        let pool = SharedUnboundedPool::new(ResetNothing);
        let integers: [_; SIZE] = array::from_fn(|_| pool.get(|| 1_usize));
        for (idx, mut integer) in integers.into_iter().enumerate() {
            assert_eq!(*integer, 1);
            *integer = idx;
            assert_eq!(*integer, idx);
        }

        // They haven't been reset, nor is the constructor used
        // NOTE: users should not rely on the order.
        let integers: [_; SIZE] = array::from_fn(|_| pool.get(|| 1_usize));
        // This one is new.
        assert_eq!(*pool.get(|| 11), 11);

        for (idx, integer) in integers.into_iter().rev().enumerate() {
            assert_eq!(*integer, idx);
        }
    }

    /// This test has unspecified behavior that a user should not rely on.
    #[test]
    fn init_and_reset_disagreeing() {
        let pool = SharedUnboundedPool::new(|int: &mut i32| *int = 2);
        let first_int = pool.get_default();
        assert_eq!(*first_int, 0);
        drop(first_int);
        let mut reset_first_int = pool.get_default();
        assert_eq!(*reset_first_int, 2);
        let second_int = pool.get_default();
        assert_eq!(*second_int, 0);
        *reset_first_int = 3;
        assert_eq!(*reset_first_int, 3);
        drop(reset_first_int);
        let re_reset_first_int = pool.get_default();
        assert_eq!(*re_reset_first_int, 2);
    }

    #[test]
    fn multithreaded_one_capacity() {
        let pool: SharedUnboundedPool<i32, _> = SharedUnboundedPool::new_without_reset();

        let cloned_pool = pool.clone();

        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_resources(), 0);

        let (signal_main, wait_for_thread) = mpsc::channel();
        let (signal_thread, wait_for_main) = mpsc::channel();

        thread::spawn(move || {
            let mut int = cloned_pool.get_default();
            signal_main.send(()).unwrap();
            wait_for_main.recv().unwrap();
            assert_eq!(*int, 0);
            *int = 1;
            drop(int);
            signal_main.send(()).unwrap();
        });

        wait_for_thread.recv().unwrap();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_resources(), 0);
        signal_thread.send(()).unwrap();
        wait_for_thread.recv().unwrap();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_resources(), 1);
        assert_eq!(*pool.get_default(), 1);
    }
}

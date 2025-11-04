#![expect(unsafe_code, reason = "let unsafe code in Pools rely on PooledResource Drop impl")]

#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::{
    channel::{bounded_channel, Receiver, Sender},
    other_utils::{ResetNothing, ResetResource, ResourcePoolEmpty},
    pooled_resource::{PooledResource, SealedPool},
};


/// A threadsafe resource pool with a fixed number of `Resource`s.
#[derive(Debug)]
pub struct SharedBoundedPool<Resource, Reset> {
    sender:         Sender<Resource>,
    receiver:       Receiver<Resource>,
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
        #![expect(clippy::missing_panics_doc, reason = "false positive")]

        let (sender, receiver) = bounded_channel(pool_size);
        for _ in 0..pool_size {
            #[expect(clippy::expect_used, reason = "works by inspection of the two channel impls")]
            sender.send(init_resource()).expect("channel is not yet closed");
        }

        Self { sender, receiver, reset_resource }
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
    /// Get a `Resource` from the pool, if any are available.
    pub fn try_get(&self) -> Result<PooledResource<Self, Resource>, ResourcePoolEmpty> {
        #![expect(clippy::missing_panics_doc, reason = "false positive")]
        #[expect(clippy::expect_used, reason = "works by inspection of the two channel impls")]
        let resource = self.receiver
            .try_recv()
            .expect("channel is not closed; `self` has both a sender and receiver");

        if let Some(resource) = resource {
            let returner = (self.sender.clone(), self.reset_resource.clone());
            // SAFETY:
            // It's safe for the `PooledResource` to call `return_resource` however it
            // likes, actually, and thus safe in the restricted guaranteed scenario.
            Ok(unsafe { PooledResource::new(returner, resource) })
        } else {
            Err(ResourcePoolEmpty)
        }
    }

    /// Get a `Resource` from the pool.
    ///
    /// May need to wait for a resource to become available.
    ///
    /// # Potential Panics or Deadlocks
    /// If `self.pool_size() == 0`, then this method will panic or deadlock.
    /// This method may also cause a deadlock if no `Resource`s are currently available, and the
    /// current thread needs to make progress in order to release a `Resource`.
    #[expect(clippy::missing_panics_doc, reason = "false positive")]
    #[must_use]
    pub fn get(&self) -> PooledResource<Self, Resource> {
        #[expect(clippy::expect_used, reason = "works by inspection of the two channel impls")]
        let resource = self.receiver
            .recv()
            .expect("channel is not closed; `self` has both a sender and receiver");
        let returner = (self.sender.clone(), self.reset_resource.clone());

        // SAFETY:
        // It's safe for the `PooledResource` to call `return_resource` however it
        // likes, actually, and thus safe in the restricted guaranteed scenario.
        unsafe { PooledResource::new(returner, resource) }
    }

    /// Get the total number of `Resource`s in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
        self.receiver.capacity()
    }

    /// Get the number of `Resource`s in the pool which are not currently being used.
    #[must_use]
    pub fn available_resources(&self) -> usize {
        self.receiver.len()
    }
}

impl<Resource, Reset> SealedPool<Resource> for SharedBoundedPool<Resource, Reset>
where
    Reset: ResetResource<Resource> + Clone,
{
    type Returner = (Sender<Resource>, Reset);

    /// Used by [`PooledResource`] to return a `Resource` to a pool.
    ///
    /// # Safety
    /// Must be called at most once in the `Drop` impl of a `PooledResource` constructed
    /// via `PooledResource::new`, where `*returner` must be the `returner` value passed to
    /// `PooledResource::new`.
    unsafe fn return_resource(returner: &Self::Returner, mut resource: Resource) {
        let (sender, reset_resource) = returner;

        reset_resource.reset(&mut resource);
        // If the pool already died, it's no issue to just drop the resource here.
        let _err = sender.send(resource);
    }
}

impl<Resource, ResetResource: Clone> Clone for SharedBoundedPool<Resource, ResetResource> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            sender:         self.sender.clone(),
            receiver:       self.receiver.clone(),
            reset_resource: self.reset_resource.clone(),
        }
    }

    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.sender.clone_from(&source.sender);
        self.receiver.clone_from(&source.receiver);
        self.reset_resource.clone_from(&source.reset_resource);
    }
}

// TODO: this is a lie. `MirroredClone<ConstantTime>` holds, but the function acquires a lock.
#[cfg(feature = "clone-behavior")]
impl<Resource, ResetResource, S> MirroredClone<S> for SharedBoundedPool<Resource, ResetResource>
where
    ResetResource: MirroredClone<S>,
    S:             Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            sender:         self.sender.clone(),
            receiver:       self.receiver.clone(),
            reset_resource: self.reset_resource.mirrored_clone(),
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
        let integers: [_; CAPACITY] = array::from_fn(|_| pool.get());
        for integer in integers {
            assert!((0..CAPACITY).contains(&*integer));
        }
    }

    #[test]
    fn init_and_reset_disagreeing() {
        let pool = SharedBoundedPool::new(2, || 1, |int: &mut i32| *int = 2);
        let first_int = pool.get();
        let second_int = pool.get();
        assert_eq!(*first_int, 1);
        assert_eq!(*second_int, 1);
        drop(first_int);
        let mut reset_first_int = pool.get();
        assert_eq!(*reset_first_int, 2);
        *reset_first_int = 3;
        assert_eq!(*reset_first_int, 3);
        drop(reset_first_int);
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

#![expect(unsafe_code, reason = "let unsafe code in Pools rely on PooledResource Drop impl")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::{
    channel::{Receiver, Sender, unbounded_channel},
    pooled_resource::{PooledResource, SealedPool},
    other_utils::{ResetNothing, ResetResource},
};


/// A threadsafe resource pool with a growable number of `Resource`s.
#[derive(Debug)]
pub struct SharedUnboundedPool<Resource, Reset> {
    pool_size:      Arc<AtomicUsize>,
    sender:         Sender<Resource>,
    receiver:       Receiver<Resource>,
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
        let (sender, receiver) = unbounded_channel();
        Self {
            pool_size: Arc::new(AtomicUsize::new(0)),
            sender,
            receiver,
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
        #![expect(clippy::missing_panics_doc, reason = "false positive")]
        #[expect(clippy::expect_used, reason = "works by inspection of the two channel impls")]
        let resource = self.receiver
            .try_recv()
            .expect("channel is not closed; `self` has both a sender and receiver");

        let resource = resource.unwrap_or_else(|| {
            self.pool_size.fetch_add(1, Ordering::Relaxed);
            init_resource()
        });
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
        self.pool_size.load(Ordering::Relaxed)
    }

    /// Get the number of `Resource`s in the pool which are not currently being used.
    #[must_use]
    pub fn available_resources(&self) -> usize {
        self.receiver.len()
    }
}

impl<Resource, Reset> SealedPool<Resource> for SharedUnboundedPool<Resource, Reset>
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
        // eprintln!("{:?}", _err);
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
            pool_size:      Arc::clone(&self.pool_size),
            sender:         self.sender.clone(),
            receiver:       self.receiver.clone(),
            reset_resource: self.reset_resource.clone(),
        }
    }

    #[inline]
    fn clone_from(&mut self, source: &Self) {
        self.pool_size.clone_from(&source.pool_size);
        self.sender.clone_from(&source.sender);
        self.receiver.clone_from(&source.receiver);
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
            pool_size:      Arc::clone(&self.pool_size),
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
        let integers: [_; SIZE] = array::from_fn(|_| pool.get(|| 1_usize));
        // This one is new.
        assert_eq!(*pool.get(|| 11), 11);

        for integer in integers {
            assert!((0..SIZE).contains(&*integer));
        }
    }

    #[test]
    fn init_and_reset_disagreeing() {
        let pool = SharedUnboundedPool::new(|int: &mut i32| *int = 2);
        let first_int = pool.get_default();
        assert_eq!(*first_int, 0);
        let second_int = pool.get_default();
        assert_eq!(*second_int, 0);
        drop(first_int);
        let mut reset_first_int = pool.get_default();
        assert_eq!(*reset_first_int, 2);
        *reset_first_int = 3;
        assert_eq!(*reset_first_int, 3);
        drop(reset_first_int);
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

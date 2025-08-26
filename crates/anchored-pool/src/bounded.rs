#![expect(
    unsafe_code,
    reason = "Use UnsafeCell instead of the needless overhead of RefCell;
              let unsafe code in Pools rely on PooledResource Drop impl",
)]

use std::iter;
use std::{cell::UnsafeCell, rc::Rc};

#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::{
    other_utils::{ResetNothing, ResetResource, ResourcePoolEmpty},
    pooled_resource::{PooledResource, SealedPool},
};


/// A resource pool with a fixed number of `Resource`s.
#[derive(Debug)]
pub struct BoundedPool<Resource, Reset> {
    /// Safety: the `UnsafeCell` is only accessed from `Self::try_get`, `Self::available_resources`,
    /// and `Self::return_resource`. None of them allow a reference to something inside the
    /// `UnsafeCell` to escape outside the function body, and they do not call each other,
    /// except possibly via the `reset_resource` callback, which is not run while a borrow to
    /// the `UnsafeCell` contents is active.
    pool:           Rc<[UnsafeCell<Option<Resource>>]>,
    reset_resource: Reset,
}

impl<Resource, Reset> BoundedPool<Resource, Reset> {
    /// Create a new `BoundedPool` which has the indicated, fixed number of `Resource`s.
    ///
    /// Each `Resource` is immediately initialized, using the provided function.
    ///
    /// Whenever a `Resource` is returned to the pool, `reset_resource` is run on it first.
    #[inline]
    #[must_use]
    pub fn new<F>(pool_size: usize, mut init_resource: F, reset_resource: Reset) -> Self
    where
        F:     FnMut() -> Resource,
        Reset: ResetResource<Resource> + Clone,
    {
        let mut pool = Vec::new();
        pool.reserve_exact(pool_size);
        pool.extend(
            iter::repeat_with(|| UnsafeCell::new(Some(init_resource()))).take(pool_size)
        );
        Self {
            pool: Rc::from(pool),
            reset_resource,
        }
    }
}

impl<Resource: Default, Reset> BoundedPool<Resource, Reset> {
    /// Create a new `BoundedPool` which has the indicated, fixed number of `Resource`s.
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

impl<Resource> BoundedPool<Resource, ResetNothing> {
    /// Create a new `BoundedPool` which has the indicated, fixed number of `Resource`s.
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

impl<Resource: Default> BoundedPool<Resource, ResetNothing> {
    /// Create a new `BoundedPool` which has the indicated, fixed number of `Resource`s.
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

impl<Resource, Reset: ResetResource<Resource> + Clone> BoundedPool<Resource, Reset> {
    /// Get a `Resource` from the pool, if any are available.
    pub fn try_get(&self) -> Result<PooledResource<Self, Resource>, ResourcePoolEmpty> {
        self.pool.iter()
            .enumerate()
            .find_map(|(slot_idx, slot)| {
                let slot: *mut Option<Resource> = slot.get();
                // SAFETY:
                // We only need to ensure that this access is unique in order for this to be sound.
                // See the note on `BoundedPool.pool`. This is one of only three functions that
                // access the `UnsafeCell` contents, and none allow a reference to escape, or call
                // each other, except possibly in the carefully-handled `reset_resource` callback.
                let slot: &mut Option<Resource> = unsafe { &mut *slot };

                slot.take().map(|resource| {
                    let pool = self.clone();
                    // SAFETY:
                    // It's safe for the `PooledResource` to call `return_resource` however it
                    // likes, actually, and thus safe in the restricted guaranteed scenario.
                    unsafe { PooledResource::new(pool, resource, slot_idx) }
                })
            })
            .ok_or(ResourcePoolEmpty)
    }

    /// Get a `Resource` from the pool.
    ///
    /// # Panics
    /// Panics if no resources are currently available. As `BoundedPool` is `!Send + !Sync`, no
    /// resource could ever become available while in the body of this function.
    #[must_use]
    pub fn get(&self) -> PooledResource<Self, Resource> {
        #[expect(
            clippy::expect_used,
            reason = "this call would never succeed if it fails once. Also, this is documented.",
        )]
        self.try_get().expect(
            "A single-threaded BoundedPool ran out of `Resource`s and had `get()` called on \
             it, which can never succeed",
        )
    }

    /// Get the total number of `Resource`s in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
        self.pool.len()
    }

    /// Get the number of `Resource`s in the pool which are not currently being used.
    #[must_use]
    pub fn available_resources(&self) -> usize {
        self.pool.iter()
            .map(|slot| {
                let slot: *const Option<Resource> = slot.get().cast_const();
                // SAFETY:
                // We can guarantee that this access is unique, implying that this is sound.
                // See the note on `BoundedPool.pool`. This is one of only three functions that
                // access the `UnsafeCell` contents, and none allow a reference to escape, or call
                // each other, except possibly in the carefully-handled `reset_resource` callback.
                let slot: &Option<Resource> = unsafe { & *slot };

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

impl<Resource, Reset> SealedPool<Resource> for BoundedPool<Resource, Reset>
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
        // Note that we must call this before getting `slot_contents`.
        self.reset_resource.reset(&mut resource);

        let slot_idx: usize = extra_data;

        #[expect(
            clippy::indexing_slicing,
            reason = "the pool slice's length is never changed after construction, and `slot_idx` \
                      was a valid index into the slice when the `PooledResource` was made",
        )]
        let slot_contents: *mut Option<Resource> = self.pool[slot_idx].get();

        // SAFETY:
        // We only need to ensure that this access is unique in order for this to be sound.
        // See the note on `BoundedPool.pool`. This is one of only three functions that access the
        // `UnsafeCell` contents, and none allow a reference to escape, or call each other,
        // except possibly in the carefully-handled `reset_resource` callback.
        let slot_contents: &mut Option<Resource> = unsafe { &mut *slot_contents };

        // Correctness:
        // `slot_contents` is necessarily `None` right now, we called `Option::take` on the slot
        // at index `slot_idx` to get a resource. We're putting a resource back into a slot
        // which was in-use (namely, by the `PooledResource` calling this method).
        *slot_contents = Some(resource);
    }
}

impl<Resource, ResetResource: Clone> Clone for BoundedPool<Resource, ResetResource> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            pool:           Rc::clone(&self.pool),
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
impl<Resource, ResetResource, S> MirroredClone<S> for BoundedPool<Resource, ResetResource>
where
    ResetResource: MirroredClone<S>,
    S:             Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self {
            pool:           Rc::clone(&self.pool),
            reset_resource: self.reset_resource.mirrored_clone()
        }
    }
}


#[cfg(test)]
mod tests {
    use std::array;
    use super::*;


    #[test]
    fn zero_capacity() {
        let pool: BoundedPool<(), ResetNothing> = BoundedPool::new_default_without_reset(0);
        assert_eq!(pool.pool_size(), 0);
        assert_eq!(pool.available_resources(), 0);
        assert!(pool.try_get().is_err());
    }

    #[test]
    #[should_panic]
    fn zero_capacity_fail() {
        let pool: BoundedPool<(), ResetNothing> = BoundedPool::new_default_without_reset(0);
        let unreachable = pool.get();
        let _: &() = &*unreachable;
    }

    #[test]
    fn one_capacity() {
        let pool: BoundedPool<(), ResetNothing> = BoundedPool::new_default_without_reset(1);
        let unit = pool.get();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_resources(), 0);
        assert!(pool.try_get().is_err());
        drop(unit);
        assert_eq!(pool.available_resources(), 1);
    }

    #[test]
    #[should_panic]
    fn one_capacity_fail() {
        let pool: BoundedPool<(), ResetNothing> = BoundedPool::new_default_without_reset(1);
        let _unit = pool.get();
        assert_eq!(pool.pool_size(), 1);
        assert_eq!(pool.available_resources(), 0);
        let _unreachable = pool.get();
    }

    #[test]
    fn init_and_reset() {
        const CAPACITY: usize = 10;

        let pool = BoundedPool::new(CAPACITY, || 1_usize, |int: &mut usize| *int = 1);
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

        let pool = BoundedPool::new(CAPACITY, || 1_usize, ResetNothing);
        let integers: [_; CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, mut integer) in integers.into_iter().enumerate() {
            assert_eq!(*integer, 1);
            *integer = idx;
            assert_eq!(*integer, idx);
        }

        // They haven't been reset. NOTE: users should not rely on the order.
        let integers: [_; CAPACITY] = array::from_fn(|_| pool.get());
        for (idx, integer) in integers.into_iter().enumerate() {
            assert_eq!(*integer, idx);
        }
    }

    /// This test has unspecified behavior that a user should not rely on.
    #[test]
    fn init_and_reset_disagreeing() {
        let pool = BoundedPool::new(2, || 1, |int: &mut i32| *int = 2);
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
}

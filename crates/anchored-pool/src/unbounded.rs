#![expect(
    unsafe_code,
    reason = "Use UnsafeCell instead of the needless overhead of RefCell;
              let unsafe code in Pools rely on PooledResource Drop impl",
)]

use std::{cell::UnsafeCell, rc::Rc};

#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::{
    pooled_resource::{PooledResource, SealedPool},
    other_utils::{ResetNothing, ResetResource},
};


/// A resource pool with a growable number of `Resource`s.
#[derive(Debug)]
pub struct UnboundedPool<Resource, Reset> {
    /// Safety: the `UnsafeCell` is only accessed from `Self::get`, `Self::len`,
    /// `Self::available_resources`, `Self::trim_unused`, and `Self::return_resource`. None of them
    /// allow a reference to something inside the `UnsafeCell` to escape outside the function body.
    /// The only potential for them to call each other is in the callback passed to `Self::get`,
    /// and the `reset_resource` callback. `Self::get` ensures that the contents of the
    /// `UnsafeCell` are not borrowed while the callback is run, and likewise for
    /// `Self::return_resource` with the `reset_resource` callback. Other than that, they do not
    /// call each other.
    pool: Rc<UnsafeCell<(
        // Pool contents
        Vec<Resource>,
        // The number of resources currently in-use
        usize,
    )>>,
    reset_resource: Reset,
}

impl<Resource, Reset> UnboundedPool<Resource, Reset> {
    /// Create a new `UnboundedPool`, which initially has zero `Resource`s.
    ///
    /// Whenever a `Resource` is returned to the pool, `reset_resource` is run on it first.
    #[inline]
    #[must_use]
    pub fn new(reset_resource: Reset) -> Self
    where
        Reset: ResetResource<Resource> + Clone,
    {
        Self {
            pool: Rc::new(UnsafeCell::new((Vec::new(), 0))),
            reset_resource,
        }
    }
}

impl<Resource> UnboundedPool<Resource, ResetNothing> {
    /// Create a new `UnboundedPool`, which initially has zero `Resource`s.
    ///
    /// When a `Resource` is returned to the pool, it is not reset in any way.
    #[inline]
    #[must_use]
    pub fn new_without_reset() -> Self {
        Self::new(ResetNothing)
    }
}

impl<Resource, Reset> UnboundedPool<Resource, Reset>
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

impl<Resource, Reset: ResetResource<Resource> + Clone> UnboundedPool<Resource, Reset> {
    /// Get a `Resource` from the pool.
    #[must_use]
    pub fn get<F>(&self, init_resource: F) -> PooledResource<Self, Resource>
    where
        F: FnOnce() -> Resource,
    {
        let raw_cell_contents: *mut (Vec<Resource>, usize) = self.pool.get();

        // SAFETY:
        // The only potential way for any borrow to the cell contents to overlap with
        // this access is via user-provided callbacks. We drop the resulting reference
        // before running the callback, so the access is unique.
        let mut cell_contents: &mut (Vec<Resource>, usize) = unsafe { &mut *raw_cell_contents };

        let resource = if let Some(resource) = cell_contents.0.pop() {
            resource
        } else {
            #[expect(
                dropping_references,
                reason = "ensure there is no active borrow while the callback is run",
            )]
            drop(cell_contents);
            let resource = init_resource();
            // SAFETY:
            // The only potential way for any borrow to the cell contents to overlap with
            // this access was via the `init_resource` callback. The access is unique for the
            // remainder of this function.
            cell_contents = unsafe { &mut *raw_cell_contents };
            resource
        };

        let pool = self.clone();
        cell_contents.1 += 1;

        // SAFETY:
        // It's safe for the `PooledResource` to call `return_resource` however it
        // likes, actually, and thus safe in the restricted guaranteed scenario.
        unsafe { PooledResource::new(pool, resource, ()) }
    }

    /// Get the total number of `Resource`s in this pool, whether available or in-use.
    #[inline]
    #[must_use]
    pub fn pool_size(&self) -> usize {
        let raw_cell_contents: *const (Vec<Resource>, usize) = self.pool.get().cast_const();

        // SAFETY:
        // We can ensure that this access is unique, implying that this is sound.
        // See the note on `UnboundedPool.0`. This is one of only five functions that access
        // the `UnsafeCell` contents, and none allow a reference to escape, and they do not call
        // each other while the `UnsafeCell` contents are borrowed.
        let cell_contents: &(Vec<Resource>, usize) = unsafe { &*raw_cell_contents };

        cell_contents.0.len() + cell_contents.1
    }

    /// Get the number of `Resource`s in the pool which are not currently being used.
    #[must_use]
    pub fn available_resources(&self) -> usize {
        let raw_cell_contents: *const (Vec<Resource>, usize) = self.pool.get().cast_const();

        // SAFETY:
        // We can ensure that this access is unique, implying that this is sound.
        // See the note on `UnboundedPool.0`. This is one of only five functions that access
        // the `UnsafeCell` contents, and none allow a reference to escape, and they do not call
        // each other while the `UnsafeCell` contents are borrowed.
        let cell_contents: &(Vec<Resource>, usize) = unsafe { &*raw_cell_contents };

        cell_contents.0.len()
    }

    /// Discard extra unused `Resource`s, keeping only the first `max_unused` unused `Resource`s.
    pub fn trim_unused(&self, max_unused: usize) {
        let raw_cell_contents: *mut (Vec<Resource>, usize) = self.pool.get();

        // SAFETY:
        // We only need to ensure that this access is unique for this to be sound.
        // See the note on `UnboundedPool.0`. This is one of only five functions that access
        // the `UnsafeCell` contents, and none allow a reference to escape, and they do not call
        // each other while the `UnsafeCell` contents are borrowed.
        let cell_contents: &mut (Vec<Resource>, usize) = unsafe { &mut *raw_cell_contents };

        cell_contents.0.truncate(max_unused);
    }
}

impl<Resource, Reset> SealedPool<Resource> for UnboundedPool<Resource, Reset>
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
        // We must run this before getting the `cell_contents` borrow.
        self.reset_resource.reset(&mut resource);

        let raw_cell_contents: *mut (Vec<Resource>, usize) = self.pool.get();

        // SAFETY:
        // We only need to ensure that this access is unique for this to be sound.
        // See the note on `UnboundedPool.0`. This is one of only five functions that access
        // the `UnsafeCell` contents, and none allow a reference to escape, and they do not call
        // each other while the `UnsafeCell` contents are borrowed.
        let cell_contents: &mut (Vec<Resource>, usize) = unsafe { &mut *raw_cell_contents };

        cell_contents.1 -= 1;
        cell_contents.0.push(resource);
    }
}

impl<Resource, Reset> Default for UnboundedPool<Resource, Reset>
where
    Reset: ResetResource<Resource> + Clone + Default,
{
    #[inline]
    fn default() -> Self {
        Self::new(Reset::default())
    }
}

impl<Resource, ResetResource: Clone> Clone for UnboundedPool<Resource, ResetResource> {
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
impl<Resource, ResetResource, S> MirroredClone<S> for UnboundedPool<Resource, ResetResource>
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

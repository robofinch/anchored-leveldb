#![expect(unsafe_code, reason = "Makes other unsafe code in this crate easier to reason about")]

use std::cell::UnsafeCell;
use std::fmt::{Debug, Formatter, Result as FmtResult};


/// A wrapper around `UnsafeCell` whose user must manually enforce mutual exclusion in the same way
/// as `Mutex<T>`.
#[repr(transparent)]
pub(crate) struct UnsafeMutexCell<T: ?Sized>(UnsafeCell<T>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<T> UnsafeMutexCell<T> {
    #[inline]
    #[must_use]
    pub const fn new(data: T) -> Self {
        Self(UnsafeCell::new(data))
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<T: ?Sized> UnsafeMutexCell<T> {
    /// # Safety
    /// The aliasing rules must be manually upheld: for the duration of lifetime `'_`,
    /// no reference or pointer derived from other calls to `self.get_mut()` are permitted to exist
    /// or be used, respectively.
    ///
    /// Note that this must hold true across *all* threads.
    #[expect(clippy::mut_from_ref, reason = "yes, this is intentional")]
    pub const unsafe fn get_mut(&self) -> &mut T {
        let inner: *mut T = self.0.get();
        // SAFETY: as noted by `UnsafeCel::get`, we need to uphold the aliasing rules.
        // We pass that entire burden to the caller.
        unsafe { &mut *inner }
    }
}

impl<T: ?Sized> Debug for UnsafeMutexCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Debug::fmt(&self.0, f)
    }
}

// SAFETY: Same as the implementation for `Mutex<T>`. Sound because the user of this type
// must manually uphold aliasing rules in the same way as `Mutex<T>`.
unsafe impl<T: ?Sized + Send> Send for UnsafeMutexCell<T> {}
// SAFETY: Same as the implementation for `Mutex<T>`. Sound because the user of this type
// must manually uphold aliasing rules in the same way as `Mutex<T>`.
unsafe impl<T: ?Sized + Send> Sync for UnsafeMutexCell<T> {}

#[repr(transparent)]
pub(crate) struct NotShared<T: ?Sized>(T);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<T> NotShared<T> {
    #[inline]
    #[must_use]
    pub const fn new(data: T) -> Self {
        Self(data)
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<T: ?Sized> NotShared<T> {
    #[inline]
    #[must_use]
    pub const fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

// SAFETY: Sharing a `NotShared` across multiple threads does not allow them to do anything with
// the `NotShared` or the inner `T`, since exclusive access over the `NotShared` is needed to
// access the inner data, and at most one thread can have that exclusive access at a time.
unsafe impl<T: ?Sized> Sync for NotShared<T> {}

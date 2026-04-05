#![expect(unsafe_code, reason = "Makes other unsafe code in this crate easier to reason about")]

use std::cell::UnsafeCell;
use std::fmt::{Debug, Formatter, Result as FmtResult};


/// A wrapper around `UnsafeCell` whose user must manually enforce aliasing XOR mutation in the
/// same way as `RwLock<()>`.
pub(crate) struct ExternallySynchronized<T: ?Sized>(UnsafeCell<T>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<T> ExternallySynchronized<T> {
    #[inline]
    #[must_use]
    pub const fn new(data: T) -> Self {
        Self(UnsafeCell::new(data))
    }

    #[inline]
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<T: ?Sized> ExternallySynchronized<T> {
    /// # Safety
    /// The aliasing rules must be manually upheld: for the duration of lifetime `'_`,
    /// no reference or pointer derived from `self.get_mut()` is permitted to exist or be used,
    /// respectively.
    ///
    /// Note that this must hold true across *all* threads.
    pub const unsafe fn get(&self) -> &T {
        let inner: *const T = self.0.get();
        // SAFETY: as noted by `UnsafeCel::get`, we need to uphold the aliasing rules.
        // We pass that entire burden to the caller.
        unsafe { &*inner }
    }

    /// # Safety
    /// The aliasing rules must be manually upheld: for the duration of lifetime `'_`,
    /// no reference or pointer derived from `self.get()` or other calls to `self.get_mut()`
    /// are permitted to exist or be used, respectively.
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

impl<T: ?Sized> Debug for ExternallySynchronized<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Debug::fmt(&self.0, f)
    }
}

// SAFETY: Same as the implementation for `RwLock<T>`. Sound because the user of this type
// must manually uphold aliasing rules in the same way as `RwLock<T>`.
unsafe impl<T: ?Sized + Send> Send for ExternallySynchronized<T> {}
// SAFETY: Same as the implementation for `RwLock<T>`. Sound because the user of this type
// must manually uphold aliasing rules in the same way as `RwLock<T>`.
unsafe impl<T: ?Sized + Send + Sync> Sync for ExternallySynchronized<T> {}

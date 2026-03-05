#![expect(unsafe_code, reason = "Makes other unsafe code in this crate easier to reason about")]

use std::cell::UnsafeCell;


/// Avoid an unconstrained lifetime.
///
/// # Safety
/// Same as the safety of `unsafe { &mut *this.get() }`.
#[expect(clippy::mut_from_ref, reason = "yes, this is intentional")]
#[inline]
#[must_use]
pub(crate) unsafe fn unsafe_cell_get_mut_unchecked<T: ?Sized>(this: &UnsafeCell<T>) -> &mut T {
    // SAFETY: Asserted by caller.
    unsafe { &mut *this.get() }
}

/// Avoid an unconstrained lifetime.
///
/// # Safety
/// Same as the safety of `unsafe { &*this.get() }`.
#[inline]
#[must_use]
pub(crate) unsafe fn unsafe_cell_get_ref_unchecked<T: ?Sized>(this: &UnsafeCell<T>) -> &T {
    // SAFETY: Asserted by caller.
    unsafe { &*this.get() }
}

#![expect(unsafe_code, reason = "Allow a non-`Sync` type to be unsafely externally synchronized")]

use core::{alloc::Layout, ptr::NonNull};
use core::fmt::{Formatter, Result as FmtResult};

use bumpalo::{AllocErr, Bump};

use super::node::LINK_ALIGN;


const MAX_BUMP_ALIGN: usize = 16;

const BUMP_ALIGN: usize = {
    // Ideally, we might depend on `node_align::<ENTRY_ALIGN>()`, but absent `const_generic_exprs`
    // or similar, the best we can do is `LINK_ALIGN`.
    if LINK_ALIGN <= MAX_BUMP_ALIGN {
        LINK_ALIGN
    } else {
        MAX_BUMP_ALIGN
    }
};

pub(super) struct ExternallySynchronizedBump(Bump<BUMP_ALIGN>);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl ExternallySynchronizedBump {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Bump::with_min_align_and_capacity(capacity))
    }

    /// Attempts to allocate space for an object with the given `Layout` or else returns an `Err`.
    ///
    /// The returned pointer points at uninitialized memory. That memory remains valid until either
    /// the `ExternallySynchronizedBump` is dropped or until [`self.reset()`] is called.
    ///
    /// # Errors
    ///
    /// Errors if reserving space matching `layout` fails.
    ///
    /// # Safety
    /// Methods on [`ExternallySynchronizedBump`] should not be called concurrently. That is,
    /// calls to this method must *not* race with other calls to methods of `self` (in
    /// particular, [`self.try_alloc_layout(_)`] and [`self.debug(_)`]).
    ///
    /// [`self.reset()`]: ExternallySynchronizedBump::reset
    /// [`self.try_alloc_layout(_)`]: ExternallySynchronizedBump::try_alloc_layout
    /// [`self.debug(_)`]: ExternallySynchronizedBump::debug
    #[expect(clippy::inline_always, reason = "mirroring Bump's usage of inline(always)")]
    #[inline(always)]
    pub unsafe fn try_alloc_layout(&self, layout: Layout) -> Result<NonNull<u8>, AllocErr> {
        self.0.try_alloc_layout(layout)
    }

    /// See [`Bump::reset`]. Note that this invalidates all previously returned allocations.
    pub fn reset(&mut self) {
        self.0.reset();
    }

    #[expect(dead_code, reason = "TODO: debug impl stuff")]
    /// # Safety
    /// Methods on [`ExternallySynchronizedBump`] should not be called concurrently. That is,
    /// calls to this method must *not* race with other calls to methods of `self` (in
    /// particular, [`self.try_alloc_layout(_)`] and [`self.debug(_)`]).
    ///
    /// [`self.try_alloc_layout(_)`]: ExternallySynchronizedBump::try_alloc_layout
    /// [`self.debug(_)`]: ExternallySynchronizedBump::debug
    pub unsafe fn debug(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("ExternallySynchronizedBump")
            .field(&self.0)
            .finish()
    }
}

// SAFETY: the only methods which can be called concurrently (based on their signatures) are marked
// `unsafe` and require, as safety preconditions, that they are not called concurrently.
// Therefore, the non-`Sync`ness of the sole field of this type is not a problem.
unsafe impl Sync for ExternallySynchronizedBump {}

#![expect(unsafe_code, reason = "make `RawCellMutexGuard` useful with minimal overhead")]

use core::{cell::Cell, marker::PhantomData};
use core::fmt::{Debug, Formatter, Result as FmtResult};

use crate::would_block_error::WouldBlockError;


#[derive(Debug)]
pub(super) struct RawCellMutex(
    /// # Safety invariant
    /// The stored value is `false` only if the mutex is not locked.
    Cell<bool>,
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl RawCellMutex {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(Cell::new(false))
    }

    #[inline]
    pub fn lock(&self) -> RawCellMutexGuard<'_> {
        assert!(
            !self.0.get(),
            "Attempted to lock an anchored-sync MaybeSyncMutex \
             on a thread which already holds the lock",
        );

        // Safety invariant: this is always fine (at worst makes the lock unusable,
        // and since we shouldn't have a logic error, that can't happen
        // unless the user forgets a guard)
        self.0.set(true);

        // We checked that the mutex is not locked (by asserting `!self.0.get()`)
        RawCellMutexGuard(PhantomData)
    }

    #[inline]
    pub fn lock_ignoring_poison(&self) -> RawCellMutexGuard<'_> {
        self.lock()
    }

    #[inline]
    pub fn try_lock(&self) -> Result<RawCellMutexGuard<'_>, WouldBlockError> {
        if self.0.get() {
            Err(WouldBlockError)
        } else {
            // Safety invariant: this is always fine.
            self.0.set(true);

            // We checked that the mutex is not locked (since, in this branch, `!self.0.get()`)
            Ok(RawCellMutexGuard(PhantomData))
        }
    }

    #[inline]
    pub fn try_lock_ignoring_poison(&self) -> Result<RawCellMutexGuard<'_>, WouldBlockError> {
       self.try_lock()
    }

    #[expect(clippy::unused_self, reason = "mirroring std::sync impl")]
    #[inline]
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        false
    }

    #[expect(clippy::unused_self, reason = "mirroring std::sync impl")]
    #[inline]
    pub const fn clear_poison(&self) {}

    /// # Safety
    /// The provided `RawMutexGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.lock()`, `self.lock_ignoring_poison()`,
    /// or a `try_` variant of those two functions.
    #[inline]
    pub unsafe fn unlock(&self, _guard: RawCellMutexGuard<'_>) {
        // Safety invariant: there should always be at most one guard for this mutex,
        // so since this method consumes that sole guard, it releases the sole lock of the mutex.
        // (Note that guards can only be constructed by this module, and do not implement `Clone`
        // or similar.)
        // Therefore, we can set the lock field to `false`, since the mutex is no longer locked.
        self.0.set(false);
    }
}

#[must_use = "guards should not be unintentionally dropped"]
pub(super) struct RawCellMutexGuard<'a>(PhantomData<(&'a (), *const ())>);

impl Debug for RawCellMutexGuard<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawCellMutexGuard").finish_non_exhaustive()
    }
}

#![expect(unsafe_code, reason = "make `RawMutexGuard` useful with minimal overhead")]

use core::marker::PhantomData;
use core::fmt::{Debug, Formatter, Result as FmtResult};

use parking_lot::RawMutex as ParkingLotRawMutex;
use parking_lot::lock_api::RawMutex as _;

use crate::would_block_error::WouldBlockError;


pub(super) struct RawMutex(ParkingLotRawMutex);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl RawMutex {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(ParkingLotRawMutex::INIT)
    }

    pub fn lock(&self) -> RawMutexGuard<'_> {
        self.0.lock();

        RawMutexGuard(PhantomData)
    }

    pub fn lock_ignoring_poison(&self) -> RawMutexGuard<'_> {
        self.lock()
    }

    pub fn try_lock(&self) -> Result<RawMutexGuard<'_>, WouldBlockError> {
        if self.0.try_lock() {
            Ok(RawMutexGuard(PhantomData))
        } else {
            Err(WouldBlockError)
        }
    }

    pub fn try_lock_ignoring_poison(&self) -> Result<RawMutexGuard<'_>, WouldBlockError> {
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
    /// The provided `RawMutexGuard` must be a guard of *this* `self` value which was obtained
    /// on the current thread.
    ///
    /// That is, it must have been obtained on this thread from `self.lock()`,
    /// `self.lock_ignoring_poison()`, or a `try_` variant of those two functions.
    pub unsafe fn unlock(&self, _guard: RawMutexGuard<'_>) {
        // SAFETY: as proven by the guard (which does not impl `Send`, `Clone`, etc, and can only
        // be constructed in this module), `self.0` was locked by this thread.
        unsafe { self.0.unlock(); }
    }
}

impl Debug for RawMutex {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawMutex").field(&"<parking_lot::RawMutex>").finish()
    }
}

#[must_use = "guards should not be unintentionally dropped"]
pub(super) struct RawMutexGuard<'a>(PhantomData<(&'a (), *const ())>);

impl Debug for RawMutexGuard<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawMutexGuard").field(&"<lock token>").finish()
    }
}

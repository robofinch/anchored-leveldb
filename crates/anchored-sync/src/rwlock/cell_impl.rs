#![expect(unsafe_code, reason = "make `RawCell{Read,Write}Guard` useful with minimal overhead")]

use core::{cell::Cell, marker::PhantomData};
use core::fmt::{Debug, Formatter, Result as FmtResult};

use crate::would_block_error::WouldBlockError;


#[derive(Debug)]
pub(super) struct RawCellRwLock(
    /// # Safety invariant
    ///
    /// The cell storing `n @ 0..usize::MAX` indicates that there are currently `n` readers
    /// (and zero writers).
    ///
    /// The cell storing [`usize::MAX`] indicates that there is currently one writer
    /// (and zero readers).
    Cell<usize>,
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl RawCellRwLock {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(Cell::new(0))
    }

    /// Checks that `state < usize::MAX - 1`.
    #[inline]
    #[must_use]
    const fn can_read_lock(state: usize) -> bool {
        state < usize::MAX - 1
    }

    /// Checks that `state == 0`.
    #[inline]
    #[must_use]
    const fn can_write_lock(state: usize) -> bool {
        state == 0
    }

    /// If this function successfully returns, then `state < usize::MAX - 1`.
    #[inline]
    fn assert_can_read_lock(state: usize) {
        // Assert that the state is neither `usize::MAX` (write-locked)
        // nor `usize::MAX - 1` (max number of readers)
        if !Self::can_read_lock(state) {
            // To be precise, the panics are documented by `MaybeSyncRwLock`.
            #[expect(clippy::panic, reason = "panic is documented, and necessary for assertion")]
            if state == usize::MAX {
                panic!(
                    "Attempted to acquire a read lock of an anchored-sync MaybeSyncRwLock \
                     on a thread which already holds a write lock of that rwlock",
                );
            } else {
                panic!(
                    "Attempted to acquire a read lock of an anchored-sync MaybeSyncRwLock \
                     which already has the maximum number of readers (`usize::MAX - 1`)",
                );
            }
        }
    }

    /// If this function successfully returns, then `state == 0`.
    #[inline]
    fn assert_can_write_lock(state: usize) {
        // Assert that the state is neither write-locked nor read-locked
        if !Self::can_write_lock(state) {
            // To be precise, the panics are documented by `MaybeSyncRwLock`.
            #[expect(clippy::panic, reason = "panic is documented, and necessary for assertion")]
            if state == usize::MAX {
                panic!(
                    "Attempted to acquire a write lock of an anchored-sync MaybeSyncRwLock \
                     on a thread which already holds a write lock of that rwlock",
                );
            } else {
                panic!(
                    "Attempted to acquire a write lock of an anchored-sync MaybeSyncRwLock \
                     on a thread which already holds a read lock of that rwlock",
                );
            }
        }
    }

    #[inline]
    pub fn read(&self) -> RawCellReadGuard<'_> {
        let state = self.0.get();
        Self::assert_can_read_lock(state);

        // SAFETY: if we get here, then `assert_can_read_lock` successfully returned,
        // so `state < usize::MAX - 1` and thus this increment cannot overflow `usize`.
        // Safety invariant: we checked that there is not an existing writer, and setting
        // the state to `num_readers + 1` will not spill into the `usize::MAX` write-locked
        // state (since `state < usize::MAX - 1`).
        self.0.set(unsafe { state.unchecked_add(1) });

        // We acquired a read lock.
        RawCellReadGuard(PhantomData)
    }

    #[inline]
    pub fn read_ignoring_poison(&self) -> RawCellReadGuard<'_> {
        self.read()
    }

    #[inline]
    pub fn try_read(&self) -> Result<RawCellReadGuard<'_>, WouldBlockError> {
        let state = self.0.get();
        if Self::can_read_lock(state) {
            // SAFETY: in this branch, `state < usize::MAX - 1` and thus this increment cannot
            // overflow `usize`.
            // Safety invariant: we checked that there is not an existing writer, and setting
            // the state to `num_readers + 1` will not spill into the `usize::MAX` write-locked
            // state (since `state < usize::MAX - 1`).
            self.0.set(unsafe { state.unchecked_add(1) });

            // We acquired a read lock.
            Ok(RawCellReadGuard(PhantomData))
        } else {
            Err(WouldBlockError)
        }
    }

    #[inline]
    pub fn try_read_ignoring_poison(&self) -> Result<RawCellReadGuard<'_>, WouldBlockError> {
       self.try_read()
    }

    #[inline]
    pub fn write(&self) -> RawCellWriteGuard<'_> {
        let state = self.0.get();
        Self::assert_can_write_lock(state);

        // Safety invariant: we checked that there is not an existing reader or writer.
        // We set `self.0` to `usize::MAX`, which is the write-locked state.
        self.0.set(usize::MAX);

        // We acquired the write lock.
        RawCellWriteGuard(PhantomData)
    }

    #[inline]
    pub fn write_ignoring_poison(&self) -> RawCellWriteGuard<'_> {
        self.write()
    }

    #[inline]
    pub fn try_write(&self) -> Result<RawCellWriteGuard<'_>, WouldBlockError> {
        let state = self.0.get();
        if Self::can_write_lock(state) {
            // Safety invariant: we checked that there is not an existing reader or writer.
            // We set `self.0` to `usize::MAX`, which is the write-locked state.
            self.0.set(usize::MAX);

            // We acquired the write lock.
            Ok(RawCellWriteGuard(PhantomData))
        } else {
            Err(WouldBlockError)
        }
    }

    #[inline]
    pub fn try_write_ignoring_poison(&self) -> Result<RawCellWriteGuard<'_>, WouldBlockError> {
       self.try_write()
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
    /// The provided `RawCellReadGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.read()`, `self.read_ignoring_poision()`,
    /// or a `try_` variant of those two functions.
    #[inline]
    pub unsafe fn unlock_reader(&self, _guard: RawCellReadGuard<'_>) {
        let state = self.0.get();
        // (Note that guards can only be constructed by this module, and do not implement `Clone`
        // or similar. This method takes an owned read guard.)
        // SAFETY: As proven by the existence of the guard, there was at least one reader,
        // which means that the cell currently stores `n @ 1..usize::MAX` to indicate that there
        // are `n` readers. Therefore, subtracting `1` does not underflow.
        // Safety invariant: There were `state`-many readers and 0 writers, and this function
        // removes one reader, leaving `state-1` readers and 0 writers, so this function leaves
        // `self.0` in the correct state.
        self.0.set(unsafe { state.unchecked_sub(1) });
    }

    /// # Safety
    /// The provided `RawCellWriteGuard` must be a guard of *this* `self` value.
    ///
    /// That is, it must have been obtained from `self.write()`, `self.write_ignoring_poison()`,
    /// or a `try_` variant of those two functions.
    #[inline]
    pub unsafe fn unlock_writer(&self, _guard: RawCellWriteGuard<'_>) {
        // (Note that guards can only be constructed by this module, and do not implement `Clone`
        // or similar. This method takes an owned write guard.)
        // Safety invariant: As proven by the existence of the guard, there was one writer and zero
        // readers, and this function removes that reader, leaving 0 readers and 0 writers, which
        // is encoded as state `0`.
        self.0.set(0);
    }
}

#[must_use = "guards should not be unintentionally dropped"]
pub(super) struct RawCellReadGuard<'a>(PhantomData<(&'a (), *const ())>);

impl Debug for RawCellReadGuard<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawCellReadGuard").finish_non_exhaustive()
    }
}

#[must_use = "guards should not be unintentionally dropped"]
pub(super) struct RawCellWriteGuard<'a>(PhantomData<(&'a (), *const ())>);

impl Debug for RawCellWriteGuard<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("RawCellWriteGuard").finish_non_exhaustive()
    }
}

#![expect(
    unsafe_code,
    reason = "needed to read union fields, drop ManuallyDrop fields, and impl Send + Sync",
)]

use core::mem::ManuallyDrop;

use super::{
    MaybeSyncRawReadGuard, MaybeSyncRawWriteGuard, MaybeSyncReadGuard, MaybeSyncRwLock,
    MaybeSyncWriteGuard, RawCellReadGuard, RawCellRwLock, RawCellWriteGuard, RawReadGuard,
    RawRwLock, RawWriteGuard,
};


// Rough table of contents:
// - `unsafe` helper functions for `MaybeSyncRwLock`
// - impls of `Drop`, `Send`, and `Sync` for `MaybeSyncRwLock`
// - impls of `Drop` and `Sync` for `MaybeSync{Read,Write}Guard`

impl<const SYNC: bool, T: ?Sized> MaybeSyncRwLock<SYNC, T> {
    /// # Safety
    /// `self.raw` must not be used again after calling this method (not even by moving it).
    #[inline]
    pub(super) unsafe fn drop_raw_rwlock(&mut self) {
        if SYNC {
            // SAFETY: if `SYNC`, then the `sync` field is initialized
            let sync = unsafe { &mut self.raw.sync };
            // SAFETY: we do not use `sync` (not even moving it) after calling
            // `ManuallyDrop::drop` on `sync`, since we do not use our reference to `self.raw`
            // or `self.raw.sync`, and the caller promises not to do so either.
            unsafe {
                ManuallyDrop::drop(sync);
            };
        } else {
            // SAFETY: if `SYNC`, then the `unsync` field is initialized
            let unsync = unsafe { &mut self.raw.unsync };
            // SAFETY: we do not use `unsync` (not even moving it) after calling
            // `ManuallyDrop::drop` on `unsync`, since we do not use our reference to `self.raw`
            // or `self.raw.unsync`, and the caller promises not to do so either.
            unsafe {
                ManuallyDrop::drop(unsync);
            };
        }
    }

    /// # Safety
    /// `sync_f` and `unsync_f` must, on the thread on which `read_fn` is called, obtain and
    /// return a raw read guard associated with the rwlock given to them as an argument.
    ///
    /// This ensures that the created guard can be unlocked on the current thread and precludes
    /// any pathological functions that return guards from some other rwlock.
    #[inline]
    pub(super) unsafe fn read_fn<'a, IfSync, IfUnsync>(
        &'a self,
        sync_f:   IfSync,
        unsync_f: IfUnsync,
    ) -> MaybeSyncReadGuard<'a, SYNC, T>
    where
        IfSync:   FnOnce(&'a RawRwLock) -> RawReadGuard<'a>,
        IfUnsync: FnOnce(&'a RawCellRwLock) -> RawCellReadGuard<'a>,
    {
        if SYNC {
            // SAFETY: `SYNC` is true, so the `sync` field is initialized.
            let sync = unsafe { &self.raw.sync };

            let raw_guard = sync_f(sync);

            MaybeSyncReadGuard {
                rwlock: self,
                raw:    MaybeSyncRawReadGuard {
                    // Safety invariant: `SYNC` is true, and we initialize the `sync` field
                    // to a guard associated with `guard.rwlock.raw.sync` obtained on the current
                    // thread.
                    sync: ManuallyDrop::new(raw_guard),
                },
            }
        } else {
            // SAFETY: `SYNC` is false, so the `unsync` field is initialized.
            let unsync = unsafe { &self.raw.unsync };

            let raw_guard = unsync_f(unsync);

            MaybeSyncReadGuard {
                rwlock: self,
                raw:    MaybeSyncRawReadGuard {
                    // Safety invariant: `SYNC` is false, and we initialize the `unsync` field
                    // to a guard associated with `guard.rwlock.raw.unsync`.
                    unsync: ManuallyDrop::new(raw_guard),
                },
            }
        }
    }

    /// # Safety
    /// If `sync_f` or `unsync_f` returns `Ok(_)`, then it must, on the thread on which
    /// `try_read_fn` is called, obtain and return a raw read guard associated with the rwlock
    /// given to it as an argument.
    ///
    /// This ensures that the created guard can be unlocked on the current thread and precludes
    /// any pathological functions that return guards from some other rwlock.
    #[inline]
    pub(super) unsafe fn try_read_fn<'a, IfSync, IfUnsync, E>(
        &'a self,
        sync_f:   IfSync,
        unsync_f: IfUnsync,
    ) -> Result<MaybeSyncReadGuard<'a, SYNC, T>, E>
    where
        IfSync:   FnOnce(&'a RawRwLock) -> Result<RawReadGuard<'a>, E>,
        IfUnsync: FnOnce(&'a RawCellRwLock) -> Result<RawCellReadGuard<'a>, E>,
    {
        if SYNC {
            // SAFETY: `SYNC` is true, so the `sync` field is initialized.
            let sync = unsafe { &self.raw.sync };

            let raw_guard = sync_f(sync)?;

            Ok(MaybeSyncReadGuard {
                rwlock: self,
                raw:    MaybeSyncRawReadGuard {
                    // Safety invariant: `SYNC` is true, and we initialize the `sync` field
                    // to a guard associated with `guard.rwlock.raw.sync` obtained on the current
                    // thread.
                    sync: ManuallyDrop::new(raw_guard),
                },
            })
        } else {
            // SAFETY: `SYNC` is false, so the `unsync` field is initialized.
            let unsync = unsafe { &self.raw.unsync };

            let raw_guard = unsync_f(unsync)?;

            Ok(MaybeSyncReadGuard {
                rwlock: self,
                raw:    MaybeSyncRawReadGuard {
                    // Safety invariant: `SYNC` is false, and we initialize the `unsync` field
                    // to a guard associated with `guard.rwlock.raw.unsync`.
                    unsync: ManuallyDrop::new(raw_guard),
                },
            })
        }
    }

    /// # Safety
    /// `sync_f` and `unsync_f` must, on the thread on which `write_fn` is called, obtain and
    /// return a raw write guard associated with the rwlock given to them as an argument.
    ///
    /// This ensures that the created guard can be unlocked on the current thread and precludes
    /// any pathological functions that return guards from some other rwlock.
    #[inline]
    pub(super) unsafe fn write_fn<'a, IfSync, IfUnsync>(
        &'a self,
        sync_f:   IfSync,
        unsync_f: IfUnsync,
    ) -> MaybeSyncWriteGuard<'a, SYNC, T>
    where
        IfSync:   FnOnce(&'a RawRwLock) -> RawWriteGuard<'a>,
        IfUnsync: FnOnce(&'a RawCellRwLock) -> RawCellWriteGuard<'a>,
    {
        if SYNC {
            // SAFETY: `SYNC` is true, so the `sync` field is initialized.
            let sync = unsafe { &self.raw.sync };

            let raw_guard = sync_f(sync);

            MaybeSyncWriteGuard {
                rwlock: self,
                raw:    MaybeSyncRawWriteGuard {
                    // Safety invariant: `SYNC` is true, and we initialize the `sync` field
                    // to a guard associated with `guard.rwlock.raw.sync` obtained on the current
                    // thread.
                    sync: ManuallyDrop::new(raw_guard),
                },
            }
        } else {
            // SAFETY: `SYNC` is false, so the `unsync` field is initialized.
            let unsync = unsafe { &self.raw.unsync };

            let raw_guard = unsync_f(unsync);

            MaybeSyncWriteGuard {
                rwlock: self,
                raw:    MaybeSyncRawWriteGuard {
                    // Safety invariant: `SYNC` is false, and we initialize the `unsync` field
                    // to a guard associated with `guard.rwlock.raw.unsync`.
                    unsync: ManuallyDrop::new(raw_guard),
                },
            }
        }
    }

    /// # Safety
    /// If `sync_f` or `unsync_f` returns `Ok(_)`, then it must, on the thread on which
    /// `try_write_fn` is called, obtain and return a raw write guard associated with the rwlock
    /// given to it as an argument.
    ///
    /// This ensures that the created guard can be unlocked on the current thread and precludes
    /// any pathological functions that return guards from some other rwlock.
    #[inline]
    pub(super) unsafe fn try_write_fn<'a, IfSync, IfUnsync, E>(
        &'a self,
        sync_f:   IfSync,
        unsync_f: IfUnsync,
    ) -> Result<MaybeSyncWriteGuard<'a, SYNC, T>, E>
    where
        IfSync:   FnOnce(&'a RawRwLock) -> Result<RawWriteGuard<'a>, E>,
        IfUnsync: FnOnce(&'a RawCellRwLock) -> Result<RawCellWriteGuard<'a>, E>,
    {
        if SYNC {
            // SAFETY: `SYNC` is true, so the `sync` field is initialized.
            let sync = unsafe { &self.raw.sync };

            let raw_guard = sync_f(sync)?;

            Ok(MaybeSyncWriteGuard {
                rwlock: self,
                raw:    MaybeSyncRawWriteGuard {
                    // Safety invariant: `SYNC` is true, and we initialize the `sync` field
                    // to a guard associated with `guard.rwlock.raw.sync` obtained on the current
                    // thread.
                    sync: ManuallyDrop::new(raw_guard),
                },
            })
        } else {
            // SAFETY: `SYNC` is false, so the `unsync` field is initialized.
            let unsync = unsafe { &self.raw.unsync };

            let raw_guard = unsync_f(unsync)?;

            Ok(MaybeSyncWriteGuard {
                rwlock: self,
                raw:    MaybeSyncRawWriteGuard {
                    // Safety invariant: `SYNC` is false, and we initialize the `unsync` field
                    // to a guard associated with `guard.rwlock.raw.unsync`.
                    unsync: ManuallyDrop::new(raw_guard),
                },
            })
        }
    }
}

impl<const SYNC: bool, T: ?Sized> Drop for MaybeSyncRwLock<SYNC, T> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: we do not use `self.raw` after calling this function, since this is the last
        // action taken in the destructor of `self`.
        unsafe { self.drop_raw_rwlock() }
    }
}

// SAFETY: Same as the impl for `RwLock<T>`. It needs only `T: Send` since the `RwLock` wraps
// the `T` inline (it does not refcount the `T` or something, so sending ownership over the
// entire lock also sends ownership over the `T`). Additionally, we know that for `SYNC = true`,
// the raw rwlock impl is threadsafe.
unsafe impl<T: ?Sized + Send> Send for MaybeSyncRwLock<true, T> {}
// SAFETY: Same as the impl for `RwLock<T>`. It needs `T: Send` since getting a `&mut T` from
// `write` allows a `T` to be moved out (possibly into a different thread). A `Sync` bound is also
// needed since multiple `&T` references can be obtained from `read` called on multiple threads
// concurrently (if the `MaybeSyncRwLock` implements `Sync`). Additionally, we know that for
// `SYNC = true`, the raw rwlock impl is threadsafe.
unsafe impl<T: ?Sized + Send + Sync> Sync for MaybeSyncRwLock<true, T> {}

impl<const SYNC: bool, T: ?Sized> Drop for MaybeSyncReadGuard<'_, SYNC, T> {
    #[inline]
    fn drop(&mut self) {
        // Note: the safety invariants of `self.raw` and `self.rwlock.raw` permit this destructor
        // function to soundly access them.
        if SYNC {
            // SAFETY: if `SYNC`, then the `sync` field is initialized
            let sync = unsafe { &mut self.raw.sync };
            // SAFETY: we do not use `sync` (not even moving it) after calling
            // `ManuallyDrop::take` on `sync`; we know this since we are in the destructor,
            // so even if `rwlock_sync.unlock` were to run arbitrary code, that code would not
            // be able to access the `raw.sync` field owned by the
            // no-longer-soundly-reachable-from-other-code `self` value.
            let guard = unsafe { ManuallyDrop::take(sync) };

            // SAFETY: if `SYNC`, then the `sync` field is initialized
            let rwlock_sync = unsafe { &self.rwlock.raw.sync };

            // SAFETY: since `MaybeSyncReadGuard` does not implement `Send`, regardless of
            // `SYNC` (since it has a union field of type `RawCellReadGuard`, which does not
            // implement `Send`), we know we're on the same thread from which the guard was
            // obtained. Additionally, the guard was *not* obtained from a different rwlock; we keep
            // the rwlock reference and guard together, and do not swap them out.
            unsafe {
                rwlock_sync.unlock_reader(guard);
            }
        } else {
            // SAFETY: if `!SYNC`, then the `unsync` field is initialized
            let unsync = unsafe { &mut self.raw.unsync };
            // SAFETY: we do not use `unsync` (not even moving it) after calling
            // `ManuallyDrop::take` on `unsync`; we know this since we are in the destructor,
            // so even if `rwlock_unsync.unlock` were to run arbitrary code, that code would not
            // be able to access the `raw.unsync` field owned by the
            // no-longer-soundly-reachable-from-other-code `self` value.
            let guard = unsafe { ManuallyDrop::take(unsync) };

            // SAFETY: if `!SYNC`, then the `unsync` field is initialized
            let rwlock_unsync = unsafe { &self.rwlock.raw.unsync };

            // SAFETY: The guard was *not* obtained from a different rwlock; we keep
            // the rwlock reference and guard together, and do not swap them out.
            unsafe {
                rwlock_unsync.unlock_reader(guard);
            }
        }
    }
}

// SAFETY: Same as the impl for `std::sync::RwLockReadGuard<T>`. Sharing a read guard across
// threads (behind a `&`) is akin to sharing a `&&T` between threads, which is permitted
// iff `T: Sync`. The relevant `&T`, in this case, comes from `guard.rwlock.data`.
// The `guard` also has `guard.raw` and `guard.rwlock.raw`. As per the safety invariants of
// the `guard` fields, we only access those fields in the destructor, so we do not allow concurrent
// access to them from multiple threads even if the guard implements `Sync`. Therefore, the fact
// that those two fields' types might not be `Sync` does not matter.
unsafe impl<const SYNC: bool, T: ?Sized + Sync> Sync for MaybeSyncReadGuard<'_, SYNC, T> {}

impl<const SYNC: bool, T: ?Sized> Drop for MaybeSyncWriteGuard<'_, SYNC, T> {
    #[inline]
    fn drop(&mut self) {
        // Note: the safety invariants of `self.raw` and `self.rwlock.raw` permit this destructor
        // function to soundly access them.
        if SYNC {
            // SAFETY: if `SYNC`, then the `sync` field is initialized
            let sync = unsafe { &mut self.raw.sync };
            // SAFETY: we do not use `sync` (not even moving it) after calling
            // `ManuallyDrop::take` on `sync`; we know this since we are in the destructor,
            // so even if `rwlock_sync.unlock` were to run arbitrary code, that code would not
            // be able to access the `raw.sync` field owned by the
            // no-longer-soundly-reachable-from-other-code `self` value.
            let guard = unsafe { ManuallyDrop::take(sync) };

            // SAFETY: if `SYNC`, then the `sync` field is initialized
            let rwlock_sync = unsafe { &self.rwlock.raw.sync };

            // SAFETY: since `MaybeSyncReadGuard` does not implement `Send`, regardless of
            // `SYNC` (since it has a union field of type `RawCellReadGuard`, which does not
            // implement `Send`), we know we're on the same thread from which the guard was
            // obtained. Additionally, the guard was *not* obtained from a different rwlock; we keep
            // the rwlock reference and guard together, and do not swap them out.
            unsafe {
                rwlock_sync.unlock_writer(guard);
            }
        } else {
            // SAFETY: if `!SYNC`, then the `unsync` field is initialized
            let unsync = unsafe { &mut self.raw.unsync };
            // SAFETY: we do not use `unsync` (not even moving it) after calling
            // `ManuallyDrop::take` on `unsync`; we know this since we are in the destructor,
            // so even if `rwlock_unsync.unlock` were to run arbitrary code, that code would not
            // be able to access the `raw.unsync` field owned by the
            // no-longer-soundly-reachable-from-other-code `self` value.
            let guard = unsafe { ManuallyDrop::take(unsync) };

            // SAFETY: if `!SYNC`, then the `unsync` field is initialized
            let rwlock_unsync = unsafe { &self.rwlock.raw.unsync };

            // SAFETY: The guard was *not* obtained from a different rwlock; we keep
            // the rwlock reference and guard together, and do not swap them out.
            unsafe {
                rwlock_unsync.unlock_writer(guard);
            }
        }
    }
}

// SAFETY: Same as the impl for `std::sync::RwLockWriteGuard<T>`. Sharing a read guard across
// threads (behind a `&`) is akin to sharing a `& &mut T` between threads, which is permitted
// iff `T: Sync`. The relevant `&mut T`, in this case, comes from `guard.rwlock.data`.
// The `guard` also has `guard.raw` and `guard.rwlock.raw`. As per the safety invariants of
// the `guard` fields, we only access those fields in the destructor, so we do not allow concurrent
// access to them from multiple threads even if the guard implements `Sync`. Therefore, the fact
// that those two fields' types might not be `Sync` does not matter.
unsafe impl<const SYNC: bool, T: ?Sized + Sync> Sync for MaybeSyncWriteGuard<'_, SYNC, T> {}

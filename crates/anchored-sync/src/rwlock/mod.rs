/// Implementations of `Drop`, `Send`, and `Sync`, as well as a few helper functions.
mod unsafe_impls;

/// Replica of the public API of `RwLock` (in particular: stabilized methods, associated functions,
/// and many though not all trait implementations).
mod rwlock_api;
/// Replica of the public APIs of `RwLockReadGuard` and `RwLockWriteGuard` (in particular:
/// stabilized methods, associated functions, and many though not all trait implementations).
mod guard_api;

#[cfg(not(feature = "parking_lot"))]
mod std_sync_impl;
#[cfg(feature = "parking_lot")]
mod parking_lot_impl;

mod cell_impl;


use core::{cell::UnsafeCell, mem::ManuallyDrop};

use self::cell_impl::{RawCellRwLock, RawCellReadGuard, RawCellWriteGuard};

#[cfg(not(feature = "parking_lot"))]
use self::std_sync_impl::{RawRwLock, RawReadGuard, RawWriteGuard};
#[cfg(feature = "parking_lot")]
use self::parking_lot_impl::{RawRwLock, RawReadGuard, RawWriteGuard};


pub(super) const POISON_ERROR_MSG: &str =
    "Unwrapping poison in anchored-sync's `std::sync`-based rwlock";


/// A reader-writer lock that allows a number of readers or at most one writer at any
/// point in time, optionally using a faster but not threadsafe implementation (if `SYNC` is false).
///
/// If `SYNC` is true, then this type guards the `T` data with either a
/// [`std::sync::RwLock<()>`] or (if the `parking_lot` feature is enabled) a
/// [`parking_lot::RawRwLock`].
///
/// Otherwise, a [`Cell<usize>`] is used to guard the `T` data. This should be more performant than
/// the sync version.
///
/// [`Cell<usize>`]: core::cell::Cell
#[cfg_attr(not(feature = "parking_lot"), doc = " [`parking_lot::RawRwLock`]: https://docs.rs/parking_lot/latest/parking_lot/struct.RawRwLock.html")]
pub struct MaybeSyncRwLock<const SYNC: bool, T: ?Sized> {
    /// # Safety invariant
    /// If `SYNC` is true, then the `sync` field of this union is initialized; otherwise,
    /// the `unsync` field is initialized.
    raw:  MaybeSyncRawRwLock,
    data: UnsafeCell<T>,
}

#[expect(clippy::default_union_representation, reason = "we never do *any* type punning")]
union MaybeSyncRawRwLock {
    sync:   ManuallyDrop<RawRwLock>,
    unsync: ManuallyDrop<RawCellRwLock>,
}

/// A struct which semantically wraps a `&T` immutable reference to the contents
/// of a [`MaybeSyncRwLock<SYNC, T>`].
///
/// When the guard is dropped, its shared read access is released.
///
/// Note that the `SYNC` boolean affects whether the lock is `Send + Sync`, but the guard
/// type never implements `Send` and its condition for implementing `Sync` does not depend
/// on `SYNC`.
#[must_use = "if unused the MaybeSyncRwLock will immediately unlock"]
// #[must_not_suspend = "holding a MaybeSyncReadGuard across suspend \
//                       points can cause deadlocks, delays, \
//                       and cause Futures to not implement `Send`"]
#[clippy::has_significant_drop]
pub struct MaybeSyncReadGuard<'a, const SYNC: bool, T: ?Sized> {
    /// # Safety invariant
    /// Outside of the destructor (and, technically, constructor), we may only access the
    /// `guard.rwlock.data` field.
    ///
    /// We must not access `guard.rwlock.raw` except in the destructor
    /// (and, technically, constructor).
    ///
    /// This ensures that even if the `MaybeSyncRawRwLock` is not `Sync`, the `MaybeSyncReadGuard`
    /// can be `Sync` when `T: Sync` (noting that `&mut T` is `Sync` iff `T: Sync`), since a shared
    /// reference to a `MaybeSyncReadGuard` only exposes shared access to a `&mut T`
    /// obtained from `guard.rwlock.data` and does not concurrently access `guard.rwlock.raw`.
    rwlock: &'a MaybeSyncRwLock<SYNC, T>,
    /// # Safety invariant
    /// If `SYNC` is true, then the `sync` field of this union is initialized to a guard associated
    /// with `self.rwlock.raw.sync` obtained on the thread on which this struct is constructed;
    /// otherwise, the `unsync` field is initialized to a guard associated with
    /// `self.rwlock.raw.unsync`.
    ///
    /// Additionally, we must not access `guard.raw` except in the destructor
    /// (and, technically, constructor). This ensures that even if the `MaybeSyncRawReadGuard`
    /// is not `Sync`, the `MaybeSyncReadGuard` can be `Sync` when `T: Sync`
    /// (noting that `&mut T` is `Sync` iff `T: Sync`), since a shared reference to a
    /// `MaybeSyncReadGuard` only exposes shared access to a `&mut T` obtained from
    /// `guard.rwlock.data` and does not concurrently access `guard.raw`.
    raw:    MaybeSyncRawReadGuard<'a>,
}

union MaybeSyncRawReadGuard<'a> {
    sync:   ManuallyDrop<RawReadGuard<'a>>,
    unsync: ManuallyDrop<RawCellReadGuard<'a>>,
}

/// A struct which semantically wraps a `&mut T` mutable reference to the contents
/// of a [`MaybeSyncRwLock<SYNC, T>`].
///
/// When the guard is dropped, the exclusive write access is released.
///
/// Note that the `SYNC` boolean affects whether the lock is `Send + Sync`, but the guard
/// type never implements `Send` and its condition for implementing `Sync` does not depend
/// on `SYNC`.
#[must_use = "if unused the MaybeSyncRwLock will immediately unlock"]
// #[must_not_suspend = "holding a MaybeSyncWriteGuard across suspend \
//                       points can cause deadlocks, delays, \
//                       and cause Futures to not implement `Send`"]
#[clippy::has_significant_drop]
pub struct MaybeSyncWriteGuard<'a, const SYNC: bool, T: ?Sized> {
    /// # Safety invariant
    /// Outside of the destructor (and, technically, constructor), we may only access the
    /// `guard.rwlock.data` field.
    ///
    /// We must not access `guard.rwlock.raw` except in the destructor
    /// (and, technically, constructor).
    ///
    /// This ensures that even if the `MaybeSyncRawRwLock` is not `Sync`, the `MaybeSyncWriteGuard`
    /// can be `Sync` when `T: Sync` (noting that `&mut T` is `Sync` iff `T: Sync`), since a shared
    /// reference to a `MaybeSyncWriteGuard` only exposes shared access to a `&mut T`
    /// obtained from `guard.rwlock.data` and does not concurrently access `guard.rwlock.raw`.
    rwlock: &'a MaybeSyncRwLock<SYNC, T>,
    /// # Safety invariant
    /// If `SYNC` is true, then the `sync` field of this union is initialized to a guard associated
    /// with `self.rwlock.raw.sync` obtained on the thread on which this struct is constructed;
    /// otherwise, the `unsync` field is initialized to a guard associated with
    /// `self.rwlock.raw.unsync`.
    ///
    /// Additionally, we must not access `guard.raw` except in the destructor
    /// (and, technically, constructor). This ensures that even if the `MaybeSyncRawWriteGuard`
    /// is not `Sync`, the `MaybeSyncWriteGuard` can be `Sync` when `T: Sync`
    /// (noting that `&mut T` is `Sync` iff `T: Sync`), since a shared reference to a
    /// `MaybeSyncWriteGuard` only exposes shared access to a `&mut T` obtained from
    /// `guard.rwlock.data` and does not concurrently access `guard.raw`.
    raw:    MaybeSyncRawWriteGuard<'a>,
}

union MaybeSyncRawWriteGuard<'a> {
    sync:   ManuallyDrop<RawWriteGuard<'a>>,
    unsync: ManuallyDrop<RawCellWriteGuard<'a>>,
}

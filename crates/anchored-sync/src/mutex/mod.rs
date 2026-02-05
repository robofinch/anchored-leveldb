/// Implementations of `Drop`, `Send`, and `Sync`, as well as a few helper functions.
mod unsafe_impls;

/// Replica of the public API of `Mutex` and `RefCell` (in particular: stabilized methods,
/// associated functions, and many though not all trait implementations).
mod mutex_api;
/// Replica of the public API of `MutexGuard` and `RefMut` (in particular: stabilized methods,
/// associated functions, and many though not all trait implementations).
mod guard_api;

#[cfg(not(feature = "parking_lot"))]
mod std_sync_impl;
#[cfg(feature = "parking_lot")]
mod parking_lot_impl;

mod cell_impl;


use core::{cell::UnsafeCell, mem::ManuallyDrop};

use self::cell_impl::{RawCellMutex, RawCellMutexGuard};

#[cfg(not(feature = "parking_lot"))]
use self::std_sync_impl::{RawMutex, RawMutexGuard};
#[cfg(feature = "parking_lot")]
use self::parking_lot_impl::{RawMutex, RawMutexGuard};


pub(super) const POISON_ERROR_MSG: &str =
    "Unwrapping poison in anchored-sync's `std::sync`-based mutex";


/// A mutual exclusion primitive useful for protecting shared data across multiple threads
/// (if `SYNC` is true) or a single thread (if `SYNC` is false).
///
/// If `SYNC` is true, then this type guards the `T` data with either a
/// [`std::sync::Mutex<()>`] or (if the `parking_lot` feature is enabled) a
/// [`parking_lot::Mutex<()>`].
///
/// Otherwise, a [`Cell<bool>`] is used to guard the `T` data. This should be more performant than
/// the sync version.
///
/// [`Cell<bool>`]: core::cell::Cell
#[cfg_attr(not(feature = "parking_lot"), doc = " [`parking_lot::Mutex<()>`]: https://docs.rs/parking_lot/latest/parking_lot/type.Mutex.html")]
pub struct MaybeSyncMutex<const SYNC: bool, T: ?Sized> {
    /// # Safety invariant
    /// If `SYNC` is true, then the `sync` field of this union is initialized; otherwise,
    /// the `unsync` field is initialized.
    raw:  MaybeSyncRawMutex,
    data: UnsafeCell<T>,
}

#[expect(clippy::default_union_representation, reason = "we never do *any* type punning")]
union MaybeSyncRawMutex {
    sync:   ManuallyDrop<RawMutex>,
    unsync: ManuallyDrop<RawCellMutex>,
}

/// A struct which semantically wraps a `&mut T` mutable reference to the contents
/// of a locked [`MaybeSyncMutex<SYNC, T>`] mutex.
///
/// When the guard is dropped, the mutex is unlocked.
///
/// Note that the `SYNC` boolean affects whether the mutex is `Send + Sync`, but the guard
/// type never implements `Send` and its condition for implementing `Sync` does not depend
/// on `SYNC`.
#[must_use = "if unused the MaybeSyncMutex will immediately unlock"]
// #[must_not_suspend = "holding a MaybeSyncMutexGuard across suspend \
//                       points can cause deadlocks, delays, \
//                       and cause Futures to not implement `Send`"]
#[clippy::has_significant_drop]
pub struct MaybeSyncMutexGuard<'a, const SYNC: bool, T: ?Sized> {
    /// # Safety invariant
    /// Outside of the destructor (and, technically, constructor), we may only access the
    /// `guard.mutex.data` field.
    ///
    /// We must not access `guard.mutex.raw` except in the destructor
    /// (and, technically, constructor).
    ///
    /// This ensures that even if the `MaybeSyncRawMutex` is not `Sync`, the `MaybeSyncMutexGuard`
    /// can be `Sync` when `T: Sync` (noting that `&mut T` is `Sync` iff `T: Sync`), since a shared
    /// reference to a `MaybeSyncMutexGuard` only exposes shared access to a `&mut T`
    /// obtained from `guard.mutex.data` and does not concurrently access `guard.mutex.raw`.
    mutex: &'a MaybeSyncMutex<SYNC, T>,
    /// # Safety invariant
    /// If `SYNC` is true, then the `sync` field of this union is initialized to a guard associated
    /// with `self.mutex.raw.sync` obtained on the thread on which this struct is constructed;
    /// otherwise, the `unsync` field is initialized to a guard associated with
    /// `self.mutex.raw.unsync`.
    ///
    /// Additionally, we must not access `guard.raw` except in the destructor
    /// (and, technically, constructor). This ensures that even if the `MaybeSyncRawMutexGuard`
    /// is not `Sync`, the `MaybeSyncMutexGuard` can be `Sync` when `T: Sync`
    /// (noting that `&mut T` is `Sync` iff `T: Sync`), since a shared reference to a
    /// `MaybeSyncMutexGuard` only exposes shared access to a `&mut T` obtained from
    /// `guard.mutex.data` and does not concurrently access `guard.raw`.
    raw:   MaybeSyncRawMutexGuard<'a>,
}

union MaybeSyncRawMutexGuard<'a> {
    sync:   ManuallyDrop<RawMutexGuard<'a>>,
    unsync: ManuallyDrop<RawCellMutexGuard<'a>>,
}

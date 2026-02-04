#![expect(unsafe_code, reason = "implement unsafe functions in the stable public API of Weak")]

use core::fmt::{Debug, Formatter, Result as FmtResult};

#[expect(unused_imports, reason = "used for a large number of doc comments")]
use alloc::{rc, sync};
use alloc::{rc::Weak as WeakRc, sync::Weak as WeakArc};

#[cfg(feature = "clone-behavior")]
use clone_behavior::{MirroredClone, Speed};

use crate::maybe_sync::MaybeSync;
use super::{MaybeSyncArc, MaybeSyncWeak};


impl<const SYNC: bool, T> MaybeSyncWeak<SYNC, T> {
    // TODO: once const traits are supported, this should be `const`ified.
    /// See [`sync::Weak::new`] and [`rc::Weak::new`].
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_weak)(WeakArc::new()),
            MaybeSync::Unsync(ops) => (ops.from_weak)(WeakRc::new()),
        }
    }
}

impl<const SYNC: bool, T: ?Sized> MaybeSyncWeak<SYNC, T> {
    /// See [`sync::Weak::from_raw`] and [`rc::Weak::from_raw`].
    ///
    /// # Safety
    /// If `SYNC` is true, then the safety requirements of [`sync::Weak::from_raw`] must be met.
    /// Otherwise, `SYNC` is false, and the safety requirements of [`rc::Weak::from_raw`] must be
    /// met.
    #[inline]
    #[must_use]
    pub unsafe fn from_raw(ptr: *const T) -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => {
                // SAFETY: `SYNC` is true in this branch, so the caller asserts this is sound.
                let arc = unsafe { WeakArc::from_raw(ptr) };
                (ops.from_weak)(arc)
            }
            MaybeSync::Unsync(ops) => {
                // SAFETY: `SYNC` is false in this branch, so the caller asserts this is sound.
                let rc = unsafe { WeakRc::from_raw(ptr) };
                (ops.from_weak)(rc)
            }
        }
    }

    /// See [`sync::Weak::into_raw`] and [`rc::Weak::into_raw`].
    #[must_use = "losing the pointer will leak memory"]
    pub fn into_raw(this: Self) -> *const T {
        match Self::operations() {
            MaybeSync::Sync(ops) => WeakArc::into_raw((ops.into_weak)(this)),
            MaybeSync::Unsync(ops) => WeakRc::into_raw((ops.into_weak)(this)),
        }
    }

    /// See [`sync::Weak::as_ptr`] and [`rc::Weak::as_ptr`].
    #[inline]
    #[must_use]
    pub fn as_ptr(this: &Self) -> *const T {
        match Self::operations() {
            MaybeSync::Sync(ops) => WeakArc::as_ptr((ops.as_weak_ref)(this)),
            MaybeSync::Unsync(ops) => WeakRc::as_ptr((ops.as_weak_ref)(this)),
        }
    }

    /// See [`sync::Weak::upgrade`] and [`rc::Weak::upgrade`].
    #[inline]
    #[must_use]
    pub fn upgrade(this: &Self) -> Option<MaybeSyncArc<SYNC, T>> {
        match Self::operations() {
            MaybeSync::Sync(ops) => WeakArc::upgrade((ops.as_weak_ref)(this)).map(ops.from_arc),
            MaybeSync::Unsync(ops) => WeakRc::upgrade((ops.as_weak_ref)(this)).map(ops.from_arc),
        }
    }

    /// See [`sync::Weak::weak_count`] and [`rc::Weak::weak_count`].
    #[inline]
    #[must_use]
    pub fn weak_count(this: &Self) -> usize {
        match Self::operations() {
            MaybeSync::Sync(ops) => WeakArc::weak_count((ops.as_weak_ref)(this)),
            MaybeSync::Unsync(ops) => WeakRc::weak_count((ops.as_weak_ref)(this)),
        }
    }

    /// See [`sync::Weak::strong_count`] and [`rc::Weak::strong_count`].
    #[inline]
    #[must_use]
    pub fn strong_count(this: &Self) -> usize {
        match Self::operations() {
            MaybeSync::Sync(ops) => WeakArc::strong_count((ops.as_weak_ref)(this)),
            MaybeSync::Unsync(ops) => WeakRc::strong_count((ops.as_weak_ref)(this)),
        }
    }

    /// See [`sync::Weak::ptr_eq`] and [`rc::Weak::ptr_eq`].
    #[inline]
    #[must_use]
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        match Self::operations() {
            MaybeSync::Sync(ops) => WeakArc::ptr_eq(
                (ops.as_weak_ref)(this),
                (ops.as_weak_ref)(other),
            ),
            MaybeSync::Unsync(ops) => WeakRc::ptr_eq(
                (ops.as_weak_ref)(this),
                (ops.as_weak_ref)(other),
            ),
        }
    }
}

// Cloning traits

impl<const SYNC: bool, T: ?Sized> Clone for MaybeSyncWeak<SYNC, T> {
    #[inline]
    fn clone(&self) -> Self {
        match Self::operations() {
            MaybeSync::Sync(ops) => (ops.from_weak)(WeakArc::clone((ops.as_weak_ref)(self))),
            MaybeSync::Unsync(ops) => (ops.from_weak)(WeakRc::clone((ops.as_weak_ref)(self))),
        }
    }
}

#[cfg(feature = "clone-behavior")]
impl<const SYNC: bool, T: ?Sized, S: Speed> MirroredClone<S> for MaybeSyncWeak<SYNC, T> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

// Construction traits

impl<const SYNC: bool, T> Default for MaybeSyncWeak<SYNC, T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// Formatting traits

impl<const SYNC: bool, T: ?Sized + Debug> Debug for MaybeSyncWeak<SYNC, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "(Weak)")
    }
}

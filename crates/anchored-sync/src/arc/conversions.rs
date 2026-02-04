use core::pin::Pin;

use alloc::{
    rc::{Rc, Weak as WeakRc},
    sync::{Arc, Weak as WeakArc},
};

use crate::maybe_sync::MaybeSync;
use super::{MaybeSyncArc, MaybeSyncWeak};


impl<const SYNC: bool, T: ?Sized> MaybeSyncArc<SYNC, T> {
    #[inline]
    #[must_use]
    pub fn into_maybe_sync(this: Self) -> MaybeSync<Arc<T>, Rc<T>> {
        match Self::operations() {
            MaybeSync::Sync(ops) => MaybeSync::Sync((ops.into_arc)(this)),
            MaybeSync::Unsync(ops) => MaybeSync::Unsync((ops.into_arc)(this)),
        }
    }

    #[inline]
    #[must_use]
    pub fn as_maybe_sync_ref(this: &Self) -> MaybeSync<&Arc<T>, &Rc<T>> {
        match Self::operations() {
            MaybeSync::Sync(ops) => MaybeSync::Sync((ops.as_arc_ref)(this)),
            MaybeSync::Unsync(ops) => MaybeSync::Unsync((ops.as_arc_ref)(this)),
        }
    }

    #[inline]
    #[must_use]
    pub fn as_maybe_sync_mut(this: &mut Self) -> MaybeSync<&mut Arc<T>, &mut Rc<T>> {
        match Self::operations() {
            MaybeSync::Sync(ops) => MaybeSync::Sync((ops.as_arc_mut)(this)),
            MaybeSync::Unsync(ops) => MaybeSync::Unsync((ops.as_arc_mut)(this)),
        }
    }

    #[inline]
    #[must_use]
    pub fn into_maybe_sync_pin(this: Pin<Self>) -> MaybeSync<Pin<Arc<T>>, Pin<Rc<T>>> {
        match Self::operations() {
            MaybeSync::Sync(ops) => MaybeSync::Sync((ops.into_arc_pin)(this)),
            MaybeSync::Unsync(ops) => MaybeSync::Unsync((ops.into_arc_pin)(this)),
        }
    }
}

impl<T: ?Sized> MaybeSyncArc<true, T> {
    #[inline]
    #[must_use]
    pub fn from_sync_arc(arc: Arc<T>) -> Self {
        (Self::sync_operations().from_arc)(arc)
    }

    #[inline]
    #[must_use]
    pub fn into_sync_arc(this: Self) -> Arc<T> {
        (Self::sync_operations().into_arc)(this)
    }

    #[inline]
    #[must_use]
    pub fn as_sync_arc_ref(this: &Self) -> &Arc<T> {
        (Self::sync_operations().as_arc_ref)(this)
    }

    #[inline]
    #[must_use]
    pub fn as_sync_arc_mut(this: &mut Self) -> &mut Arc<T> {
        (Self::sync_operations().as_arc_mut)(this)
    }

    #[inline]
    #[must_use]
    pub fn from_sync_arc_pin(arc: Pin<Arc<T>>) -> Pin<Self> {
        (Self::sync_operations().from_arc_pin)(arc)
    }

    #[inline]
    #[must_use]
    pub fn into_sync_arc_pin(this: Pin<Self>) -> Pin<Arc<T>> {
        (Self::sync_operations().into_arc_pin)(this)
    }
}

impl<T: ?Sized> MaybeSyncArc<false, T> {
    #[inline]
    #[must_use]
    pub fn from_unsync_rc(rc: Rc<T>) -> Self {
        (Self::unsync_operations().from_arc)(rc)
    }

    #[inline]
    #[must_use]
    pub fn into_unsync_rc(this: Self) -> Rc<T> {
        (Self::unsync_operations().into_arc)(this)
    }

    #[inline]
    #[must_use]
    pub fn as_unsync_rc_ref(this: &Self) -> &Rc<T> {
        (Self::unsync_operations().as_arc_ref)(this)
    }

    #[inline]
    #[must_use]
    pub fn as_unsync_rc_mut(this: &mut Self) -> &mut Rc<T> {
        (Self::unsync_operations().as_arc_mut)(this)
    }

    #[inline]
    #[must_use]
    pub fn from_unsync_rc_pin(arc: Pin<Rc<T>>) -> Pin<Self> {
        (Self::unsync_operations().from_arc_pin)(arc)
    }

    #[inline]
    #[must_use]
    pub fn into_unsync_rc_pin(this: Pin<Self>) -> Pin<Rc<T>> {
        (Self::unsync_operations().into_arc_pin)(this)
    }
}

impl<const SYNC: bool, T: ?Sized> MaybeSyncWeak<SYNC, T> {
    #[inline]
    #[must_use]
    pub fn into_maybe_sync(this: Self) -> MaybeSync<WeakArc<T>, WeakRc<T>> {
        match Self::operations() {
            MaybeSync::Sync(ops) => MaybeSync::Sync((ops.into_weak)(this)),
            MaybeSync::Unsync(ops) => MaybeSync::Unsync((ops.into_weak)(this)),
        }
    }

    #[inline]
    #[must_use]
    pub fn as_maybe_sync_ref(this: &Self) -> MaybeSync<&WeakArc<T>, &WeakRc<T>> {
        match Self::operations() {
            MaybeSync::Sync(ops) => MaybeSync::Sync((ops.as_weak_ref)(this)),
            MaybeSync::Unsync(ops) => MaybeSync::Unsync((ops.as_weak_ref)(this)),
        }
    }

    #[inline]
    #[must_use]
    pub fn as_maybe_sync_mut(this: &mut Self) -> MaybeSync<&mut WeakArc<T>, &mut WeakRc<T>> {
        match Self::operations() {
            MaybeSync::Sync(ops) => MaybeSync::Sync((ops.as_weak_mut)(this)),
            MaybeSync::Unsync(ops) => MaybeSync::Unsync((ops.as_weak_mut)(this)),
        }
    }
}

impl<T: ?Sized> MaybeSyncWeak<true, T> {
    #[inline]
    #[must_use]
    pub fn from_sync_weak(weak: WeakArc<T>) -> Self {
        (Self::sync_operations().from_weak)(weak)
    }

    #[inline]
    #[must_use]
    pub fn into_sync_weak(this: Self) -> WeakArc<T> {
        (Self::sync_operations().into_weak)(this)
    }

    #[inline]
    #[must_use]
    pub fn as_sync_weak_ref(this: &Self) -> &WeakArc<T> {
        (Self::sync_operations().as_weak_ref)(this)
    }

    #[inline]
    #[must_use]
    pub fn as_sync_weak_mut(this: &mut Self) -> &mut WeakArc<T> {
        (Self::sync_operations().as_weak_mut)(this)
    }
}

impl<T: ?Sized> MaybeSyncWeak<false, T> {
    #[inline]
    #[must_use]
    pub fn from_unsync_weak(weak: WeakRc<T>) -> Self {
        (Self::unsync_operations().from_weak)(weak)
    }

    #[inline]
    #[must_use]
    pub fn into_unsync_weak(this: Self) -> WeakRc<T> {
        (Self::unsync_operations().into_weak)(this)
    }

    #[inline]
    #[must_use]
    pub fn as_unsync_weak_ref(this: &Self) -> &WeakRc<T> {
        (Self::unsync_operations().as_weak_ref)(this)
    }

    #[inline]
    #[must_use]
    pub fn as_unsync_weak_mut(this: &mut Self) -> &mut WeakRc<T> {
        (Self::unsync_operations().as_weak_mut)(this)
    }
}

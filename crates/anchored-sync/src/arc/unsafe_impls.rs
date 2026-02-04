#![expect(
    unsafe_code,
    reason = "needed to read union fields, drop ManuallyDrop fields, and impl Send + Sync",
)]

use core::mem::ManuallyDrop;

use super::{MaybeSyncArc, MaybeSyncWeak};


impl<const SYNC: bool, T: ?Sized> Drop for MaybeSyncArc<SYNC, T> {
    #[inline]
    fn drop(&mut self) {
        if SYNC {
            // SAFETY: if `SYNC`, then the `sync` field is active
            let sync = unsafe { &mut self.inner.sync };
            // SAFETY: we do not use `sync` (not even moving it) after calling
            // `ManuallyDrop::drop` on `sync`; we know this since it is the last action taken
            // in this branch of the destructor.
            unsafe {
                ManuallyDrop::drop(sync);
            };
        } else {
            // SAFETY: if `SYNC`, then the `unsync` field is active
            let unsync = unsafe { &mut self.inner.unsync };
            // SAFETY: we do not use `unsync` (not even moving it) after calling
            // `ManuallyDrop::drop` on `unsync`; we know this since it is the last action taken
            // in this branch of the destructor.
            unsafe {
                ManuallyDrop::drop(unsync);
            };
        }
    }
}

impl<const SYNC: bool, T: ?Sized> Drop for MaybeSyncWeak<SYNC, T> {
    #[inline]
    fn drop(&mut self) {
        if SYNC {
            // SAFETY: if `SYNC`, then the `sync` field is active
            let sync = unsafe { &mut self.inner.sync };
            // SAFETY: we do not use `sync` (not even moving it) after calling
            // `ManuallyDrop::drop` on `sync`; we know this since it is the last action taken
            // in this branch of the destructor.
            unsafe {
                ManuallyDrop::drop(sync);
            };
        } else {
            // SAFETY: if `SYNC`, then the `unsync` field is active
            let unsync = unsafe { &mut self.inner.unsync };
            // SAFETY: we do not use `unsync` (not even moving it) after calling
            // `ManuallyDrop::drop` on `unsync`; we know this since it is the last action taken
            // in this branch of the destructor.
            unsafe {
                ManuallyDrop::drop(unsync);
            };
        }
    }
}

// The attribute currently needs to be above the safety comment
// in order for the lint requiring safety comments to notice the comment
#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "even though `inner` is `!Send` in general, its `Arc` `sync` field can be `Send`",
)]
// SAFETY: Same as the impl for `Arc<T>`. It needs `T: Send` since the `Drop` impl (at the very
// least) in addition to `get_mut` and `into_inner` can send the `T`. Moreover, cloning `Self`
// and sending the clone to a different thread enables concurrent access to the inner `T`,
// thus requiring `T: Sync`.
unsafe impl<T: ?Sized + Send + Sync> Send for MaybeSyncArc<true, T> {}
// SAFETY: Same as the impl for `Arc<T>`. See above for further reasoning, noting that sending
// a `&Self` to a different thread and then cloning it to get a `Self` is the same as cloning a
// `Self` and sending the clone.
unsafe impl<T: ?Sized + Send + Sync> Sync for MaybeSyncArc<true, T> {}

// The attribute currently needs to be above the safety comment
// in order for the lint requiring safety comments to notice the comment
#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "even though `inner` is `!Send` in general, its `WeakArc` `sync` field can be `Send`",
)]
// SAFETY: Same as the impl for `Weak<T>`. It needs `T: Send` since the `Drop` impl (at the very
// least) in addition to `upgrade` and `into_inner` can send the `T`. Moreover, cloning `Self`
// and sending the clone to a different thread (and then upgrading that copy) enables concurrent
// access to the inner `T`, thus requiring `T: Sync`.
unsafe impl<T: ?Sized + Send + Sync> Send for MaybeSyncWeak<true, T> {}
// SAFETY: Same as the impl for `Weak<T>`. See above for further reasoning, noting that sending
// a `&Self` to a different thread and then cloning it to get a `Self` is the same as cloning a
// `Self` and sending the clone.
unsafe impl<T: ?Sized + Send + Sync> Sync for MaybeSyncWeak<true, T> {}

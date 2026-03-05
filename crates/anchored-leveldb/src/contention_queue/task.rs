#![expect(unsafe_code, reason = "assert that a generic type is covariant over a lifetime")]

use std::{cell::UnsafeCell, marker::PhantomData};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    mem::{MaybeUninit, transmute},
    sync::{Condvar, MutexGuard, PoisonError},
};

use variance_family::UpperBound;

use crate::utils::unsafe_cell_get_mut_unchecked;
use super::ad_hoc_variance_family_trait::AdHocCovariantFamily;
use super::queue::ProcessingPanicked;


const FRONT_BIT: u8 = 0b_01;
const PANIC_BIT: u8 = 0b_10;

/// This is the table that a woken `TaskState` should use to decide what to do. The LSB indicates
/// whether the state is at the front of the queue. The second-least significant bit indicates
/// whether a panic occurred while processing it. (Since `queue_poisoned` may be from a *prior*
/// panic.)
/// <pre overflow-x: scroll>
/// ┌─────────┬────────────────┬──────────────────┬───────────────────────────────────────────┐
/// │ `state` │ `value()`      │ `queue_poisoned` | Response                                  │
/// ├─────────┼────────────────┼──────────────────┼───────────────────────────────────────────┤
/// │ 0's X 0 │ Some(_) | None │  true || false   | Unprocessed, or processing. Keep waiting. │
/// ├─────────┼────────────────┼──────────────────┼───────────────────────────────────────────┤
/// │ 0's X 1 │ Some(_) | None │  true            | At the front. If `unwrap_queue_poison`,   │
/// │         │                │                  | wake next task, then panic. Otherwise,    │
/// │         │                │                  | ignore `queue_poisoned`; see below.       │
/// ├─────────┼────────────────┼──────────────────┼───────────────────────────────────────────┤
/// │ 0's X 1 │ Some(_)        │  false           | At the front. Start processing stuff.     │
/// │         │                │  (or ignored)    | When done, make the next task the front.  │
/// ├─────────┼────────────────┼──────────────────┼───────────────────────────────────────────┤
/// │ 0's 0 1 │ None           │  false           | ProcessedElsewhere. Make the next task    │
/// │         │                │  (or ignored)    | the front, and return.                    │
/// ├─────────┼────────────────┼──────────────────┼───────────────────────────────────────────┤
/// │ 0's 1 1 │ None           │  false           | ProcessingPanicked. Make the next task    │
/// │         │                │  (or ignored)    | the front, and return.                    │
/// ├─────────┴────────────────┴──────────────────┼───────────────────────────────────────────┤
/// │           Anything else                     | Impossible. This case can be ignored.     │
/// └─────────────────────────────────────────────┴───────────────────────────────────────────┘
/// </pre>
/// Note that an unprocessed or processing task is never added to an empty queue (if any active
/// front task is counted as making the queue nonempty); therefore, they don't need to check if
/// they're at the front. Whatever pops them from the queue is responsible for updating their
/// state.
#[derive(Debug)]
pub(super) struct TaskState {
    condvar: Condvar,
    state:   UnsafeCell<u8>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl TaskState {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            condvar: Condvar::new(),
            state:   UnsafeCell::new(0),
        }
    }

    /// Returns the input guard, in addition to whether the task panicked while something else
    /// processed it (if it was processed by something else).
    ///
    /// Note that this function unwraps poison in order to avoid panicking. However, it does
    /// not clear poison.
    ///
    /// # Robust guarantee
    /// The returned guard is a guard of the same mutex as the given guard.
    ///
    /// # Safety
    /// All concurrent calls to `self`'s unsafe methods must be synchronized across threads by the
    /// `Mutex` associated with `guard`.
    pub unsafe fn wait_until_at_front<'m, M>(
        &self,
        mut guard: MutexGuard<'m, M>,
    ) -> (MutexGuard<'m, M>, ProcessingPanicked) {
        // Correctness of robust guarantee: holds by correctness of `std::sync::Condvar`.
        loop {
            guard = self.condvar.wait(guard).unwrap_or_else(PoisonError::into_inner);

            // SAFETY: We only access `self.state`'s contents within `self`'s unsafe methods,
            // so the caller asserts that we are the only function trying to access `self.state`'s
            // contents (since all such method calls are synchronized by a `Mutex` we hold
            // (as asserted by the caller), and we do not leak references to `self.state`'s
            // contents outside the `unsafe` methods of `self`).
            // Therefore, we can exclusively borrow `self.state`'s contents.
            let state = unsafe { *unsafe_cell_get_mut_unchecked(&self.state) };
            if state & FRONT_BIT != 0 {
                break (guard, ProcessingPanicked(state & PANIC_BIT != 0));
            }
        }
    }

    /// # Safety
    /// All concurrent calls to `self`'s unsafe methods must be synchronized across threads by a
    /// lock.
    /// (The lock must be held when calling this method.)
    pub unsafe fn wake_front_task(&self) {
        // SAFETY: Same as in `self.wait`.
        let state = unsafe { unsafe_cell_get_mut_unchecked(&self.state) };
        *state |= FRONT_BIT;

        self.condvar.notify_one();
    }

    /// # Safety
    /// All concurrent calls to `self`'s unsafe methods must be synchronized across threads by a
    /// lock.
    /// (The lock must be held when calling this method.)
    pub unsafe fn wake_front_task_panicking(&self) {
        // SAFETY: Same as in `self.wait`.
        let state = unsafe { unsafe_cell_get_mut_unchecked(&self.state) };
        *state |= PANIC_BIT | FRONT_BIT;

        self.condvar.notify_one();
    }
}

pub(super) struct Task<'t, 'v, Value: AdHocCovariantFamily, Upper: UpperBound> {
    pub state: &'t TaskState,
    pub value: Option<Value::Varying<'v>>,
    pub _future_proofing: PhantomData<Upper>,
}

impl<'v, V: AdHocCovariantFamily, U: UpperBound> Debug for Task<'_, 'v, V, U>
where
    V::Varying<'v>: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Task")
            .field("state", &self.state)
            .field("value", &self.value)
            .finish()
    }
}

pub(super) struct ErasedTask<'erase, Value: AdHocCovariantFamily> {
    /// # Safety invariant
    /// Should be initialized to a value which is valid as type
    /// `Task<'t, 'varying, Value, &'erase ()>` for the `'t` and `'varying` lifetimes used in
    /// `Self::new`.
    ///
    /// If that lifetime is dead, this value may be dangling (in which case the user cannot
    /// soundly call `Self::into_inner`, `Self::inner`, or `Self::take`).
    maybe_dangling: MaybeUninit<Task<'erase, 'erase, Value, &'erase ()>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'erase, V: AdHocCovariantFamily> ErasedTask<'erase, V> {
    #[inline]
    #[must_use]
    pub const fn new<'t: 't, 'varying: 'varying>(task: Task<'t, 'varying, V, &'erase ()>) -> Self {
        let not_dangling = MaybeUninit::new(task);

        // SAFETY: `not_dangling` is trivially valid
        // as type `MaybeUninit<Task<'t, 'varying, V, &'erase ()>>`
        // or as type `MaybeUninit<Task<'erase, 'erase, V, &'erase ()>>`,
        // since `MaybeUninit` has no validity (or safety) requirements.
        let maybe_dangling = unsafe {
            transmute::<
                MaybeUninit<Task<'t, 'varying, V, &'erase ()>>,
                MaybeUninit<Task<'erase, 'erase, V, &'erase ()>>,
            >(not_dangling)
        };

        // Safety invariant: Trivially, `maybe_dangling` is initialized to a valid value of
        // type `Task<'t, 'varying, V, &'erase ()>`.
        Self { maybe_dangling }
    }

    /// # Safety
    /// The unerased task from which `self` was created (via [`Self::new`]) must have had lifetimes
    /// which were at least as long as `'t` and `'varying`, respectively.
    #[inline]
    #[must_use]
    pub unsafe fn into_inner<'t, 'varying>(self) -> Task<'t, 'varying, V, &'erase ()> {
        let maybe_dangling = self.maybe_dangling;

        // SAFETY: `maybe_dangling` is trivially valid
        // as type `MaybeUninit<Task<'erase, 'erase, V, &'erase ()>>`,
        // or as type `MaybeUninit<Task<'t, 'varying, V, &'erase ()>>`
        // since `MaybeUninit` has no validity (or safety) requirements.
        let not_dangling = unsafe {
            transmute::<
                MaybeUninit<Task<'erase, 'erase, V, &'erase ()>>,
                MaybeUninit<Task<'t, 'varying, V, &'erase ()>>,
            >(maybe_dangling)
        };

        // SAFETY: By the callers assertion, and by the safety invariant of `self.maybe_dangling`,
        // the round-trip between `Self::new` and `Self::into_inner` is equivalent to a lifetime
        // transmute of `Task<'long_a, 'long_varying, V, &'erase ()>`
        // to `Task<'t, 'varying, V, &'erase ()>`. Since `Task` is covariant over `'t`,
        // and the unsafe trait bound on `V` implies that it's sound to perform covariant
        // casts of `'varying`, and since covariant coecions allow lifetimes to be shortened
        // in this position, this lifetime transmute is sound. That is, `not_dangling` is properly
        // inititalized and valid for its output type of `Task<'t, 'varying, V, &'erase ()>`.
        unsafe { not_dangling.assume_init() }
    }

    /// # Safety
    /// The unerased task from which `self` was created (via [`Self::new`]) must have had lifetimes
    /// which were at least as long as `'t` and `'varying`, respectively.
    #[inline]
    #[must_use]
    pub unsafe fn inner<'s: 's, 't, 'varying>(&'s self) -> &'s Task<'t, 'varying, V, &'erase ()> {
        let maybe_dangling = &self.maybe_dangling;

        // SAFETY: `maybe_dangling` is valid
        // as type `&'_ MaybeUninit<Task<'erase, 'erase, V, &'erase ()>>`,
        // or as type `&'_ MaybeUninit<Task<'t, 'varying, V, &'erase ()>>`
        // since `MaybeUninit` has no validity (or safety) requirements, and they have the same
        // size and alignment, since they only differ in lifetimes; therefore, a reference
        // to a pointee of either type is valid as a reference to a pointee of the other type.
        let not_dangling = unsafe {
            transmute::<
                &'_ MaybeUninit<Task<'erase, 'erase, V, &'erase ()>>,
                &'_ MaybeUninit<Task<'t, 'varying, V, &'erase ()>>,
            >(maybe_dangling)
        };

        // SAFETY: By the callers assertion, and by the safety invariant of `self.maybe_dangling`,
        // the round-trip between `Self::new` and `Self::inner` is equivalent to a lifetime
        // transmute of `&'_ Task<'long_a, 'long_varying, V, &'erase ()>`
        // to `&'_ Task<'t, 'varying, V, &'erase ()>`. Since `Task` is covariant over `'t`,
        // and the unsafe trait bound on `V` implies that it's sound to perform covariant
        // casts of `'varying`, and since covariant coecions allow lifetimes to be shortened
        // in this position, this lifetime transmute is sound. That is, `not_dangling` is properly
        // inititalized and valid for its output type of `Task<'t, 'varying, V, &'erase ()>`.
        unsafe { not_dangling.assume_init_ref() }
    }

    /// # Safety
    /// The unerased task from which `self` was created (via [`Self::new`]) must have had a
    /// `'long_varying` lifetime which was at least as long as `'varying`.
    #[inline]
    #[must_use]
    pub unsafe fn take<'varying: 'varying, 's: 's>(&'s mut self) -> Option<V::Varying<'varying>> {
        // Safety invariant: see `value.replace(None)` below, which is the only place in this
        // function where we mutate `self.maybe_dangling`.
        let task: *mut Task<'erase, 'erase, V, &'erase ()> = self.maybe_dangling.as_mut_ptr();

        // SAFETY: `self.maybe_dangling: MaybeUninit<Task<'_, '_, V, &'erase ()>>`, so its
        // allocation is large enough to contain a value of type
        // `Task<'erase, 'erase, V, &'erase ()>`. Therefore, adding the offset of the `value`
        // field to the `task` pointer remains in-bounds of its source allocation, and the
        // addition does not wrap around the address space or exceed `isize::MAX`.
        // Note that, despite this looking like a dereference, no `Deref` operation or similar
        // coercion occurs (since `task` is a raw pointer to a type which has a field named
        // `value`), meaning that the safety requirements of
        // <https://doc.rust-lang.org/std/ptr/macro.addr_of_mut.html#safety> apply, which are the
        // same as those of <https://doc.rust-lang.org/std/primitive.pointer.html#method.offset>,
        // which we have met.
        let value: *mut Option<V::Varying<'erase>> = unsafe { &raw mut (*task).value };

        // We are *shortening* the lifetime of `Option<V<'erase>>` in an invariant position.
        // **Writing a `Some(V)` value to the reference might be unsound**,
        // since writes require that only contravariant casts have occurred.
        // We only ever write `None` below, which is fine. We may read an owned `Some(V<'erase>)`
        // as a `V<'varying>` value, and since `V` is covariant over `'varying` and since the
        // caller asserts that the backing data remains valid, this is fine.
        let value: *mut Option<V::Varying<'varying>> = value.cast();

        // SAFETY: `value` points to a valid value of type `Option<V::Varying<'long_varying>>`,
        // where `'long_varying: 'varying`, as asserted by the caller. (Since `'varying` is
        // currently active, so must `'long_varying` be active.) As required by the unsafe trait
        // bound on `V`, covariant coercions of the `'varying` lifetime of `V::Varying<'varying>`
        // must be sound. Since `Option<_>` is covariant over its generic parameter, and since
        // this read (combined with `Self::new`) amounts to a lifetime transmute from
        // `Option<V::Varying<'long_varying>>` to `Option<V::Varying<'varying>>` (which shortens
        // the lifetime), and since covariant coercions allow lifetimes to be shortened in this
        // position, it follows that any value of type `Option<V::Varying<'long_varying>>` must
        // be valid for type `Option<V::Varying<'varying>>` as well, and thus this is sound.
        //
        // NOTE: `value.replace` places no safety requirements on what it's replacing the pointee
        // of `value` with. However, we also need to fulfill the safety invariant of
        // `self.maybe_dangling`.
        // Safety invariant: After this operation, the `value` field of the task is still a
        // valid value of type `Option<V::Varying<'long_varying>>`, since
        // `Option::<V::Varying<'varying>>::None` is still valid as a value of type
        // `Option<V::Varying<'any>>` for `'any` lifetime, since that enum variant holds no data.
        // Since that's the only field of `self.maybe_dangling` which we mutate, we thus have
        // that `self.maybe_dangling` continues to be initialized to a valid value of type
        // `Option<V::Varying<'long_varying>>`.
        unsafe { value.replace(None) }
    }
}

impl<V: AdHocCovariantFamily> Debug for ErasedTask<'_, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ErasedTask").finish_non_exhaustive()
    }
}

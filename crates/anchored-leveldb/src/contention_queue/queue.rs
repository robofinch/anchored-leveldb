#![expect(unsafe_code, reason = "synchronize concurrent accesses without storing a mutex inline")]

use std::{mem, process, ptr};
use std::{cell::UnsafeCell, collections::VecDeque, marker::PhantomData, mem::MaybeUninit};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    panic::{AssertUnwindSafe, catch_unwind, RefUnwindSafe, resume_unwind, UnwindSafe},
    sync::{atomic::{AtomicUsize, Ordering}, Mutex, MutexGuard, PoisonError},
};

use crate::utils::{unsafe_cell_get_mut_unchecked, unsafe_cell_get_ref_unchecked, UnwrapPoison as _};
use super::ad_hoc_variance_family_trait::AdHocCovariantFamily;
use super::task::{ErasedTask, Task, TaskState};


/// Queue state that can only be accessed by the thread whose task is currently processing tasks.
struct FrontExclusive<FS> {
    /// User-controlled state. There is no safety invariant on this field.
    front_state: FS,
}

/// Queue state that can only be accessed while the mutex is held.
struct MutexExclusive<'upper, Value: AdHocCovariantFamily> {
    /// Whether the `FrontExclusive` state is locked. Equivalently, whether `process_unchecked`
    /// is currently processing something, or whether exclusive permissions over `front_exclusive`
    /// are currently held by *something*.
    ///
    /// # Safety invariant
    /// See `ContentionQueue.front_exclusive`.
    front_locked:   bool,
    /// Whether any previous task panicked since the last time `queue.clear_queue_poison()` was
    /// called.
    ///
    /// There is no safety invariant on this field.
    queue_poisoned: bool,
    /// The queue consists of tasks whose `value_unvarying` fields are `None`, which have
    /// semantically been popped from the queue facade presented to the user but have not yet
    /// been woken up, followed by tasks whose `value_unvarying` fields are `Some` and which
    /// have not yet been popped in the facade presented to the user.
    ///
    /// # Safety invariants
    /// - Only the front task of the queue may be woken (with `wake_front_task` or
    ///   `wake_front_task_panicking`).
    /// - An invocation of `process_unchecked` must execute the full sequence of
    ///   pushing an erased task into the queue, calling `task_state.wait_until_at_front(_)`, and
    ///   popping the erased task from the queue without unwinding. (Else, abort.)
    /// - Erased tasks must not be pushed or popped into/from the queue except as provided for
    ///   above.
    /// - The safety requirement for synchronizing accesses to the contents of `mutex_exclusive`
    ///   should be considered to extend to unsafe accesses to the tasks pushed/popped into/from
    ///   the queue.
    ///
    /// ## Implications
    /// The above requirements imply that popping an erased task from the queue can use
    /// `unwrap_unchecked` *and* that the task which an invocation of `process_unchecked` pops
    /// is the same one that it pushed.
    ///
    /// # Correctness invariants
    /// - All tasks in the queue must have a thread waiting for that task to be woken. Otherwise,
    ///   a hang would occur. (Our solution for a thread panicking after pushing a task onto
    ///   the queue and before popping it is to just abort, for simplicity :P)
    /// - A task should only be pushed onto the queue only if something else holds exclusive
    ///   permissions over `front_exclusive` (which means that something else would wake up this
    ///   task and transfer it permissions).
    queue:          VecDeque<ErasedTask<'upper, Value>>,
}

pub(crate) struct ContentionQueue<'upper, FrontState, Value: AdHocCovariantFamily> {
    /// # Safety invariant
    /// This must be initialized to `0`. Its management is handled by `Self::assert_mutex_good`,
    /// which has an extensive safety comment, and this field should not be mutated anywhere else.
    mutex_address:    AtomicUsize,
    /// There is no safety invariant on this field.
    options:          PanicOptions,
    /// # Safety invariant
    /// Can only be accessed by a given invocation of `self.process_unchecked` if the invocation
    /// acquired exclusive permissions over `front_exclusive` by either:
    /// - changing `front_locked` from `false` to `true`, or
    /// - successfully pushing an erased task into the queue, calling
    ///   `task_state.wait_until_at_front(_)`, and popping the erased task from the queue
    ///   (all without unwinding),
    ///
    /// **and** has not yet released exclusive permissions over `front_exclusive` by changing
    /// `front_locked` from `true` to `false`,
    /// **and** has not yet transferred exclusive permissions over `front_exclusive` to the front
    /// task of the queue (if any) by calling `wake_front_task` or `wake_front_task_panicking`
    /// on it. (By the safety invariants of `queue`, only the front task is permitted to be woken.)
    ///
    /// Additionally, the invocation of `self.process_unchecked` is allowed to release or transfer
    /// exclusive permissions over `front_exclusive` only if it had acquired those permissions and
    /// not yet released or transferred them.
    ///
    /// ## Sufficiency of requirements
    /// We need to make sure that at most one thread ever has exclusive permissions over
    /// `front_exclusive`. Note that we only ever read or write `front_locked`, wake the top task of
    /// the queue, or push something into the queue while we hold a mutex that synchronizes all
    /// those operations. This greatly simplifies the reasoning. Additionally, each invocation of
    /// `self.process` internally uses only a single thread. (User callbacks might be multithreaded,
    /// but the `QueueHandle` is `!Send + !Sync`.)
    ///
    /// - `front_locked` is `false` iff exclusive permissions over `front_exclusive` are *not*
    ///   currently held, so changing `front_locked` from `false` to `true` allows only a single
    ///   invocation of `self.process_unchecked` (and thus a single thread) to acquire exclusive
    ///   permissions.
    /// - Conversely, changing `front_locked` from `true` to `false` if permissions are currently
    ///   held - and then not continuing to act as though we still hold exclusive permissions -
    ///   means that 0 threads then hold exclusive permissions over `front_exclusive` (and the next
    ///   change from `false` to `true` will work correctly).
    /// - Since `wake_front_task` or `wake_front_task_panicking` can be called only if exclusive
    ///   permissions are being transferred, and those are the only two methods which cause
    ///   `task_state.wait_until_at_front(_)` to return (normally, and without unwinding), the
    ///   second means of acquiring exclusive permissions (from a handoff) is also sufficient. The
    ///   restriction on successfully pushing and popping a task without unwinding (together with
    ///   the other safety invariants of `queue`) are, to some extent, "merely" correctness
    ///   requirements for managing exclusive permissions over `front_exclusive`, but elevating
    ///   them to safety requirements makes the state of `queue` easier to reason about.
    ///
    /// # Correctness invariant
    /// If an invocation of `process_unchecked` holds exclusive permissions over `front_exclusive`,
    /// it should transfer them or release them (if there's nothing to transfer them to) before
    /// returning or unwinding out of the function.
    /// Otherwise, indefinite hangs could occur while other threads wait, in futility, to have
    /// exclusive permissions transferred to them.
    front_exclusive:  UnsafeCell<FrontExclusive<FrontState>>,
    /// # Safety invariant
    /// Its contents may only be accessed if a lock used to synchronize this `ContentionQueue`
    /// -- that is, a `mutex` for which `self.assert_mutex_good(mutex)` successfully returned without
    /// panicking -- is currently held. Note that in some places we acquire only shared rather
    /// than exclusive access over this field; standard aliasing rules apply, as though there
    /// were no `UnsafeCell` (while the `mutex` is held).
    mutex_exclusive:  UnsafeCell<MutexExclusive<'upper, Value>>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, V: AdHocCovariantFamily> ContentionQueue<'_, FS, V> {
    #[inline]
    #[must_use]
    pub const fn new(front_state: FS) -> Self {
        let default_options = PanicOptions {
            unwrap_mutex_poison: true,
            unwrap_queue_poison: true,
        };
        Self::new_with_options(front_state, default_options)
    }

    #[inline]
    #[must_use]
    pub const fn new_with_options(front_state: FS, options: PanicOptions) -> Self {
        Self {
            // Safety invariant: initialized to zero.
            mutex_address:   AtomicUsize::new(0),
            options,
            front_exclusive: UnsafeCell::new(FrontExclusive {
                front_state,
            }),
            mutex_exclusive: UnsafeCell::new(MutexExclusive {
                // Correctness invariant: nothing has exclusive permissions over `front_state`.
                front_locked:   false,
                queue_poisoned: false,
                // Correctness invariant: vacuously, all of the zero tasks in the queue have a
                // thread waiting for that task to be woken.
                queue:          VecDeque::new(),
            }),
        }
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'upper, FS, V: AdHocCovariantFamily> ContentionQueue<'upper, FS, V> {
    /// # Robust guarantee
    /// If this function successfully returns, then the contents of `self.mutex_exclusive` can be
    /// soundly accessed while `mutex` is locked.
    ///
    /// # Panics
    /// May panic if `mutex` is not the same [`Mutex`] used by previous calls to
    /// `self.process(..)`, `self.is_queue_poisoned(_)`, or `self.clear_queue_poison(_)`.
    ///
    /// To be more precise, this function is guaranteed to panic if `mutex` is not located at the
    /// same address in memory as previously-used `Mutex`es.
    fn assert_mutex_good<M>(&self, mutex: &Mutex<M>) {
        // See `std/src/sys/sync/condvar/pthread.rs::Condvar::Verify`.

        let mutex_addr = ptr::from_ref::<Mutex<M>>(mutex).addr();

        // `Relaxed` is fine here because we never read through `mutex`, we truly care only
        // about the address of the mutex.
        #[expect(clippy::panic, reason = "panic is declared, and is effectively a manual assert!")]
        match self.mutex_address.compare_exchange(
            0,
            mutex_addr,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => {}                     // Stored the address
            Err(n) if n == mutex_addr => {} // The same address had already been stored
            _ => panic!(
                "All calls to `ContentionQueue::process` \
                 on the same queue must use the same `Mutex`",
            ),
        }

        assert!(size_of_val(mutex) > 0, "`std::sync::Mutex` should not be a ZST");

        // Correctness of robust guarantee:
        // Any concurrent calls to methods of `self` must use the same mutex as this call or
        // otherwise immediately panic in the above asserts, due to the below reasons. Therefore,
        // locking `mutex` is sufficient to hold exclusive permissions over the contents of
        // `mutex_exclusive` (so long as, by convention, all accesses to `mutex_exclusive` are
        // guarded by `mutex`, which is indeed the case as per its safety invariant).
        // - Regardless of atomic ordering, across all threads, only the first call to
        //   `assert_mutex_good` can observe `0` in `self.mutex_address` and receive permission to
        //   use the mutex regardless of its address.
        // - Later calls to `assert_mutex_good` *might* coincidentally receive permission to use a
        //   different `Mutex` that was moved or otherwise placed at the same location in memory as
        //   the original mutex. However, because we are given a mutex reference, that implies that
        //   the `mutex` is temporarily pinned in memory for the duration we have the reference.
        //
        //   Moreover, the `mutex` is also pinned in memory for the duration of any concurrent
        //   calls. If would be UB if `mutex` were switched for a different mutex in the overlap
        //   in the below diagram:
        //   ┌──────────┬─────┬──────────┬──────────────────────────────────┬──────────┐
        //   │ thread 1 │ ... | ...      │          call to `self.is_queue_poisoned`   │
        //   │ mutex    │ ... │ pinned   │ still pinned, UB to move it here │   pinned │
        //   │ thread 2 │ ... |   call to `self.process`                    │      ... │
        //   └──────────┴─────┴──────────┴──────────────────────────────────┴──────────┘
        //   Therefore, `mutex` remains pinned at the same address for the duration of all
        //   concurrent calls to `self.process`. All `Mutex<T>`s are not `ZST`s... and even
        //   if `std::sync::Mutex<()>` ever did become a ZST, or if there's some technically-sound
        //   shenanigans involving types like `&std::sync::Mutex<!>`, an above assert would prevent
        //   that from causing a problem.
        //
        //   Additionally, even though we do not fix `M` (meaning that, e.g., accesses via
        //   `mutex: &Mutex<[u8; 1]>` and `mutex: &Mutex<[u8]>` could be concurrent), that should
        //   not be a problem; **distinct mutexes cannot overlap**, so this code is sound.
        //   Only marginally shakier is the claim that non-distinct mutexes have the same base
        //   address, though that is "merely" required for this function to not unexpectedly panic.
        //   (And, to be clear, this code is correct for all current implementations of
        //   `std::sync::Mutex`; I just don't feel absolutely confident in claiming that `std`
        //   **cannot** ever change that fact. It'd *probably* be a breaking change? But no need
        //   to even worry about that, since this code would remain sound.)
        //
        //   By the above reasoning, at most one `Mutex` can be at a given memory address at a
        //   given time.
        //   (And, two references to of the same `Mutex` *probably should* have the same address).
        //   Since `self.assert_mutex_good(mutex)`'s asserts only permit a single memory address to
        //   be used for `mutex`, this implies that only a single `Mutex` can pass the above
        //   asserts for the entire duration of a series of concurrent calls to `self.process`.
        // Other note:
        // - It would still be possible to switch out the `Mutex` between non-concurrent calls
        //   and pass the asserts. Note that this would not even cause a panic with `Condvar`s,
        //   since they're only used within concurrent/contending calls.
    }

    /// # Safety
    /// A `mutex` for which `self.assert_mutex_good(mutex)` successfully returned must be locked
    /// by the current thread.
    ///
    /// No other references to the contents of `self.mutex_exclusive` (not derived from the
    /// returned reference) may be active during the lifetime `'_`.
    #[allow(clippy::mut_from_ref, reason = "yeah, places a high burden on the caller")]
    #[inline]
    #[must_use]
    unsafe fn mutex_exclusive_mut(&self) -> &mut MutexExclusive<'upper, V> {
        // SAFETY: The safety preconditions are equivalent to those of
        // `unsafe { &mut *self.mutex_exclusive.get() }`. We only need to ensure the aliasing
        // rules are upheld. As per the safety precondition of this function and the extensive
        // reasoning above in `self.assert_mutex_good(_)`, we have that this is sound. The caller
        // does have to manually uphold the aliasing rules, though.
        unsafe {
            unsafe_cell_get_mut_unchecked(&self.mutex_exclusive)
        }
    }

    /// # Safety
    /// A `mutex` for which `self.assert_mutex_good(mutex)` successfully returned must be locked
    /// by the current thread.
    ///
    /// No mutable references to the contents of `self.mutex_exclusive` may be active during the
    /// lifetime `'_`.
    #[inline]
    #[must_use]
    unsafe fn mutex_exclusive_ref(&self) -> &MutexExclusive<'upper, V> {
        // SAFETY: The safety preconditions are equivalent to those of
        // `unsafe { &mut *self.mutex_exclusive.get() }`. We only need to ensure the aliasing
        // rules are upheld. As per the safety precondition of this function and the extensive
        // reasoning above in `self.assert_mutex_good(_)`, we have that this is sound. The caller
        // does have to manually uphold the aliasing rules, though.
        unsafe {
            unsafe_cell_get_ref_unchecked(&self.mutex_exclusive)
        }
    }

    /// # Robust guarantee
    /// This function returns `true` without unwinding only if exclusive access over
    /// `self.front_exclusive` has been acquired.
    ///
    /// # Safety
    /// A `mutex` for which `self.assert_mutex_good(mutex)` successfully returned must be locked
    /// by the current thread.
    ///
    /// No references to the contents of `self.mutex_exclusive` may exist when this function is
    /// called.
    #[allow(clippy::mut_from_ref, reason = "`UnsafeCell` is involved in this `unsafe fn`")]
    #[inline]
    #[must_use]
    unsafe fn try_acquire_front_fast(&self) -> bool {
        // SAFETY:
        // - A `mutex` for which `self.assert_mutex_good(mutex)` successfully returned must be
        //   locked by the current thread, as asserted by the caller.
        // - No other references to the contents of `self.mutex_exclusive` are active during the
        //   lifetime `'_` of `mutex_exclusive`, whose lifetime lasts only within this function
        //   call, as asserted by the caller.
        let mutex_exclusive = unsafe { self.mutex_exclusive_mut() };

        if mutex_exclusive.front_locked {
            false
        } else {
            // Safety invariant, and correctness of robust guarantee:
            // We change `mutex_exclusive.front_locked` from `false` to `true`, thereby acquiring
            // exclusive access over `front_exclusive`, as described by the safety invariant of
            // that field.
            mutex_exclusive.front_locked = true;
            true
        }
    }

    /// Note that this function ignores poison to avoid panicking. Both mutex and queue
    /// poison need to be checked. It does, however, allow OOM.
    ///
    /// # Robust guarantees
    /// - When this function returns *or* unwinds, exclusive permissions over
    ///   `self.front_exclusive` have been acquired.
    /// - When this function returns or unwinds, the task pushed onto `queue` by this function has
    ///   already been popped. In other words, it is sound to drop the backing data of `task_state`
    ///   and `value` as soon as this function returns or unwinds and all other usages of them
    ///   (including the returned `TaskWaitResult<V::Varying<'v>>`) cease.
    /// - The returned guard is a guard of `mutex`.
    ///
    /// # Correctness
    /// Something else should have exclusive permissions over `self.front_exclusive`, or else
    /// an indefinite hang may occur.
    ///
    /// # Safety
    /// `guard` must be the guard of a `mutex` for which `self.assert_mutex_good(mutex)`
    /// successfully returned.
    ///
    /// No references to the contents of `self.mutex_exclusive` may exist when this function is
    /// called.
    #[allow(clippy::mut_from_ref, reason = "`UnsafeCell` is involved in this `unsafe fn`")]
    unsafe fn try_wait_until_at_front<'m, 't, 'v, M>(
        &self,
        abort_on_drop: AbortIfNotAtFront,
        mut guard:     MutexGuard<'m, M>,
        task_state:    &'t TaskState,
        value:         V::Varying<'v>,
    ) -> (MutexGuard<'m, M>, TaskWaitResult<V::Varying<'v>>)
    where
        V: AdHocCovariantFamily,
    {
        let unerased_task: Task<'t, 'v, V, &()> = Task {
            state:            task_state,
            value:            Some(value),
            _future_proofing: PhantomData,
        };
        let erased_task = ErasedTask::new(unerased_task);

        {
            // SAFETY: As proven by `guard` and by the caller's assertion, the current thread
            // has a lock for which `self.assert_mutex_good(_)` successfully returned.
            // Additionally, as asserted by the caller, no other references to the contents of
            // `self.mutex_exclusive` exist when `try_wait_until_at_front` is called. Therefore,
            // `mutex_exclusive` is unique within this block (to which its lifetime is constrained).
            let mutex_exclusive = unsafe { self.mutex_exclusive_mut() };

            // This could cause OOM. We document it.
            // Correctness: this thread waits for the pushed task below. Additionally, as asserted
            // by the caller, something else holds exclusive permissions over
            // `self.front_exclusive`.
            mutex_exclusive.queue.push_back(erased_task);
        };

        // Note that this function ignores poison to avoid panicking.
        // SAFETY:
        // The sole calls to `task_state`'s unsafe methods are:
        // - `task_state.wait_until_at_front(guard)` in `try_wait_until_at_front` here,
        // - `next_front_task.state.wake_front_task()` in `self.process_unchecked`, and
        // - `next_front_task.state.wake_front_task_panicking()` in `self.process_unchecked`.
        // During all three calls, we hold a `guard` of a `mutex` for which
        // `self.assert_mutex_good(mutex)` successfully returned, so access to the task is
        // synchronized across threads by a lock.
        // This also fulfills the safety invariant of `mutex_exclusive` that extends to tasks pushed
        // or popped into/from the queue.
        let (returned_guard, processing_panicked) = unsafe {
            task_state.wait_until_at_front(guard)
        };
        // Robustness guarantee of `wait_until_at_front` implies that `returned_guard` is a guard
        // of the same mutex as `guard` was (namely, of `mutex`).
        guard = returned_guard;

        let result = {
            // SAFETY: As proven by `guard` and by the caller's assertion, the current thread
            // has a lock for which `self.assert_mutex_good(_)` successfully returned.
            // Additionally, as asserted by the caller, no other references to the contents of
            // `self.mutex_exclusive` exist when `try_wait_until_at_front` is called. Therefore,
            // `mutex_exclusive` is unique within this block (to which its lifetime is constrained).
            let mutex_exclusive = unsafe { self.mutex_exclusive_mut() };

            let this_task = mutex_exclusive.queue.pop_front();
            // SAFETY: By the safety invariants of `self.mutex_exclusive.queue`, this is sound.
            // (See the documentation of that field for full details.)
            let this_task = unsafe { this_task.unwrap_unchecked() };
            // SAFETY: By the safety invariant of `self.mutex_exclusive.queue`, `this_task`
            // is the task which we pushed, which is the erased version of `unerased_task`,
            // whose lifetimes were `'t` and `'v`. Trivially, `'t` and `'v` are at least as long
            // as `'t` and `'v`, respectively, so this call is sound.
            let this_task = unsafe { this_task.into_inner::<'t, 'v>() };

            if let Some(this_value) = this_task.value {
                TaskWaitResult::Process(this_value)
            } else if processing_panicked.0 {
                TaskWaitResult::ProcessingPanicked
            } else {
                TaskWaitResult::ProcessedElsewhere
            }
        };

        // Safety and correctness invariants of `self.mutex_exclusive.queue`:
        // If the queue cannot be guaranteed to be in an expected state (due to
        // `try_wait_until_at_front` unwinding), we abort.
        // Robustness guarantees of this function: we do not release exclusive permissions
        // over `front_exclusive` below, and we acquired them above by pushing a task onto the
        // queue, waiting for it to be the front, and popping it, all without unwinding (since we
        // do not catch unwinds in this function, and even escalate them to aborts). As described by
        // `front_exclusive`, this acquires exclusive permissions over `front_exclusive`.
        // Therefore, regardless of whether we unwind or return below, we'd still have exclusive
        // permissions over `front_exclusive`. Note that there are no early returns from this
        // function (though, even if we missed on, `abort_on_drop` would be dropped and prevent
        // unsoundness).
        //
        // If we get here, the task that we pushed onto the queue has been popped. Unwinds above
        // are prohibited by `abort_on_drop`, so this function can return or unwind only if
        // the backing data of `task_state` and `value` are no longer referenced by the queue.
        //
        // Additionally, in the sole place where `guard` is mutated,
        // it is set to a guard of `mutex`.
        #[expect(
            clippy::mem_forget,
            reason = "we need to wait until acquiring `front_exclusive` access \
                        before defusing the destructor of `abort_on_drop`",
        )]
        mem::forget(abort_on_drop);

        // Correctness of other robust guarantees: the only way to return from this function
        // (without unwinding) is to pass through all the blocks, which push a task onto the queue,
        // wait for it to be the front, and pop it, all without unwinding (since we do not catch
        // unwinds in this function, and even escalate them to aborts). As described by
        // `front_exclusive`, this acquires exclusive permissions over `front_exclusive`.
        // Additionally, in the sole place where `guard` is mutated,
        // it is set to a guard of `mutex`.
        (guard, result)
    }

    /// # Robust guarantees
    /// If this function returns *or* unwinds, then exclusive permissions over
    /// `self.front_exclusive` has been acquired. The returned guard is a guard of `mutex`.
    ///
    /// # Safety
    /// `guard` must be the guard of `mutex`, and `self.assert_mutex_good(mutex)`
    /// must have successfully returned.
    ///
    /// No references to the contents of `self.mutex_exclusive` may exist when this function is
    /// called.
    unsafe fn try_process_unchecked<'m, 'v, M, T, R>(
        &self,
        abort_on_drop: AbortIfNotAtFront,
        mutex:         &'m Mutex<M>,
        mut guard:     MutexGuard<'m, M>,
        mut value:     V::Varying<'v>,
        task:          T,
    ) -> (MutexGuard<'m, M>, ProcessResult<R>)
    where
        T: ProcessTask<'v, 'upper, M, FS, V, R>,
    {
        // Correctness invariant of `self.front_exclusive`: if we acquired exclusive permissions
        // over `self.front_exclusive` in `self.try_acquire_front_fast()`, then we don't
        // release them when unwinding here... but we abort, so at least no deadlock occurs, ig.
        // SAFETY:
        // - A `mutex` for which `self.assert_mutex_good(mutex)` successfully returned must be
        //   locked by the current thread, as asserted by the caller.
        // - No other references to the contents of `self.mutex_exclusive` exist when this call to
        //   `self.is_already_front()` is made, since we do not create any such reference above
        //   within this function, and the caller asserts that one did not already exist.
        let acquired_front = unsafe { self.try_acquire_front_fast() };

        if acquired_front {
            // Robustness guarantee of this function: we do not release exclusive permissions
            // over `front_exclusive` below, and we acquired them above (as per the robust
            // guarantee of `try_acquire_front_fast`). Therefore, regardless of whether we unwind
            // or return below, we'd still have exclusive permissions over `front_exclusive`.
            #[expect(
                clippy::mem_forget,
                reason = "we need to wait until acquiring `front_exclusive` access \
                          before defusing the destructor of `abort_on_drop`",
            )]
            mem::forget(abort_on_drop);
        } else {
            // Note: `task_state` is dropped only after `try_wait_until_at_front` returns or
            // unwinds, in which case it is no longer referenced by anything on the queue.
            // (The backing data of `value` is dropped even later.)
            let task_state = TaskState::new();

            // Note that this function ignores poison to avoid panicking. We need to check
            // the panic state ourselves below. It does, however, allow OOM, which is escalated
            // to an abort.

            // Robustness guarantee of this function: we do not release exclusive permissions
            // over `front_exclusive` below, and as per the robust guarantee of
            // `try_wait_until_at_front`, the below
            // we acquired them above (as per the robust
            // guarantee of `try_wait_until_at_front`). Therefore, regardless of whether we unwind
            // or return below, we'd still have exclusive permissions over `front_exclusive`.

            // Correctness: since `!acquired_front`, something else has exclusive permissions
            // over `front_exclusive`.
            // SAFETY: `guard` is the guard of a `mutex` for which
            // `self.assert_mutex_good(mutex)` successfully returned, and no references to
            // the contents of `self.mutex_exclusive` exist when this call is made.
            let (returned_guard, result) = unsafe {
                self.try_wait_until_at_front(abort_on_drop, guard, &task_state, value)
            };

            // Robustness guarantee of this function: we do not release exclusive permissions
            // over `front_exclusive` below, and as per the robust guarantee of
            // `try_wait_until_at_front`, the above method call acquires exclusive permissions
            // over `front_exclusive` (regardless of whether it returns or unwinds).

            // Robustness guarantee of `try_wait_until_at_front` implies that `returned_guard` is a
            // guard of the same mutex as `guard` was (namely, of `mutex`).
            guard = returned_guard;

            if self.options.unwrap_queue_poison {
                // SAFETY: As proven by `guard` and by the caller's assertion, the current thread
                // has a lock for which `self.assert_mutex_good(_)` successfully returned.
                // Additionally, as asserted by the caller, no other references to the contents of
                // `self.mutex_exclusive` exist when `try_process_unchecked` is called, and while
                // above method calls may create transient such references, they are encapsulated
                // within functions which do not leak references to the contents of
                // `self.mutex_exclusive` in their return values. Therefore, `mutex_exclusive` is
                // unique within this block (to which its lifetime is constrained).
                let mutex_exclusive = unsafe { self.mutex_exclusive_mut() };
                assert!(!mutex_exclusive.queue_poisoned, "a ContentionQueue was poisoned");
            }

            // Correctness of robustness guarantee about the returned guard: it is set to a guard
            // of `mutex` above.
            match result {
                TaskWaitResult::Process(this_value) => value = this_value,
                TaskWaitResult::ProcessedElsewhere
                    => return (guard, ProcessResult::ProcessedElsewhere),
                TaskWaitResult::ProcessingPanicked
                    => return (guard, ProcessResult::ProcessingPanicked),
            }
        }

        if self.options.unwrap_mutex_poison {
            assert!(!mutex.is_poisoned(), "the Mutex used for a ContentionQueue was poisoned");
        }

        let mut maybe_uninit_guard = MaybeUninit::new(guard);

        // If we get here, we are at the front of the list, and nobody else has processed our value.
        // Since we process values *strictly* in order, this also implies that all later values
        // should be `Some`. This is taken as a correctness invariant rather than a safety
        // invariant.
        let output = {
            // Correctness: all later values after this task (i.e., all the ones in the queue)
            // should have `Some` values.
            // SAFETY: `&mut maybe_uninit_guard` is a reference to an initialized guard of
            // `mutex`, such that `contention_queue.assert_mutex_good(mutex)` returned successfully,
            // and we pass `&contention_queue.mutex_exclusive`. Lastly, as explained just below,
            // we have exclusive access over `front_exclusive` for the duration of
            // `queue_handle`'s existence, so the backing data of stuff in the queue is protected
            // for at least the duration of `queue_handle`'s existence.
            let queue_handle = unsafe {
                QueueHandle::new(mutex, &mut maybe_uninit_guard, &self.mutex_exclusive)
            };

            // SAFETY: (and safety invariant:) if we get here, either we acquired exclusive access
            // over `self.front_exclusive` in `self.try_acquire_front_fast()` or
            // `self.try_wait_until_at_front(..)`, as per those methods' robust guarantees.
            // We do not release that access in this function.
            let front_exclusive = unsafe {
                unsafe_cell_get_mut_unchecked(&self.front_exclusive)
            };

            task.process(value, &mut front_exclusive.front_state, queue_handle)
        };

        // SAFETY: The robust guarantee of `QueueHandle` implies that `maybe_uninit_guard` is
        // initialized to a guard of `mutex`.
        guard = unsafe { maybe_uninit_guard.assume_init() };

        // We *could* check whether `mutex` is poisoned here, but if we get here, `task.process`
        // finished running without panicking, and it's better to report that result.

        // Correctness of robustness guarantee about the returned guard:
        // it is either set to a guard of `mutex` above, or left unmutated, and the caller asserts
        // that the given guard was a guard of `mutex`.
        (guard, ProcessResult::Processed(output))
    }

    /// # Safety
    /// The `self.assert_mutex_good(mutex)` must have successfully returned.
    ///
    /// # Deadlocks, Panics, Aborts, or other non-termination
    /// This function acquires the `mutex` lock (except inside `queue_handle.unlocked(_)`).
    /// This comes with all the usual threats of deadlocks and other non-termination.
    unsafe fn process_unchecked<'v, M, T, R>(
        &self,
        mutex: &Mutex<M>,
        value: V::Varying<'v>,
        task:  T,
    ) -> ProcessResult<R>
    where
        T: ProcessTask<'v, 'upper, M, FS, V, R>,
    {
        // NOTE: if this panics, that's fine. We have not yet interacted with the queue, so
        // we won't leave any tasks indefinitely asleep.
        let mut guard = mutex.lock_unwrapping_poison(self.options.unwrap_mutex_poison);

        // Regardless of how `catch_unwind` returns, we ensure that we are at the front of the list.
        // That is... if we aren't, we abort :)
        let abort_on_drop = AbortIfNotAtFront;

        // If an unwind occurs, we re-throw, so it doesn't matter whether `V::Varying<'v>`
        // is unwind safe.
        let process_result = match catch_unwind(AssertUnwindSafe(|| {
            // SAFETY:
            // - `guard` is the guard of a `mutex` for which `self.assert_mutex_good(mutex)`
            //   successfully returned, since we acquired it above.
            // - No references to the contents of `self.mutex_exclusive` exist when this function
            //   is called, since we create none above, and if acquiring `guard` succeeds, there
            //   cannot have been any other references to the contents of `self.mutex_exclusive`
            //   (as such references are only permitted to exist while the lock is held, by the
            //   safety invariant).
            unsafe { self.try_process_unchecked(abort_on_drop, mutex, guard, value, task) }
        })) {
            Ok((returned_guard, process_result)) => {
                guard = returned_guard;
                process_result
            }
            Err(panic_payload) => {
                // No need to cause a double-panic by unwrapping poison.
                guard = mutex.lock().unwrap_or_else(PoisonError::into_inner);

                // Wake up the following task, and tell it that the task which may have processed it
                // panicked. (If the task is unprocessed, the panic bit is ignored.)
                {
                    // SAFETY: See above call to `try_process_unchecked`. For the same reason,
                    // no other references to the contents of `self.mutex_exclusive` exist when
                    // this call is made.
                    let mutex_exclusive = unsafe { self.mutex_exclusive_mut() };

                    mutex_exclusive.queue_poisoned = true;

                    if let Some(next_front_task) = mutex_exclusive.queue.front() {
                        // SAFETY: The only time that something is pushed into the queue
                        // is in `try_wait_until_at_front`. The `'t` and `'v` lifetimes of the
                        // task pushed onto the queue in that method (necessarily) outlive the
                        // function body itself, and `try_wait_until_at_front` makes a robust
                        // guarantee that it unwinds or returns only if the task it pushed
                        // has been popped. Clearly, it has not yet been popped, and it cannot be
                        // popped from the queue without that task's thread acquiring `mutex`.
                        // Since, for the duration of this block, we hold a guard of `mutex`,
                        // it thus follows that `try_wait_until_at_front` cannot return or unwind
                        // for the duration of this block (noting that an abort in it would not
                        // lead to unsoundness, and does not even call the panic handler), and thus
                        // the backing data of `next_front_task` outlives this block.
                        // In other words... the `'t` and `'v` lifetimes of the task from which
                        // the erased `next_front_task` was created outlive the `'_` and `'_`
                        // lifetimes provided to `inner` (which need only last within this block).
                        //
                        // TLDR: The backing data is protected by `mutex`.
                        let next_front_task = unsafe { next_front_task.inner() };

                        // Safety invariant of `front_exclusive`: as per the robust guarantee
                        // of `try_process_unchecked`, as of when we reached the `Err(_)` branch
                        // above, this thread holds exclusive permissions over `front_exclusive`.
                        // We have not since released or transferred those permissions above;
                        // therefore, we have the right to transfer them here. (By the correctness
                        // invariant, we are in fact obligated to transfer them here.)
                        // SAFETY:
                        // The sole calls to `task_state`'s unsafe methods are:
                        // - `task_state.wait_until_at_front(guard)` in `try_wait_until_at_front`,
                        // - `next_front_task.state.wake_front_task()` in `self.process_unchecked`,
                        // - `next_front_task.state.wake_front_task_panicking()` here.
                        // During all three calls, we hold a `guard` of a `mutex` for which
                        // `self.assert_mutex_good(mutex)` successfully returned, so access to the
                        // task is synchronized across threads by a lock. This also fulfills the
                        // safety invariant of `mutex_exclusive` that extends to tasks pushed
                        // or popped into/from the queue.
                        unsafe {
                            next_front_task.state.wake_front_task_panicking();
                        };
                    } else {
                        // Safety invariant of `front_exclusive`: as per the robust guarantee
                        // of `try_process_unchecked`, as of when we reached the `Err(_)` branch
                        // above, this thread holds exclusive permissions over `front_exclusive`.
                        // We have not since released or transferred those permissions above;
                        // therefore, we have the right to release them here. (The correctness
                        // invariant is fulfilled, since we are releasing our exclusive permissions
                        // even on unwind, and we first checked that there's nothing on the queue
                        // to transfer them to.)
                        mutex_exclusive.front_locked = false;
                    }
                }
                drop(guard);

                resume_unwind(panic_payload);
            }
        };

        {
            // SAFETY:
            // - `guard` is the guard of a `mutex` for which `self.assert_mutex_good(mutex)`
            //   successfully returned, since we acquired it above *or* got it back from
            //   `try_process_unchecked`, which has a robust guarantee requiring that the returned
            //   guard is also a guard of `mutex`.
            // - No references to the contents of `self.mutex_exclusive` exist when this function
            //   is called, since we create none above except within functions (which have since
            //   returned without leaking references to the contents of `self.mutex_exclusive` in
            //   their return values), and if acquiring `guard` succeeds, there cannot have been
            //   any other references to the contents of `self.mutex_exclusive` (as such references
            //   are only permitted to exist while the lock is held, by the safety invariant).
            let mutex_exclusive = unsafe { self.mutex_exclusive_mut() };

            if let Some(next_front_task) = mutex_exclusive.queue.front() {
                // SAFETY: Basically the same as the `catch_unwind` `Err` branch.
                //
                // The only time that something is pushed into the queue
                // is in `try_wait_until_at_front`. The `'t` and `'v` lifetimes of the
                // task pushed onto the queue in that method (necessarily) outlive the
                // function body itself, and `try_wait_until_at_front` makes a robust
                // guarantee that it unwinds or returns only if the task it pushed
                // has been popped. Clearly, it has not yet been popped, and it cannot be
                // popped from the queue without that task's thread acquiring `mutex`.
                // Since, for the duration of this block, we hold a guard of `mutex`,
                // it thus follows that `try_wait_until_at_front` cannot return or unwind
                // for the duration of this block (noting that an abort in it would not
                // lead to unsoundness, and does not even call the panic handler), and thus
                // the backing data of `next_front_task` outlives this block.
                // In other words... the `'t` and `'v` lifetimes of the task from which
                // the erased `next_front_task` was created outlive the `'_` and `'_`
                // lifetimes provided to `inner` (which need only last within this block).
                //
                // TLDR: The backing data is protected by `mutex`.
                let next_front_task = unsafe { next_front_task.inner() };

                // Safety invariant of `front_exclusive`:
                // Basically the same as the `catch_unwind` `Err` branch.
                //
                // As per the robust guarantee of `try_process_unchecked`, as of when we reached
                // the `Ok(_)` branch above, this thread holds exclusive permissions over
                // `front_exclusive`. We have not since released or transferred those permissions
                // above (except in the `Err(_)` branch, which diverges, implying that we would not
                // have gotten here). Therefore, we have the right to transfer them here.
                // (By the correctness invariant, we are in fact obligated to transfer them here.)
                // SAFETY:
                // The sole calls to `task_state`'s unsafe methods are:
                // - `task_state.wait_until_at_front(guard)` in `try_wait_until_at_front`,
                // - `next_front_task.state.wake_front_task()` here, and
                // - `next_front_task.state.wake_front_task_panicking()` above.
                // During all three calls, we hold a `guard` of a `mutex` for which
                // `self.assert_mutex_good(mutex)` successfully returned, so access to the task is
                // synchronized across threads by a lock.
                // This also fulfills the safety invariant of `mutex_exclusive` that extends to
                // tasks pushed or popped into/from the queue.
                unsafe {
                    next_front_task.state.wake_front_task();
                };
            } else {
                // Safety invariant of `front_exclusive`:
                // Basically the same as the `catch_unwind` `Err` branch.
                //
                // As per the robust guarantee of `try_process_unchecked`, as of when we reached
                // the `Ok(_)` branch above, this thread holds exclusive permissions over
                // `front_exclusive`. We have not since released or transferred those permissions
                // above (except in the `Err(_)` branch, which diverges, implying that we would not
                // have gotten here). Therefore, we have the right to release them here.
                // (The correctness invariant is fulfilled, since we are releasing our exclusive
                // permissions before returning, and we first checked that there's nothing on the
                // queue to transfer them to.)
                mutex_exclusive.front_locked = false;
            }
        }

        drop(guard);

        process_result
    }

    /// # Panics
    /// May panic if `mutex` is not the same [`Mutex`] used by previous calls to
    /// `self.process(..)`, `self.is_queue_poisoned(_)`, and `self.clear_queue_poison(_)`.
    ///
    /// To be more precise, this function panics if `mutex` is not located at the same address
    /// in memory as previously-used `Mutex`es.
    ///
    /// # Deadlocks, Panics, Aborts, or other non-termination
    /// This function acquires the `mutex` lock (except inside `queue_handle.unlocked(_)`).
    /// This comes with all the usual threats of deadlocks and other non-termination.
    pub fn process<'v, M, T, R>(
        &self,
        mutex: &Mutex<M>,
        value: V::Varying<'v>,
        task:  T,
    ) -> ProcessResult<R>
    where
        T: ProcessTask<'v, 'upper, M, FS, V, R>,
    {
        self.assert_mutex_good(mutex);

        // SAFETY: if this function call occurs, then `self.assert_mutex_good(mutex)`
        // successfully returned.
        unsafe { self.process_unchecked(mutex, value, task) }
    }

    pub fn is_queue_poisoned<M>(&self, mutex: &Mutex<M>) -> bool {
        self.assert_mutex_good(mutex);
        let guard = mutex.lock().unwrap_or_else(PoisonError::into_inner);
        let poisoned = {
            // SAFETY: A mutex for which `self.assert_mutex_good(mutex)` returned successfully
            // is held by this thread for the duration of the returned `mutex_exclusive`
            // borrow. No other references to the contents of `self.mutex_exclusive` exist for the
            // duration of this block, since all such references are only permitted to exist while
            // the lock is held.
            let mutex_exclusive = unsafe { self.mutex_exclusive_mut() };
            mutex_exclusive.queue_poisoned
        };
        drop(guard);
        poisoned
    }

    pub fn clear_queue_poison<M>(&self, mutex: &Mutex<M>) {
        self.assert_mutex_good(mutex);

        let guard = mutex.lock().unwrap_or_else(PoisonError::into_inner);
        {
            // SAFETY: A mutex for which `self.assert_mutex_good(mutex)` returned successfully
            // is held by this thread for the duration of the returned `mutex_exclusive`
            // borrow. No other references to the contents of `self.mutex_exclusive` exist for the
            // duration of this block, since all such references are only permitted to exist while
            // the lock is held.
            let mutex_exclusive = unsafe { self.mutex_exclusive_mut() };
            mutex_exclusive.queue_poisoned = false;
        };
        drop(guard);
    }
}

impl<FS, V: AdHocCovariantFamily> Debug for ContentionQueue<'_, FS, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ContentionQueue").finish_non_exhaustive()
    }
}

// We implement poisoning by default, so it seems reasonable to indicate that this type
// is `UnwindSafe` and `RefUnwindSafe`.
impl<FS, V: AdHocCovariantFamily> UnwindSafe for ContentionQueue<'_, FS, V> {}
impl<FS, V: AdHocCovariantFamily> RefUnwindSafe for ContentionQueue<'_, FS, V> {}

/// Expose access to parts of a [`ContentionQueue`] to a [`ProcessTask`] callback.
pub(crate) struct QueueHandle<'q, 'm, 'upper, MutexState, Value: AdHocCovariantFamily> {
    /// # Safety invariant
    /// `contention_queue.assert_mutex_good(self.mutex)` must have returned successfully, such that
    /// it is sound to access `self.mutex_exclusive` while holding a guard of `self.mutex`.
    mutex:               &'m Mutex<MutexState>,
    /// # Safety invariant
    /// This field must always be a reference to an initialized guard of `self.mutex`, *except* in
    /// `self.unlocked(_)`. "Except inside `self.unlocked(_)`" is meant strictly, and all means of
    /// exiting `self.unlocked(_)` (i.e. returning or unwinding) should ensure that `self.guard` is
    /// an initialized guard of `mutex`.
    ///
    /// This safety invariant ensures that the robust guarantee of `Self::new` is satisfied.
    guard:               &'q mut MaybeUninit<MutexGuard<'m, MutexState>>,
    /// # Correctness invariant
    /// We should process queued values in strict FIFO order.
    ///
    /// # Safety invariant
    /// Its contents may only be accessed if a lock used to synchronize this `ContentionQueue`
    /// -- that is, a `mutex` for which `self.assert_mutex_good(mutex)` successfully returned without
    /// panicking -- is currently held. Note that in some places we acquire only shared rather
    /// than exclusive access over this field; standard aliasing rules apply, as though there
    /// were no `UnsafeCell` (while the `mutex` is held).
    mutex_exclusive:     &'q UnsafeCell<MutexExclusive<'upper, Value>>,
    /// # Correctness invariant
    /// Everything in `self.mutex_exclusive.queue.get(..self.next_idx)` should have `None` values
    /// (i.e., should have already been popped), and everything in
    /// `self.mutex_exclusive.queue.get(self.next_idx..)` should have `Some(_)` values.
    ///
    /// Note that this is not a safety invariant, since it is easily asserted.
    next_idx:            usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'q, 'm: 'q, 'upper, M, V: AdHocCovariantFamily> QueueHandle<'q, 'm, 'upper, M, V> {
    /// # Robust guarantee
    /// Whenever the given `guard` is once again usable by whatever code passed in the guard
    /// reference to this function, it will be a reference to an initialized guard of `mutex`,
    /// regardless of whether `QueueHandle` is dropped, leaked, deallocated, has one of its
    /// methods panic, etc.
    ///
    /// # Correctness
    /// Should only be called if all tasks in the queue have `Some(_)` items. Since values
    /// are popped in a strictly FIFO order by a `QueueHandle`, it suffices to ensure that the
    /// current task is unprocessed.
    ///
    /// # Safety
    /// `guard` must be a reference to an initialized guard of `mutex`, and
    /// `contention_queue.assert_mutex_good(mutex)` must have returned successfully, such that
    /// it is sound to access `mutex_exclusive` while holding a guard of `mutex`.
    /// `mutex_exclusive` must be `&contention_queue.mutex_exclusive`.
    ///
    /// No references to the contents of `mutex_exclusive` should exist when this function
    /// is called.
    ///
    /// For at least lifetime `'q`, the current task/thread should have exclusive permissions
    /// over `contention_queue.front_exclusive`, such that no other task can become the front
    /// task for at least lifetime `'q`, implying that the backing data of elements in the queue
    /// will remain valid for at least lifetime `'q`.
    #[inline]
    #[must_use]
    const unsafe fn new(
        mutex:           &'m Mutex<M>,
        guard:           &'q mut MaybeUninit<MutexGuard<'m, M>>,
        mutex_exclusive: &'q UnsafeCell<MutexExclusive<'upper, V>>,
    ) -> Self {
        Self {
            // Safety invariant: asserted by caller.
            mutex,
            // Safety invariant: asserted by caller.
            guard,
            // Safety invariant: asserted by caller.
            mutex_exclusive,
            // Correctness invariant: asserted by caller.
            next_idx: 0,
        }
    }

    pub fn peek<'s>(&'s self) -> Option<&'s V::Varying<'q>> {
        // SAFETY: As per the safety invariant of `self.guard`, we hold an initialized
        // guard of `self.mutex`. As per the safety condition of `Self::new`, there were no
        // references to the contents of `self.mutex_exclusive` when the `QueueHandle` was created.
        // Additionally, `contention_queue.assert_mutex_good(self.mutex)` returned successfully, and
        // `mutex_exclusive` is `&contention_queue.mutex_exclusive`. Therefore, by the extensive
        // reasoning of `ContentionQueue::assert_mutex_good`, we need only uphold the aliasing
        // rules among the references that *we* create, *so long as we hold a guard of `mutex`*,
        // which at the very least means that we can only return references that live at most
        // as long as `&self` (to ensure that `guard` is kept initialized and not dropped).
        //
        // Since the below call is equivalent to `unsafe { &*self.mutex_exclusive.get() }`,
        // we need to ensure that no mutable references exist to the contents of
        // `self.mutex_exclusive` while this borrow is live. Well, for lifetime `'s`, `self` is
        // immutably borrowed, and the only methods of `self` which mutably borrow the contents of
        // `self.mutex_exclusive` take `&mut self`, and thus cannot be called. Therefore, the
        // aliasing rules are satisfied.
        let mutex_exclusive: &'s MutexExclusive<'upper, V> = unsafe {
            unsafe_cell_get_ref_unchecked(self.mutex_exclusive)
        };

        if let Some(next_task) = mutex_exclusive.queue.get(self.next_idx) {
            // SAFETY: The only time that something is pushed into the queue is in
            // `try_wait_until_at_front`. The `'t` and `'v` lifetimes of the task pushed onto the
            // queue in that method (necessarily) outlive the function body itself, and
            // `try_wait_until_at_front` makes a robust guarantee that it unwinds or returns only
            // if the task it pushed has been popped. Clearly, it has not yet been popped, and it
            // cannot be popped from the queue without becoming the front task. The caller of
            // `Self::new` asserts that that cannot happen for at least lifetime `'q` (which still
            // holds here, noting that we are covariant and not contravariant over `'q`).
            // Therefore, the backing data of `next_task` outlives lifetime `'q` (and thus also
            // outlives a short `'_` limited to this block). In other words... the `'t` and `'v`
            // lifetimes of the task from which the erased `next_task` was created outlive the
            // `'_` and `'q` liftimes, respectively, provided to `inner`.
            //
            // TLDR: The backing data is protected by exclusive access to `front_exclusive`.
            let task = unsafe { next_task.inner::<'s, '_, 'q>() };
            let value = task.value.as_ref();

            #[expect(
                clippy::expect_used,
                reason = "should always succeed, by a correctness invariant of QueueHandle",
            )]
            let value = value
                .expect(
                    "`QueueHandle.next_idx` should be the index of the next task to process, \
                     and unprocessed tasks' values should be `Some(_)`",
                );

            Some(value)
        } else {
            None
        }
    }

    pub fn pop(&mut self) -> Option<V::Varying<'q>> {
        // SAFETY: As explained in `self.peek()`, we need only uphold the aliasing rules
        // among the references we create to the contents of `mutex_exclusive` (so long as those
        // references don't outlive `self`).
        // (Note that `V::Varying<'q>` is protected by reasoning involving `front_exclusive`
        // rather than `mutex_exclusive`, so it's fine that `'q: '_`.)
        //
        // Since the below call is equivalent to `unsafe { &mut *self.mutex_exclusive.get() }`,
        // we need to ensure that no references exist to the contents of
        // `self.mutex_exclusive`. Well, for lifetime `'_`, `self` is exclusively borrowed, so no
        // way is exposed to borrow the contents of `self.mutex_exclusive` (except here) for
        // the duration of lifetime `'_`. Therefore, the aliasing rules are satisfied.
        let mutex_exclusive: &'q mut MutexExclusive<'upper, V> = unsafe {
            unsafe_cell_get_mut_unchecked(self.mutex_exclusive)
        };

        if let Some(next_task) = mutex_exclusive.queue.get_mut(self.next_idx) {
            // SAFETY: Same as `self.peek()`. The backing data is protected by exclusive access to
            // `front_exclusive`, which (as asserted by the caller of `Self::new`) is held
            // for at least lifetime `'q`.
            let value = unsafe { next_task.take::<'_, 'q>() };

            // Since `ErasedTask` is not a `ZST`, the queue needs a nonzero-sized allocation
            // for any nonzero length, and allocations have length at most `isize::MAX`.
            // Therefore, we cannot overflow a `usize`.
            self.next_idx += 1;

            #[expect(
                clippy::expect_used,
                reason = "should always succeed, by a correctness invariant of QueueHandle",
            )]
            let value = value
                .expect(
                    "`QueueHandle.next_idx` should be the index of the next task to process, \
                     and unprocessed tasks' values should be `Some(_)`",
                );

            Some(value)
        } else {
            None
        }
    }

    /// Access the mutex-protected state.
    pub fn mutex_state(&self) -> &M {
        // SAFETY: By the safety invariant of `self.guard`, this field is initialized.
        unsafe { self.guard.assume_init_ref() }
    }

    /// Mutably access the mutex-protected state.
    pub fn mutex_state_mut(&mut self) -> &mut M {
        // SAFETY: By the safety invariant of `self.guard`, this field is initialized.
        unsafe { self.guard.assume_init_mut() }
    }

    /// Temporarily unlock the mutex, execute the provided callback, and then re-lock the mutex.
    ///
    /// If re-locking the mutex fails, the process will be aborted. (Poison errors are ignored,
    /// so an abort should be immensely unlikely.)
    pub fn unlocked<U: FnOnce() -> R, R>(&mut self, with: U) -> R {
        struct ReLock<'q, 'm, MutexState> {
            mutex:               &'m Mutex<MutexState>,
            /// # Safety invariants
            /// Must be a guard of `self.mutex`.
            ///
            /// *May* briefly be initialized during initialization and destruction.
            ///
            /// **Must** be initialized when this type is dropped.
            guard:               &'q mut MaybeUninit<MutexGuard<'m, MutexState>>,
        }

        impl<'q, 'm, M> ReLock<'q, 'm, M> {
            /// Unlock `mutex`, and ensure that the pointee of guard will be restored to a guard
            /// of `mutex` **no matter what**.
            ///
            /// # Robust guarantee
            /// If the returned value is dropped (including during an unwind), then either
            /// the pointee of `guard` will be initialized to a valid guard of `mutex`, or the
            /// process will be aborted.
            ///
            /// # Safety
            /// `guard` must be a reference to an initialized guard of `mutex`.
            #[must_use]
            unsafe fn new(
                mutex:               &'m Mutex<M>,
                guard:               &'q mut MaybeUninit<MutexGuard<'m, M>>,
            ) -> Self {
                let this = Self {
                    mutex,
                    guard,
                };
                // If this somehow panics *before* the mutex becomes unlocked, then the
                // `Drop` implementation of `this` could result in a deadlock or abort. Otherwise,
                // the `Drop` impl would re-lock the mutex and re-initialize `guard`, and possibly
                // panic only after `guard` is initialized.
                // SAFETY: The caller asserts that `guard`, and therefore `this.guard`,
                // is initialized to a guard of `mutex`.
                unsafe {
                    this.guard.assume_init_drop();
                };
                this
            }
        }

        impl<M> Drop for ReLock<'_, '_, M> {
            fn drop(&mut self) {
                // Safety invariant of `self.guard`: one way or another, if this `Drop`
                // impl is exited (and other parts of the program begin to be run),
                // `self.guard` will be initialized. In other words, if initializing
                // `self.guard` fails, it is a robust guarantee that this function will abort
                // the process. (We do our best to print an error message first.)

                // `eprintln` can panic, and we might as well make sure to avoid any other unwinds.
                // `&mut MaybeUninit<MutexGuard<'_, M>>` is not unwind safe. We write it in
                // a single step that should not unwind (though, constructing the guard value to
                // write could unwind).
                let try_abort_with_error_message = catch_unwind(AssertUnwindSafe(|| {

                    // `&mut MaybeUninit<MutexGuard<'_, M>>` is not unwind safe.
                    let try_relock = catch_unwind(AssertUnwindSafe(|| {
                        let guard = self.mutex.lock().unwrap_or_else(PoisonError::into_inner);

                        self.guard.write(guard);
                    }));

                    #[expect(
                        clippy::disallowed_macros,
                        clippy::print_stderr,
                        reason = "this is not a stray debug print, \
                                  and the possibility of a panic is accounted for",
                    )]
                    if let Err(err) = try_relock {
                        let payload = if let Some(string) = err.downcast_ref::<&'static str>() {
                            string
                        } else if let Some(string) = err.downcast_ref::<String>() {
                            string
                        } else {
                            "an unknown Box<dyn Any> payload"
                        };

                        eprintln!(
                            "QueueHandle::unlocked could not relock its mutex due to to a panic, \
                            whose payload is: {payload}",
                        );

                        process::abort();
                    }
                }));

                if try_abort_with_error_message.is_err() {
                    // Give up :3
                    process::abort();
                }
            }
        }

        // The real function body is just the following four lines.

        // SAFETY: By the safety invariant of `self.guard`, it is a reference to an initialized
        // guard of `self.mutex`.
        let re_lock = unsafe { ReLock::new(self.mutex, self.guard) };
        let output = with();
        drop(re_lock);
        output
    }

    pub fn is_queue_poisoned(&self) -> bool {
        // SAFETY: Same as `self.peek()` (though the borrow is even shorter).
        let mutex_exclusive = unsafe {
            unsafe_cell_get_ref_unchecked(self.mutex_exclusive)
        };
        mutex_exclusive.queue_poisoned
    }

    pub fn clear_queue_poison(&mut self) {
        // SAFETY: Same as `self.pop()`.
        let mutex_exclusive = unsafe {
            unsafe_cell_get_mut_unchecked(self.mutex_exclusive)
        };
        mutex_exclusive.queue_poisoned = false;
    }
}

impl<M: Debug, V: AdHocCovariantFamily> Debug for QueueHandle<'_, '_, '_, M, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("QueueHandle")
            .field("mutex_state", self.mutex_state())
            .finish_non_exhaustive()
    }
}

/// One of the tasks submitted to [`ContentionQueue::process`]. Equivalently, if the `value`s
/// submitted to a `ContentionQueue` are seen as "tasks", implementors of this trait are what
/// process those tasks.
///
/// Choose whichever interpretation of this trait's name.
pub(crate) trait ProcessTask<'v, 'upper, MutexState, FrontState, Value, Return>
where
    Value: AdHocCovariantFamily,
{
    fn process<'q>(
        self,
        value:        Value::Varying<'v>,
        front_state:  &'q mut FrontState,
        queue_handle: QueueHandle<'q, '_, 'upper, MutexState, Value>,
    ) -> Return;
}

impl<'v, 'upper, MutexState, FrontState, Value, Return, P>
    ProcessTask<'v, 'upper, MutexState, FrontState, Value, Return>
for P
where
    Value: AdHocCovariantFamily,
    P: for<'q> FnOnce(
        Value::Varying<'v>,
        &'q mut FrontState,
        QueueHandle<'q, '_, 'upper, MutexState, Value>,
    ) -> Return,
{
    #[inline]
    fn process<'q>(
        self,
        value:        Value::Varying<'v>,
        front_state:  &'q mut FrontState,
        queue_handle: QueueHandle<'q, '_, 'upper, MutexState, Value>,
    ) -> Return {
        self(value, front_state, queue_handle)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PanicOptions {
    /// If a thread panics while holding a mutex, other threads are informed of that panic
    /// via mutex poisoning.
    ///
    /// Processing a value with `unwrap_mutex_poison = true` will unwrap mutex poison errors, and
    /// using `unwrap_mutex_poison = false` will silently ignore any poison.
    ///
    /// # Default
    /// Defaults to `true`.
    pub unwrap_mutex_poison:    bool,
    /// If a task in a [`ContentionQueue`] panics, following tasks are informed of the panic
    /// via queue poisoning.
    ///
    /// Processing a value with `unwrap_queue_poison = true` will panic if a preceding task
    /// panicked since the last time `queue.clear_queue_poison()` was called, and using
    /// `propagate_panics = false` will either:
    /// - return [`ProcessResult::ProcessingPanicked`], if the value was being processed by a
    ///   different task which panicked, or
    /// - silently ignore the panic and process the value, if the value had not begun to be
    ///   processed elsewhere yet.
    ///
    /// # Default
    /// Defaults to `true`.
    pub unwrap_queue_poison: bool,
}

impl Default for PanicOptions {
    #[inline]
    fn default() -> Self {
        Self {
            unwrap_mutex_poison: true,
            unwrap_queue_poison: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ProcessResult<R> {
    /// This call to [`ContentionQueue::process`] processed the task.
    Processed(R),
    /// The task has been processed by a different call to [`ContentionQueue::process`].
    ProcessedElsewhere,
    /// The task was being processed by a different call to [`ContentionQueue::process`], but that
    /// call panicked. It is unknown to what extent this task has been processed.
    ProcessingPanicked,
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(super) struct ProcessingPanicked(pub bool);

#[derive(Debug)]
enum TaskWaitResult<T> {
    Process(T),
    ProcessedElsewhere,
    ProcessingPanicked,
}

#[derive(Debug)]
struct AbortIfNotAtFront;

impl Drop for AbortIfNotAtFront {
    fn drop(&mut self) {
        #[expect(
            clippy::disallowed_macros,
            clippy::print_stderr,
            reason = "this is not a stray debug print, \
                      and the possibility of a panic is accounted for",
        )]
        let _maybe_panic_payload = catch_unwind(|| {
            eprintln!(
                "a ContentionQueue::process task unexpectedly panicked \
                 before it could reach the front of the queue",
            );
        });
        process::abort();
    }
}

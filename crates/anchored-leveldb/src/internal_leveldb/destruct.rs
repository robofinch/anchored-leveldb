use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{Arc, MutexGuard},
};

use anchored_vfs::LevelDBFilesystem;

use crate::{
    all_errors::aliases::RwResult,
    typed_bytes::BlockOnWrites,
    utils::UnwrapPoison as _,
};
use crate::{
    contention_queue::{ProcessTask, QueueHandle, VaryingWriteCommand, WriteCommand},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{Close, CloseStatus, FlushWrites},
};
use super::state::{InternalDBState, SharedMutableState};


#[derive(Debug, Clone, Copy)]
struct ProcessNoTasks;

impl<'v, 'upper, MS, FS> ProcessTask<'v, 'upper, MS, FS, VaryingWriteCommand, ()>
for ProcessNoTasks
{
    fn process<'q>(
        self,
        _value:        WriteCommand<'v>,
        _front_state:  &'q mut FS,
        _queue_handle: QueueHandle<'q, '_, 'upper, MS, VaryingWriteCommand>,
    ) {
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Analogous to `self.close(when, block_on_writes)`. However, this function:
    /// - Releases a refcount. (Therefore, for correctness, this function **must** only be
    ///   called shortly before the owned `Arc<Self>` is dropped, and it must not be called twice.)
    /// - Is more careful to wake up any background compaction thread; `self.close` may panic
    ///   without waking up the background thread.
    pub fn close_owned(
        self:            &Arc<Self>,
        when:            Close,
        block_on_writes: BlockOnWrites,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        // NOTE: Since `self.mutable_state` is only briefly acquired within database functions,
        // and those functions should not drop a `DB` or `DBState`, it should not already be
        // acquired. Therefore, locking it and ignoring poison should not panic.
        // After that point, it *should* be impossible for anything to panic until `catch_unwind`.
        // If an early panic does occur, that "just" means that resources are leaked, but we still
        // want to avoid that if possible.
        let mut mut_state = self.mutable_state.lock().unwrap_poison(false);

        match catch_unwind(AssertUnwindSafe(|| {
            if let Some(decremented) = mut_state.non_compactor_arc_refcounts.checked_sub(1) {
                mut_state.non_compactor_arc_refcounts = decremented;

                if decremented == 0 {
                    return self.force_close(mut_state, when, block_on_writes);
                }
            } else {
                // TODO: log that something has gone horribly wrong... the refcount was already `0`.
                // Wake up the compactor, which will see that `non_compactor_arc_refcounts` is `0`
                // and exit.
                if let Some(background_compactor) = &self.background_compactor {
                    background_compactor.start_compaction.notify_one();
                }
            }

            (mut_state.close_status, self.take_write_status(&mut mut_state, true))
        })) {
            Ok(result) => result,
            Err(payload) => {
                if let Some(background_compactor) = &self.background_compactor {
                    background_compactor.start_compaction.notify_one();
                }
                resume_unwind(payload)
            }
        }
    }

    /// A checked alternative to simply dropping this [`DBState`].
    ///
    /// If `self` is the last reference count (excluding any internal reference counts), then this
    /// function will close the database and optionally block until ongoing writes (including
    /// compactions) have stopped before returning.
    /// Note that each database iterator holds a reference count.
    ///
    /// If the database is closed, depending on the given [`Close`] argument, any ongoing
    /// compaction is either terminated as quickly as possible or is permitted to complete.
    /// No additional compactions are permitted. Compactions are resumed (if only briefly) if the
    /// database is closed.
    ///
    /// The [`CloseStatus`] of the database is returned, which is [`CloseStatus::Closed`] if
    /// `self` was the last reference count. Otherwise, if methods like [`force_close`]
    /// are avoided, the result is [`CloseStatus::Open`]. Using [`force_close`] and similar
    /// can result in any [`CloseStatus`] being returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    ///
    /// [`force_close`]: InternalDBState::force_close
    pub fn close(&self,
        when:            Close,
        block_on_writes: BlockOnWrites,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        let mut mut_state = self.lock_mutable_state();

        let last_refcount = mut_state.non_compactor_arc_refcounts == 1;

        if last_refcount {
            // This was the last public-facing reference count.
            self.force_close(mut_state, when, block_on_writes)
        } else {
            (mut_state.close_status, self.take_write_status(&mut mut_state, true))
        }
    }

    /// A checked alternative to simply dropping the [`InternalDBState`].
    ///
    /// The database will not completely close until all reads have stopped, including via
    /// database iterators. If there are no ongoing reads, then this function will optionally block
    /// until ongoing writes (including compactions) have stopped before returning.
    ///
    /// Depending on the given [`Close`] argument, any ongoing compaction is either terminated as
    /// quickly as possible or is permitted to complete. No additional compactions are permitted.
    ///
    /// This function resumes compactions (if only briefly).
    ///
    /// The [`CloseStatus`] of the database is returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    pub fn force_close(
        &self,
        mut_state:       MutexGuard<'_, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        when:            Close,
        block_on_writes: BlockOnWrites,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        // Fix lifetime error; without this rebinding, the `'_` lifetimes of
        // `self` and `mut_state` must be exactly the same.
        let mut mut_state = mut_state;

        mut_state.compaction_state.suspending_compactions = false;

        match mut_state.close_status {
            CloseStatus::Closed => return (
                mut_state.close_status,
                self.take_write_status(&mut mut_state, true),
            ),
            CloseStatus::Closing | CloseStatus::ClosingAfterCompaction => {
                if matches!(when, Close::AsSoonAsPossible) {
                    // Maybe escalate how fast the database will be closed.
                    mut_state.close_status = CloseStatus::Closing;
                }
            }
            CloseStatus::Open => {
                mut_state.close_status = match when {
                    Close::AsSoonAsPossible => CloseStatus::Closing,
                    Close::AfterCompaction  => CloseStatus::ClosingAfterCompaction,
                };
            }
        }

        self.set_compactor_should_lock(&mut_state);

        if mut_state.lockfile_refcount - mut_state.compactor_lockfile_refcounts > 0 {
            // There are ongoing reads. Return now.
            return (mut_state.close_status, self.take_write_status(&mut mut_state, true));
        }

        if matches!(block_on_writes, BlockOnWrites::False) {
            if mut_state.lockfile_refcount == 0 {
                drop(mut_state.lockfile.take());
                mut_state.close_status = CloseStatus::Closed;
            }

            return (mut_state.close_status, self.take_write_status(&mut mut_state, true));
        }

        // Wait for concurrent writes, other than the compactor.
        {
            drop(mut_state);

            // Flush `self.contention_queue`. Once we are at the front, we know that no other
            // (actual) write is behind us in the queue, since anything behind us would've acquired
            // the mutex *after* we dropped it above, which is strictly after (in the atomic sense)
            // `self.close_status.set(CloseStatus::Closing)`. Therefore, those writers would've
            // seen that the database is closing; only flush operations inserted in other
            // invocations of this function may be present. Therefore, we don't need to provide
            // a *real* `ProcessTask` implementation.
            self.contention_queue.process(
                &self.mutable_state,
                WriteCommand::Flush(FlushWrites::ToWriteAheadLog),
                ProcessNoTasks,
            );

            mut_state = self.lock_mutable_state();
        };

        // Wake everything up. Whatever the threads are waiting for might never happen.
        if let Some(background_compactor) = &self.background_compactor {
            // Wake up the background compactor, if it was asleep. It needs to eventually be
            // woken up so that it can notice that the database is being closed.
            background_compactor.start_compaction.notify_one();
        }
        self.compaction_finished.notify_all();
        self.resume_compactions.notify_all();

        // Wait for the compactor to finish.
        while mut_state.compaction_state.has_ongoing_compaction {
            mut_state = self.compaction_finished
                .wait(mut_state)
                .unwrap_poison(self.opts.unwrap_poison);
        }

        // We began closing the database, after which point nothing other than the compactor
        // should acquire lockfile refcounts;
        // at an above checkpoint, we confirmed that all existing refcounts were held by the
        // compactor;
        // we waited for the compactor to finish any ongoing compaction (though it might not
        // even be awake);
        // therefore, there should be no reference counts left.
        assert_eq!(
            mut_state.lockfile_refcount,
            0,
            "only the compactor should've had lockfile refcounts, and at rest, it holds 0",
        );

        // We want to drop the lockfile. First, though, we should try to release as many files
        // as possible. We can't easily release our file handles for `LOG`, the current `MANIFEST`,
        // or the current `.log` file... but we can at least clear the table cache.
        // I'm fairly sure that on at least some operating systems, if someone tries to open this
        // LevelDB database after the lockfile has been released but before this database struct
        // has been dropped, then opening the `MANIFEST` might fail... however, it can't cause
        // database corruption. (Moreover, we don't reuse `LOG` files, which is the only file
        // we might continue writing to.)
        // So, clearing the table cache should drop a bunch of `.ldb` (or `.sst`) files.
        // Might as well clear the block cache, too.
        self.mut_opts.block_cache.clear();
        self.mut_opts.table_cache.clear();

        drop(mut_state.lockfile.take());
        mut_state.close_status = CloseStatus::Closed;

        (mut_state.close_status, self.take_write_status(&mut mut_state, true))
    }
}

use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{Arc, mpsc::SyncSender, MutexGuard},
};

use clone_behavior::FastMirroredClone;

use anchored_vfs::{IntoChildFileIterator as _, LevelDBFilesystem};

use crate::{
    database_files::LevelDBFileName,
    memtable::MemtableReader,
    table_file::TableFileBuilder,
    utils::UnwrapPoison as _,
};
use crate::{
    all_errors::{
        aliases::{RwErrorAlias, RwErrorKindAlias},
        types::{RwErrorKind, WriteError},
    },
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{CloseStatus, FileNumber, FlushWrites, Level, NonZeroLevel},
    typed_bytes::{ContinueBackgroundCompaction, InternalKeyTag, OwnedInternalKey, UserKey},
    version::{VersionEdit, VersionSet},
};
use super::state::{InternalDBState, SharedMutableState};


#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub const fn suspend_compactions(
        mut_state: &mut SharedMutableState<FS, Cmp, Policy, Codecs, Pool>,
    ) {
        if matches!(mut_state.close_status, CloseStatus::Open) {
            mut_state.compaction_state.suspending_compactions = true;
        }
    }

    pub fn resume_compactions<'a>(
        &'a self,
        mut mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        decoders:      &mut Codecs::Decoders,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        if mut_state.compaction_state.suspending_compactions {
            mut_state.compaction_state.suspending_compactions = false;
            self.maybe_start_compaction(mut_state, decoders)
        } else {
            mut_state
        }
    }

    /// Check whether there is an ongoing compaction which hasn't been interrupted by
    /// a write error or the database closing.
    #[inline]
    #[must_use]
    const fn has_uninterrupted_ongoing_compaction(
        mut_state: &SharedMutableState<FS, Cmp, Policy, Codecs, Pool>,
    ) -> bool {
        match mut_state.close_status {
            // Either compactions have stopped, or they will soon (possibly without properly
            // completing).
            CloseStatus::Closed | CloseStatus::Closing => false,
            CloseStatus::ClosingAfterCompaction => {
                // The database is closing. No additional compactions are allowed,
                // so if none are ongoing, then compactions have stopped. However, any ongoing
                // compaction isn't interrupted (yet).
                mut_state.compaction_state.has_ongoing_compaction
            }
            CloseStatus::Open => {
                if mut_state.write_status.is_err() {
                    // Either compactions have stopped, or they will soon.
                    false
                } else {
                    // Any ongoing compaction isn't interrupted (yet).
                    mut_state.compaction_state.has_ongoing_compaction
                }
            }
        }
    }

    /// Whether new compactions are allowed to be started (provided that compactions are not
    /// suspended and there is no ongoing compaction).
    #[inline]
    #[must_use]
    const fn new_compactions_permitted(
        mut_state: &SharedMutableState<FS, Cmp, Policy, Codecs, Pool>,
    ) -> bool {
        // Note that `CloseStatus::ClosingAfterCompaction` allows ongoing compactions to finish,
        // but does not allow new ones to start.
        matches!(mut_state.close_status, CloseStatus::Open) && mut_state.write_status.is_ok()
    }

    /// Helper function for waiting for compactions.
    ///
    /// When this function returns, either there is no uninterrupted ongoing compaction,
    /// or `is_done` returned true.
    ///
    /// If compactions are suspended, waiting for them to be resumed takes priority over anything
    /// else. Consider that a compaction might not be started due to compactions being suspended.
    /// When compactions are resumed, the mutex is held until a new compaction is maybe started;
    /// that way, anything waiting for compactions to resume won't spuriously see
    /// `!has_ongoing_compaction` due to the pause in compactions.
    ///
    /// `is_done` should return `true` only if the waited-for compaction has successfully completed.
    /// (It is permitted to spuriously return `true`.)
    fn wait_for_some_compaction<'a, F>(
        &'a self,
        mut mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        mut is_done:   F,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>
    where
        F: FnMut(&mut SharedMutableState<FS, Cmp, Policy, Codecs, Pool>) -> bool,
    {
        loop {
            if mut_state.compaction_state.suspending_compactions {
                // Wait until compaction progress starts to be made again.
                mut_state = self.resume_compactions.wait(mut_state)
                    .unwrap_poison(self.opts.unwrap_poison);
            } else if is_done(&mut mut_state)
                || !Self::has_uninterrupted_ongoing_compaction(&mut_state)
            {
                break;
            } else {
                // Wait for a compaction to finish before checking again.
                mut_state = self.compaction_finished.wait(mut_state)
                    .unwrap_poison(self.opts.unwrap_poison);
            }
        }

        mut_state
    }

    /// When this function returns, there is no ongoing uninterrupted memtable compaction.
    fn wait_for_memtable_compaction<'a>(
        &'a self,
        mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        // Correctness of this function's docs: if this function returns, either there is no
        // ongoing uninterrupted compaction at all, or there's no memtable under compaction
        // (indicating the desired result).
        self.wait_for_some_compaction(
            mut_state,
            |state| state.compaction_state.memtable_under_compaction.is_none(),
        )
    }

    /// When this function returns, there is no ongoing uninterrupted manual compaction.
    fn wait_for_any_manual_compaction<'a>(
        &'a self,
        mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        // Correctness of this function's docs: if this function returns, either there is no
        // ongoing uninterrupted compaction at all, or there's no ongoing manual compaction.
        self.wait_for_some_compaction(
            mut_state,
            |state| state.compaction_state.manual_compaction.level.is_none(),
        )
    }

    /// When this function returns, there is no ongoing uninterrupted manual compaction associated
    /// with the indicated `counter`.
    fn wait_for_manual_compaction<'a>(
        &'a self,
        mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        counter:   u8,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        // Correctness of this function's docs: if this function returns, either there is no
        // ongoing uninterrupted compaction at all, or there's no ongoing manual compaction,
        // or any ongoing manual compaction is not associated with the indicated `counter`.
        self.wait_for_some_compaction(
            mut_state,
            |state| {
                state.compaction_state.manual_compaction.level.is_none()
                    || state.compaction_state.manual_compaction_counter != counter
            },
        )
    }

    /// When this function returns, there is no ongoing uninterrupted compaction.
    fn wait_for_uninterrupted_ongoing_compaction<'a>(
        &'a self,
        mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        // Correctness of this function's docs: if this function returns, either there is no
        // ongoing uninterrupted compaction, or `|_state| false` returned `true`. Obviously, the
        // latter can't happen.
        self.wait_for_some_compaction(mut_state, |_state| false)
    }

    #[must_use]
    const fn should_start_any_compaction(
        &self,
        mut_state: &SharedMutableState<FS, Cmp, Policy, Codecs, Pool>,
    ) -> bool {
        let flush = mut_state.compaction_state.memtable_under_compaction.is_some();
        let manual_compaction = mut_state.compaction_state.manual_compaction.level.is_some();
        let size_compaction = match mut_state.version_set.current().size_compaction() {
            Some(Level::ZERO) => self.opts.compaction.size_compactions.autocompact_level_zero,
            Some(_other)      => self.opts.compaction.size_compactions.autocompact_nonzero_levels,
            None              => false,
        };
        // We only even bother to record seeks if
        // `self.opts.compaction.seek_compactions.seek_autocompactions` is enabled, so no need
        // to check that option here.
        let seek_compaction = mut_state.version_set.current().seek_compaction().is_some();

        let has_compaction_work = flush || manual_compaction || size_compaction || seek_compaction;

        if mut_state.compaction_state.has_ongoing_compaction {
            // Once the ongoing compaction is complete, it will maybe start another.
        } else if !Self::new_compactions_permitted(mut_state) {
            // Do not start a new compaction.
        } else if mut_state.compaction_state.suspending_compactions {
            // Ongoing compactions are permitted to complete, but ongoing ones are not started.
        } else if !has_compaction_work {
            // No compaction work needs to be done.
        } else {
            // Start a compaction.
            return true;
        }

        false
    }

    /// Maybe start a new compaction. This function performs all necessary checks.
    pub fn maybe_start_compaction<'a>(
        &'a self,
        mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        decoders:  &mut Codecs::Decoders,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        if self.should_start_any_compaction(&mut_state) {
            // Correctness: if `should_start_any_compaction` is `true`, then
            // `mut_state.compaction_state.has_ongoing_compaction` must be `false`.
            self.start_any_compaction(mut_state, decoders)
        } else {
            mut_state
        }
    }

    /// # Checks
    /// This function handles `close_status`, `write_status`, and `suspending_compactions`.
    ///
    /// It does not check `has_ongoing_compaction`.
    ///
    /// # Correctness
    /// At minimum, `mut_state.compaction_state.has_ongoing_compaction` must be `false`.
    /// Otherwise, panics, hangs, or other errors may occur.
    fn start_any_compaction<'a>(
        &'a self,
        mut mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        decoders:      &mut Codecs::Decoders,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        if let Some(background_compactor) = &self.background_compactor {
            // Do compaction in the background.
            mut_state.compaction_state.has_ongoing_compaction = true;
            background_compactor.start_compaction.notify_one();
            mut_state
        } else {
            // Correctness: The caller asserted that `has_ongoing_compaction` was `false`,
            // and we checked that `background_compactor` is `None`.
            self.start_any_foreground_compaction(mut_state, decoders)
        }
    }

    /// # Checks
    /// This function handles `close_status`, `write_status`, and `suspending_compactions`.
    ///
    /// It does not check `has_ongoing_compaction`.
    ///
    /// # Correctness
    /// At minimum, `mut_state.compaction_state.has_ongoing_compaction` must be `false`.
    /// Otherwise, hangs or other errors may occur.
    ///
    /// # Panics
    /// `mut_state.foreground_compactor` must be `Some`. It suffices to confirm that
    /// `mut_state.compaction_state.has_ongoing_compaction` is `false` (indicating that no
    /// foreground compactor exists) and that `self.background_compactor` is `None`
    /// (since the database should always have a compactor.)
    fn start_any_foreground_compaction<'a>(
        &'a self,
        mut mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        decoders:      &mut Codecs::Decoders,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        mut_state.compaction_state.has_ongoing_compaction = true;
        // NOTE: From the time we *successfully* call `foreground_compactor.take()` to the time we
        // enter `catch_unwind`, any panic would cause the database to have no compactor and
        // therefore hang. On the path where a `Some` foreground compactor is taken, the only
        // actions taken are:
        // - `expect` confirming that it's `Some` and returning the unwrapped value,
        // - the construction of a closure,
        // - passing the closureinto `catch_unwind`.
        // That just involves moving values around (and some conditional checks), and while that
        // *could* trigger an immediate abort (if it runs out of stack space), it can't trigger a
        // panic.
        // If `expect` triggers a panic, then there's no foreground compaction resources that we
        // need to return.
        #[expect(
            clippy::expect_used,
            reason = "panic documented, and this expect can only fail if this DB has a bad bug \
                      (and having RwErrorKind::Bug or something is not idiomatic)",
        )]
        let mut foreground_compactor = mut_state.foreground_compactor.take()
            .expect("Every anchored_leveldb::DB should always have a compactor");

        match catch_unwind(AssertUnwindSafe(|| {
            // Correctness: we set `mut_state.compaction_state.has_ongoing_compaction`
            // to `true`. We're also careful to set it back to `false` even on unwind.
            self.compaction_work(
                mut_state,
                &mut foreground_compactor.encoders,
                decoders,
                &mut foreground_compactor.table_builder,
            ).0
        })) {
            Ok(returned_mut_state) => {
                mut_state = returned_mut_state;
                mut_state.foreground_compactor = Some(foreground_compactor);
                mut_state.compaction_state.has_ongoing_compaction = false;
            }
            Err(panic_payload) => {
                // Ignore poison. We're panicking anyway.
                let mut relocked_state = self.mutable_state.lock().unwrap_poison(false);
                relocked_state.foreground_compactor = Some(foreground_compactor);
                relocked_state.compaction_state.has_ongoing_compaction = false;
                resume_unwind(panic_payload);
            }
        }

        mut_state
    }

    /// Run the background compactor.
    ///
    /// # Panics or deadlocks
    /// May panic or deadlock if not called from the background compactor thread.
    pub fn background_compaction(
        self:              Arc<Self>,
        mut table_builder: TableFileBuilder<FS::WriteFile, Policy, Pool>,
        mut encoders:      Codecs::Encoders,
        mut decoders:      Codecs::Decoders,
        ready_sender:      SyncSender<()>,
    ) {
        #![expect(
            clippy::expect_used,
            reason = "there's no reason this setup should ever panic, so better to loudly error",
        )]

        let mut mut_state = self.lock_mutable_state();
        let background_compactor = self.background_compactor.as_ref()
            .expect("`background_compaction` is only be called if there's a background compactor");

        ready_sender.send(()).expect("`Self::build` should not have already failed");
        drop(ready_sender);

        match catch_unwind(AssertUnwindSafe(|| {
            loop {
                mut_state = background_compactor.start_compaction
                    .wait(mut_state)
                    .unwrap_poison(self.opts.unwrap_poison);

                if mut_state.non_compactor_arc_refcounts == 0 {
                    // All database handles were dropped without closing, somehow. Since
                    // `DB` or `DBState` should close the database when the last one is dropped,
                    // and their destructors shoul
                    // something has clearly gone wrong.
                    // TODO: log error.
                    break;
                } else if !mut_state.compaction_state.has_ongoing_compaction {
                    // Presumably a spurious wakeup from the condvar. The only times we signal it
                    // are when we start a compaction or close the database.
                    // Continue back to the top.
                } else {
                    // Note: we don't check if compactions are suspended, since we leave that
                    // for `compaction_work` to handle.
                    // Correctness: we know that `mut_state.compaction_state.has_ongoing_compaction`
                    // is `true` in this branch. We're also careful to set it back to `false`
                    // even on unwind.
                    let (returned_mut_state, continue_background) = self.compaction_work(
                        mut_state,
                        &mut encoders,
                        &mut decoders,
                        &mut table_builder,
                    );
                    mut_state = returned_mut_state;
                    match continue_background {
                        ContinueBackgroundCompaction::True => {
                            // Continue back to the top. Note that `compaction_work` is responsible
                            // for ensuring that the database is still open right now.
                        }
                        ContinueBackgroundCompaction::False => break,
                    }
                }
            }

            mut_state
        })) {
            Ok(returned_mut_state) => {
                mut_state = returned_mut_state;
                mut_state.compaction_state.has_ongoing_compaction = false;
            }
            Err(panic_payload) => {
                // Ignore poison. We're panicking anyway.
                mut_state = self.mutable_state.lock().unwrap_poison(false);
                mut_state.compaction_state.has_ongoing_compaction = false;
                resume_unwind(panic_payload);
            }
        }
    }

    #[expect(clippy::type_complexity, reason = "the number of generics is unavoidable")]
    pub fn range_compaction<'a>(
        &'a self,
        decoders:    &mut Codecs::Decoders,
        lower_bound: Option<UserKey<'_>>,
        upper_bound: Option<UserKey<'_>>,
    ) -> Result<
        MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        RwErrorAlias<FS, Cmp, Codecs>,
    > {
        // First, compact the memtable.
        self.flush(FlushWrites::ToTableFile)?;

        // Wait for there to be no ongoing memtable compaction.
        let mut mut_state = self.lock_mutable_state();
        mut_state = self.wait_for_memtable_compaction(mut_state);

        let levels = mut_state.version_set.current()
            .levels_for_range_compaction(&self.opts.cmp, lower_bound, upper_bound);

        for level in levels {
            // Note: at any point, the database could be forcefully closed or a write error could
            // occur. `self.manual_compaction(..)` handles that situation gracefully,
            // so we don't need to check for it.
            mut_state = self.manual_compaction(
                mut_state,
                decoders,
                level,
                lower_bound,
                upper_bound,
            );
        }

        Ok(mut_state)
    }

    /// Start a manual compaction (if more compactions can be started).
    fn manual_compaction<'a>(
        &'a self,
        mut mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        decoders:      &mut Codecs::Decoders,
        dst_level:     NonZeroLevel,
        lower_bound:   Option<UserKey<'_>>,
        upper_bound:   Option<UserKey<'_>>,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        fn set_bound(
            src: Option<UserKey<'_>>,
            tag: InternalKeyTag,
            dst: &mut Option<OwnedInternalKey>,
        ) {
            if let Some(src_key) = src {
                if let Some(dst_key) = dst {
                    src_key.clone_into(&mut dst_key.0);
                    dst_key.1 = tag;
                } else {
                    *dst = Some(OwnedInternalKey(src_key.to_owned(), tag));
                }
            } else {
                *dst = None;
            }
        }

        // Wait for there to be no ongoing manual compaction, and then set the new manual
        // compaction (even if there is a different ongoing compaction).
        // The compactor won't switch to the manual compaction until the ongoing one is done.
        mut_state = self.wait_for_any_manual_compaction(mut_state);

        // If new compactions are prohibited, return without potentially cloning keys below.
        if !Self::new_compactions_permitted(&mut_state) {
            return mut_state;
        }

        // Set the manual compaction, and get the corresponding counter.
        let manual_compaction = &mut mut_state.compaction_state.manual_compaction;

        // Note that the max key tag comes first in the sorted order, and vice versa for the min.
        set_bound(lower_bound, InternalKeyTag::MAX_KEY_TAG, &mut manual_compaction.lower_bound);
        set_bound(upper_bound, InternalKeyTag::MIN_KEY_TAG, &mut manual_compaction.upper_bound);
        manual_compaction.level = Some(dst_level);

        let counter = mut_state.compaction_state.manual_compaction_counter.wrapping_add(1);
        mut_state.compaction_state.manual_compaction_counter = counter;

        // If there's an ongoing compaction, the compactor will maybe start another compaction
        // once the ongoing one is complete.
        if !mut_state.compaction_state.has_ongoing_compaction {
            // Correctness: `mut_state.compaction_state.has_ongoing_compaction` is `false`.
            mut_state = self.start_any_compaction(mut_state, decoders);
        }

        // Wait for our compaction to finish. Note that if there is an ongoing compaction,
        // it should eventually pick up this job (or be interrupted). Also, compaction suspension
        // is waited for first, so even if there technically isn't an ongoing compaction right
        // now, there will be when compactions are resumed (unless the database is closed).
        // If that makes no sense... just keep in mind that the author wrote this entire codebase
        // and is probably neglecting to mention some background information, since everything
        // feels familiar.
        self.wait_for_manual_compaction(mut_state, counter)
    }

    /// The return value indicates whether the background compactor (if any) should exit its
    /// infinite loop. It has no importance for a foreground compactor.
    ///
    /// # Checks
    /// This function handles `close_status`, `write_status`, and `suspending_compactions`.
    ///
    /// It does not check `has_ongoing_compaction`.
    ///
    /// # Correctness
    /// `mut_state.compaction_state.has_ongoing_compaction` must be `true`. When this function
    /// returns **or unwinds**, the caller should set it back to `false` (and foreground compactor
    /// resources should be returned).
    ///
    /// Otherwise, hangs or other errors may occur.
    #[expect(clippy::type_complexity, reason = "only complex due to generics; it's very flat")]
    fn compaction_work<'a>(
        &'a self,
        mut mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        encoders:      &mut Codecs::Encoders,
        decoders:      &mut Codecs::Decoders,
        table_builder: &mut TableFileBuilder<FS::WriteFile, Policy, Pool>,
    ) -> (
        MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        ContinueBackgroundCompaction,
    ) {
        let continue_background_compactions = loop {
            // Wait for compactions to be resumed, if necessary.
            while mut_state.compaction_state.suspending_compactions {
                mut_state = self.resume_compactions.wait(mut_state)
                    .unwrap_poison(self.opts.unwrap_poison);
            }

            if !Self::new_compactions_permitted(&mut_state) {
                break ContinueBackgroundCompaction::False;
            }

            if let Some(memtable) = mut_state.compaction_state.memtable_under_compaction.clone() {
                // Correctness: we are the compactor, so there is no risk of contention
                // causing `compact_memtable` to panic.
                mut_state = self.compact_memtable(
                    mut_state,
                    table_builder,
                    encoders,
                    decoders,
                    &memtable,
                );
            } else {
                let manual_compaction = &mut_state.compaction_state.manual_compaction;

                if let Some(manual_level) = manual_compaction.level {
                    // Get inputs from version_set based on the manual compaction.
                    // Determine the end of the compaction/
                } else {
                    // else, size compaction (if enabled); else, seek compaction (if enabled),
                    // else no compaction. break with ContinueBackgroundCompaction::True;
                }

                // if some compaction has been chosen:
                //    if it's a trivial move: do that
                //    else, a bunch of work has to be done.
                // else, do nothing

                // update the start of the manual compaction, or clear it... idk.
            }

            mut_state = self.garbage_collect_files(mut_state);
        };

        mut_state.compaction_state.has_ongoing_compaction = false;
        self.compaction_finished.notify_all();
        (mut_state, continue_background_compactions)
    }

    /// Flush a memtable to zero or more level-0 table files.
    ///
    /// If the memtable iterator is empty, zero table files are used. Otherwise, table files are
    /// split **only** when absolutely necessary (for the sake of not overfilling the table's index
    /// block), regardless of settings for table file size. (This means that, almost always, at
    /// most one table file is used.)
    ///
    /// # Panics
    /// Only one thread should even *attempt* to call this method at a time. The mutex is
    /// temporarily released during part of this function, and if a different thread also
    /// begins calling `compact_memtable`, a panic could occur.
    ///
    /// Since there is at most one active compactor thread (whether foreground or background),
    /// it suffices to only call this method during compactions.
    fn compact_memtable<'a>(
        &'a self,
        mut mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        table_builder: &mut TableFileBuilder<FS::WriteFile, Policy, Pool>,
        encoders:      &mut Codecs::Encoders,
        decoders:      &mut Codecs::Decoders,
        memtable:      &MemtableReader<Cmp>,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        let mut edit = VersionEdit::new_empty();
        let mut memtable_iter = memtable.iter();

        let level = {
            memtable_iter.seek_to_last();
            // If the memtable is empty, there's nothing to do.
            let Some(last) = memtable_iter.current() else { return mut_state };
            memtable_iter.seek_to_first();
            // If the memtable is empty (which shouldn't be possible here), there's nothing to do.
            let Some(first) = memtable_iter.current() else { return mut_state };

            mut_state.version_set
                .current()
                .level_for_compacted_memtable(&self.opts, first.user_key(), last.user_key())
        };

        let manifest_number = mut_state.version_set.manifest_file_number();

        while let Some(first) = memtable_iter.current() {
            let Ok(table_file_number) = mut_state.version_set.new_file_number() else {
                self.compaction_err(
                    &mut mut_state,
                    RwErrorKind::Write(WriteError::OutOfFileNumbers),
                );
                return mut_state;
            };

            // Unlock the mutex while making the table file... and make sure that it isn't
            // garbage collected while we don't hold the mutex.
            mut_state.compaction_state.pending_compaction_outputs.insert(table_file_number);

            {
                drop(mut_state);

                match table_builder.flush_once(
                    &self.opts,
                    &self.mut_opts,
                    encoders,
                    decoders,
                    manifest_number,
                    table_file_number,
                    level.try_as_nonzero_level(),
                    &mut memtable_iter,
                    first,
                ) {
                    Ok(created) => edit.added_files.push((level, Arc::new(created))),
                    Err(error) => {
                        mut_state = self.lock_mutable_state();
                        self.compaction_err(&mut mut_state, error);
                        return mut_state;
                    }
                }

                mut_state = self.lock_mutable_state();
            };

            mut_state.compaction_state.pending_compaction_outputs.remove(&table_file_number);

            // Note: we could check the `close_status` here in order to theoretically close the
            // database slightly faster. However, we only do one loop in practice either way.
        }

        if matches!(mut_state.close_status, CloseStatus::Closed | CloseStatus::Closing) {
            // Since it's not `CloseStatus::ClosingAfterCompaction`
            // or `CloseStatus::Open`, we have to throw away the work.
            return mut_state;
        } else if mut_state.write_status.is_err() {
            // Protect `version_set.log_to_manifest`.
            return mut_state;
        } else {
            // Proceed to apply->log->install.
        }

        // Since we're compacting a memtable / write-ahead log, we can discard
        // all previous write-ahead logs.
        edit.prev_log_number = Some(FileNumber(0));
        edit.log_number      = Some(mut_state.version_set.current_log_number());

        // Correctness: The apply->log->install process must not be contended, and must not be
        // performed if the `log` step previously failed. We pass the "lack of contention"
        // requirement on to the caller. Since `mut_state.write_status` is never cleared to `Ok`
        // and we checked that it's currently `Ok`, and since we set it to `Err` if
        // `log_to_manifest` fails, it follows that `log_to_manifest` should not panic.
        // (Though, an unwind/panic could lead to that condition being violated. Whatever, that
        // risk is documented.)
        let log_token = mut_state.version_set.apply(&self.opts.cmp, &mut edit);
        // This is a fun feature of Rust.
        let install_token;
        {
            drop(mut_state);

            match VersionSet::log_to_manifest(log_token) {
                Ok(token) => install_token = token,
                Err(err) => {
                    mut_state = self.lock_mutable_state();
                    self.compaction_err(&mut mut_state, RwErrorKind::Write(err));
                    return mut_state;
                }
            }

            mut_state = self.lock_mutable_state();
        };

        mut_state.version_set.install(install_token, self.opts.compaction.size_compactions);

        mut_state.compaction_state.memtable_under_compaction = None;

        self.garbage_collect_files(mut_state)
    }

    /// Should only be called by the compactor (whether foreground or background).
    fn compaction_err(
        &self,
        mut_state: &mut SharedMutableState<FS, Cmp, Policy, Codecs, Pool>,
        error:     RwErrorKindAlias<FS, Cmp, Codecs>,
    ) {
        if let Err(err) = &mut mut_state.write_status {
            err.merge_worst_error(error);
        } else {
            mut_state.write_status = Err(error);
        }

        // Wake everything up (except the compactor, since we *are* the compactor).
        // Due to the error, whatever the threads are waiting for might never happen.
        self.compaction_finished.notify_all();
        self.resume_compactions.notify_all();
    }

    pub fn garbage_collect_files<'a>(
        &'a self,
        mut mut_state: MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
    ) -> MutexGuard<'a, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        if mut_state.write_status.is_err() {
            // After a write or corruption error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return mut_state;
        }

        let live_table_files = mut_state.version_set.live_table_files();
        // Applies to `MANIFEST-` and `.dbtmp` files (and, together with `live_table_files`, to
        // `.ldb` and `.sst` files).
        let is_pending = |file_number| {
            mut_state.compaction_state.pending_compaction_outputs.contains(&file_number)
        };
        // Applies to `.ldb` and `.sst` files.
        let table_is_live = |file_number| {
            live_table_files.contains(&file_number) || is_pending(file_number)
        };

        let Ok(db_files) = self.mut_opts.filesystem.child_files(&self.opts.db_directory) else {
            // Ignore error; garbage collecting files is not critical.
            return mut_state;
        };

        let mut files_to_delete = Vec::new();

        for file in db_files.child_files() {
            // Ignore error, as above.
            let Ok((file_name, _file_size)) = file else { continue };
            // All of LevelDB's files' names are ASCII and thus valid UTF-8, so any files
            // with non-UTF-8 names can be ignored.
            let Ok(file_name) = file_name.into_os_string().into_string() else { continue };
            // Only garbage collect LevelDB's files.
            let Some(parsed_name) = LevelDBFileName::parse(&file_name) else { continue };

            match parsed_name {
                LevelDBFileName::Log { file_number } => {
                    if file_number == mut_state.version_set.prev_log_number()
                        && file_number >= mut_state.version_set.current_log_number()
                    {
                        // Keep this write-ahead log
                        continue;
                    }
                }
                LevelDBFileName::Table { file_number: table_number }
                | LevelDBFileName::TableLegacyExtension { file_number: table_number }
                    => {
                        if table_is_live(table_number) {
                            // Keep this live table file
                            continue;
                        }
                    }
                LevelDBFileName::Manifest { file_number } => {
                    // Keep this invocation's current manifest file, any newer manifest file being
                    // created by this invocation, and any newer invocations' manifests
                    // (in case there is a race that allows other database invocations).
                    if file_number >= mut_state.version_set.manifest_file_number() {
                        continue;
                    }
                }
                LevelDBFileName::Temp { file_number } => {
                    // `.dbtmp` files are created while changing the `CURRENT` file, and they are
                    // given file numbers corresponding to `MANIFEST-` files. The file numbers
                    // of pending `MANIFEST-` files are recorded in `pending_outputs`, which we
                    // check here; all other `.dbtmp` files can be deleted.
                    if is_pending(file_number) {
                        continue;
                    }
                }
                LevelDBFileName::Lockfile
                | LevelDBFileName::Current
                | LevelDBFileName::InfoLog
                | LevelDBFileName::OldInfoLog
                    => continue,
            }

            // TODO: log a message about `file_name` being removed.
            files_to_delete.push(file_name);
        }

        // Unblock other threads while deleting files. Even accounting for bugs in Google's leveldb
        // that can allow one file number to be given to two files (of different types), the
        // combination of file types and the file number counter give unique names to LevelDB's
        // files. (Technically, semantically distinct files could have the same name across
        // different database invocations -- say, a writer could crash after creating a file, and
        // a following database invocation could end up overwriting that file -- but the file names
        // are distinct within this invocation of the database.)
        //
        // Therefore, no new files will overwrite existing files, so we can safely delete these
        // existing files without causing a problem.
        drop(mut_state);
        for file_name in files_to_delete {
            let _ignore_err: Result<(), _> = self.mut_opts
                .filesystem
                .remove_file(&self.opts.db_directory.join(file_name));
        }
        self.lock_mutable_state()
    }
}

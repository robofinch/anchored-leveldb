#![expect(unsafe_code, reason = "implement `into_inner` for a type which impls `Drop`")]

use std::{mem::ManuallyDrop, sync::Arc};

use clone_behavior::FastMirroredClone;

use anchored_vfs::LevelDBFilesystem;

use crate::{all_errors::aliases::RwResult, typed_bytes::BlockOnWrites};
use crate::{
    internal_leveldb::{InternalDBState, OpenFinisher, PerHandleState},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{Close, CloseStatus},
};


pub struct DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// For correctness (though not soundness), `close_owned(..)` **must** be called exactly
    /// once on this `Arc`.
    ///
    /// There must be one `non_compactor_arc_refcounts` refcount per [`DB`] or [`DBState`] struct.
    shared:     Arc<InternalDBState<FS, Cmp, Policy, Codecs, Pool>>,
    per_handle: PerHandleState<Codecs::Decoders>,
}

impl<FS, Cmp, Policy, Codecs, Pool> DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    #[inline]
    #[must_use]
    pub(crate) fn db_state(&self) -> &InternalDBState<FS, Cmp, Policy, Codecs, Pool> {
        // Correctness: this function does not expose a way to manipulate any reference counts.
        &self.shared
    }

    #[expect(clippy::type_complexity, reason = "a wrapper struct would be a pointless hassle")]
    #[inline]
    #[must_use]
    pub(crate) fn inner(
        &mut self,
    ) -> (
        &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        &mut PerHandleState<Codecs::Decoders>,
    ) {
        // Correctness: this function does not expose a way to manipulate any reference counts.
        (&self.shared, &mut self.per_handle)
    }

    #[expect(clippy::type_complexity, reason = "a wrapper struct would be a pointless hassle")]
    #[inline]
    #[must_use]
    pub(crate) fn inner_ref(
        &self,
    ) -> (
        &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        &PerHandleState<Codecs::Decoders>,
    ) {
        // Correctness: this function does not expose a way to manipulate any reference counts.
        (&self.shared, &self.per_handle)
    }

    /// For correctness (though not soundness), `close_owned(..)` **must** eventually be called
    /// exactly once on the returned `Arc`.
    ///
    /// It suffices to either immediately put the `Arc` back into a `DB` or `DBState` or to
    /// call `close_owned` on it and drop it.
    #[expect(clippy::type_complexity, reason = "relatively flat type; just has a lot of generics")]
    #[inline]
    #[must_use]
    fn into_inner(
        self
    ) -> (
        Arc<InternalDBState<FS, Cmp, Policy, Codecs, Pool>>,
        PerHandleState<Codecs::Decoders>,
    ) {
        // Note that this is a common way to implement `into_inner`.
        let this = ManuallyDrop::new(self);

        let this_shared = &raw const this.shared;
        let this_per_handle = &raw const this.per_handle;

        // SAFETY:
        // `this_shared` is valid for reads because:
        // - it's not a null pointer (since it's inbounds of a Rust allocation)
        // - it's dereferenceable for the type of `this.shared`, since it points to a Rust
        //   allocation large enough to store the `this.shared` value.
        // - this does not race with any write, since we have exclusive ownership over `self`
        // - we do not interleave accesses with pointers and references
        // It's also properly aligned for the type of `this.shared`,
        // since `Self` is not `repr(packed)`.
        // Lastly, it trivially points to a valid value of the type of `this.shared`.
        // Additionally, we avoid a double drop by disarming the destructor of `self` in advance.
        let this_shared = unsafe { this_shared.read() };
        // SAFETY: Same as above, but with `this.per_handle`.
        let this_per_handle = unsafe { this_per_handle.read() };

        // Correctness: responsibility for reference counting is passed to caller. Note that this
        // function should not unwind (it only performs a few moves), so there shouldn't be a leak
        // from disarming the destructor.
        (this_shared, this_per_handle)
    }

    /// Implementation of [`Self::close`].
    pub(super) fn close_impl(self, when: Close) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        // Correctness: we immediately call `close_owned` on the returned `Arc` and drop it
        // immediately after that.
        self.shared.close_owned(when, BlockOnWrites::True)
    }

    /// Get a reference-counted [`DBState`] handle to the database.
    ///
    /// (Use [`Clone::clone`] on `self` to get another `DB` handle.)
    #[inline]
    #[must_use]
    pub(super) fn get_db_state_impl(&self) -> DBState<FS, Cmp, Policy, Codecs, Pool> {
        // Correctness:
        // If acquiring `mut_state` or cloning `shared` panics, nothing happens.
        // If incrementing the refcount panics, then `shared` is dropped, so the state
        // reverts to what it previously was (modulo poison).
        // Nothing after that can panic, and the destructor is armed. At that point, there
        // is again one refcount per `DB` or `DBState` struct.

        let mut mut_state = self.db_state().lock_mutable_state();

        let shared = self.shared.fast_mirrored_clone();

        #[expect(clippy::expect_used, reason = "panic should never realistically happen")]
        {
            mut_state.non_compactor_arc_refcounts = mut_state.non_compactor_arc_refcounts
                .checked_add(1)
                .expect("DBState refcount overflow");
        };

        DBState { shared }
    }

    #[inline]
    #[must_use]
    pub(super) fn into_db_state_impl(self) -> DBState<FS, Cmp, Policy, Codecs, Pool> {
        // Correctness: we immediately place the returned `shared` into a `DBState` struct,
        // so there should be no opportunity for an intervening panic while the destructor
        // is disarmed.
        let shared = self.into_inner().0;
        DBState { shared }
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> DB<FS, Cmp, Policy, Codecs, Pool>
where
    // TODO: Loosen `Send + Sync` requirements
    FS:                         LevelDBFilesystem + Send + Sync + 'static,
    FS::RandomAccessFile:       Send + Sync,
    FS::WriteFile:              Send + Sync,
    FS::Lockfile:               Send,
    FS::Error:                  Send,
    Cmp:                        LevelDBComparator + FastMirroredClone + Send + Sync + 'static,
    Cmp::InvalidKeyError:       Send,
    Policy:                     FilterPolicy + FastMirroredClone + Send + Sync + 'static,
    Policy::Eq:                 CoarserThan<Cmp::Eq>,
    Codecs:                     CompressionCodecs + Send + Sync + 'static,
    Codecs::Encoders:           Send,
    Codecs::Decoders:           Send,
    Codecs::CompressionError:   Send,
    Codecs::DecompressionError: Send,
    Pool:                       BufferPool<PooledBuffer: Send + Sync> + Send + Sync + 'static,
{
    /// # Correctness
    /// The three arguments must be freshly returned from [`InternalDBState::open`].
    pub(super) fn finish_open(
        db_state:   Arc<InternalDBState<FS, Cmp, Policy, Codecs, Pool>>,
        finisher:   OpenFinisher<InternalDBState<FS, Cmp, Policy, Codecs, Pool>>,
        per_handle: PerHandleState<Codecs::Decoders>,
    ) -> Self {
        let mut this = Self {
            shared: db_state,
            per_handle,
        };

        finisher.finish_open(&mut this.inner().1.decoders);

        this
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Drop for DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    fn drop(&mut self) {
        // Correctness: we immediately call `close_owned` on the returned `Arc`. Since this
        // is the destructor of `self`, we know that `self.shared` is dropped afterwards without
        // any other intervening actions on `self.shared`.
        let _ignore_result = self.shared.close_owned(
            Close::AsSoonAsPossible,
            BlockOnWrites::False,
        );
    }
}

pub struct DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// For correctness (though not soundness), `close_owned(..)` **must** be called exactly
    /// once on this `Arc`.
    ///
    /// There must be one `non_compactor_arc_refcounts` refcount per [`DB`] or [`DBState`] struct.
    shared: Arc<InternalDBState<FS, Cmp, Policy, Codecs, Pool>>,
}

impl<FS, Cmp, Policy, Codecs, Pool> DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    #[inline]
    #[must_use]
    pub(crate) fn db_state(&self) -> &InternalDBState<FS, Cmp, Policy, Codecs, Pool> {
        // Correctness: this function does not expose a way to manipulate any reference counts.
        &self.shared
    }

    /// For correctness (though not soundness), `close_owned(..)` **must** eventually be called
    /// on the returned `Arc`.
    ///
    /// It suffices to either immediately put the `Arc` back into a `DB` or `DBState` or to
    /// call `close_owned` on it and drop it.
    #[inline]
    #[must_use]
    fn into_inner(
        self,
    ) -> Arc<InternalDBState<FS, Cmp, Policy, Codecs, Pool>> {
        // Note that this is a common way to implement `into_inner`.
        let this = ManuallyDrop::new(self);

        let this_shared = &raw const this.shared;

        // Correctness: responsibility for reference counting is passed to caller. Note that this
        // function should not unwind (it only performs a few moves), so there shouldn't be a leak
        // from disarming the destructor.
        // SAFETY: Same as above, but with `DBState.state` instead of `DB.state`.
        // Note in particular, I suppose, that `DBState` is not `repr(packed)`.
        unsafe { this_shared.read() }
    }

    /// Implementation of [`Self::close`].
    pub(super) fn close_impl(self, when: Close) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        // Correctness: we immediately call `close_owned` on the returned `Arc` and drop it
        // immediately after that.
        self.shared.close_owned(when, BlockOnWrites::True)
    }

    #[inline]
    #[must_use]
    pub(super) fn into_db_impl(self) -> DB<FS, Cmp, Policy, Codecs, Pool> {
        let per_handle = PerHandleState {
            decoders:     self.shared.opts.codecs.init_decoders(),
            iter_key_buf: Vec::new(),
        };

        // Correctness: we immediately place the returned `shared` into a `DB` struct,
        // so there should be no opportunity for an intervening panic while the destructor
        // is disarmed.
        let shared = self.into_inner();

        DB { shared, per_handle }
    }

    #[inline]
    pub(super) fn clone_impl(&self) -> Self {
        // Correctness:
        // If acquiring `mut_state` or cloning `shared` panics, nothing happens.
        // If incrementing the refcount panics, then `shared` is dropped, so the state
        // reverts to what it previously was (modulo poison).
        // Nothing after that can panic, and the destructor is armed. At that point, there
        // is again one refcount per `DB` or `DBState` struct.

        let mut mut_state = self.shared.lock_mutable_state();

        let shared = self.shared.fast_mirrored_clone();

        #[expect(clippy::expect_used, reason = "panic should never realistically happen")]
        {
            mut_state.non_compactor_arc_refcounts = mut_state.non_compactor_arc_refcounts
                .checked_add(1)
                .expect("DBState refcount overflow");
        };

        Self { shared }
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Drop for DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    fn drop(&mut self) {
        // Correctness: we immediately call `close_owned` on the returned `Arc`. Since this
        // is the destructor of `self`, we know that `self.shared` is dropped afterwards without
        // any other intervening actions on `self.shared`.
        let _ignore_result = self.shared.close_owned(
            Close::AsSoonAsPossible,
            BlockOnWrites::False,
        );
    }
}

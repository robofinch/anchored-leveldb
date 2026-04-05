#![expect(unsafe_code, reason = "implement `into_inner` for a type which impls `Drop`")]

use std::{mem::ManuallyDrop, sync::Arc};

use anchored_vfs::LevelDBFilesystem;

use crate::{
    internal_leveldb::{InternalDBState, PerHandleState},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
};
use super::structs::{DB, DBState};


impl<FS, Cmp, Policy, Codecs, Pool> DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    /// For correctness (though not soundness), the returned `Arc` **must not** be dropped until it
    /// has been put back into a `DB` or `DBState`.
    #[expect(clippy::type_complexity, reason = "relatively flat type; just has a lot of generics")]
    #[inline]
    #[must_use]
    pub(super) fn into_inner(
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

        (this_shared, this_per_handle)
    }
}


impl<FS, Cmp, Policy, Codecs, Pool> DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    /// For correctness (though not soundness), the returned `Arc` **must not** be dropped until it
    /// has been put back into a `DB` or `DBState`.
    #[inline]
    #[must_use]
    pub(super) fn into_inner(self) -> Arc<InternalDBState<FS, Cmp, Policy, Codecs, Pool>> {
        // Note that this is a common way to implement `into_inner`.
        let this = ManuallyDrop::new(self);

        let this_shared = &raw const this.shared;

        // SAFETY: Same as above, but with `DBState.state` instead of `DB.state`.
        // Note in particular, I suppose, that `DBState` is not `repr(packed)`.
        unsafe { this_shared.read() }
    }
}

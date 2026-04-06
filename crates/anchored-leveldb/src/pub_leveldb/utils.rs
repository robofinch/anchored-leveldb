use anchored_vfs::LevelDBFilesystem;

use crate::internal_leveldb::{InternalDBState, PerHandleState};
use crate::pub_traits::{
    cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
    compression::CompressionCodecs,
    pool::BufferPool,
};
use super::structs::DB;


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
        (&self.shared, &mut self.per_handle)
    }
}

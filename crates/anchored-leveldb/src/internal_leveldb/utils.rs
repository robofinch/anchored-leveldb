use std::sync::MutexGuard;

use anchored_vfs::LevelDBFilesystem;

use crate::utils::UnwrapPoison as _;
use crate::pub_traits::{
    cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
    compression::CompressionCodecs,
    pool::BufferPool,
};
use super::state::{InternalDBState, SharedMutableState};


#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + Send,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    #[inline]
    pub fn lock_mutable_state(
        &self,
    ) -> MutexGuard<'_, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        self.mutable_state.lock_unwrapping_poison(self.opts.unwrap_poison)
    }
}

use std::sync::MutexGuard;

use anchored_vfs::LevelDBFilesystem;

use crate::utils::UnwrapPoison as _;
use crate::{
    all_errors::{
        aliases::RwErrorAlias,
        types::RwError,
    },
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
};
use super::state::{InternalDBState, SharedMutableState};


#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    #[inline]
    pub fn lock_mutable_state(
        &self,
    ) -> MutexGuard<'_, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>> {
        self.mutable_state.lock().unwrap_poison(self.opts.unwrap_poison)
    }

    pub fn take_write_status(
        &self,
        mut_state:           &mut MutexGuard<'_, SharedMutableState<FS, Cmp, Policy, Codecs, Pool>>,
        ignore_close_errors: bool,
    ) -> Result<(), RwErrorAlias<FS, Cmp, Codecs>> {
        if let Err(err) = &mut mut_state.write_status {
            if ignore_close_errors && err.is_closed_error() {
                Ok(())
            } else {
                Err(RwError {
                    db_directory: self.opts.db_directory.clone(),
                    kind:         err.replace_with_writes_closed(),
                })
            }
        } else {
            Ok(())
        }
    }
}

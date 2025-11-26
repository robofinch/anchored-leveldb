use std::fmt::{Debug, Formatter, Result as FmtResult};

use tracing::Level as LogLevel;

use anchored_vfs::traits::ReadableFilesystem;

use crate::containers::{FragileRwCell as _, RwCellFamily as _};
use crate::leveldb_generics::{LdbFsCell, LdbLockfile, LevelDBGenerics};


/// Unlock the lockfile when dropped
pub struct FSGuard<LDBG: LevelDBGenerics> {
    pub filesystem: LdbFsCell<LDBG>,
    /// ### Invariant required for logical correctness, though not memory safety
    ///
    /// Aside from in a destructor, the lockfile must be `Some`.
    pub lockfile:   Option<LdbLockfile<LDBG>>,
}

impl<LDBG: LevelDBGenerics> Debug for FSGuard<LDBG>
where
    LDBG::FS: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("FSGuard")
            .field("filesystem", LDBG::RwCell::debug(&self.filesystem))
            .field("lockfile", &"<LOCK file>")
            .finish()
    }
}

impl<LDBG: LevelDBGenerics> Drop for FSGuard<LDBG> {
    fn drop(&mut self) {
        if let Some(lockfile) = self.lockfile.take() {
            // There's not much we can do if unlocking the lockfile fails.
            if let Err(lock_error) = self.filesystem.write().unlock_and_close(lockfile) {
                tracing::event!(LogLevel::DEBUG, "error while unlocking LOCK file: {lock_error}");
            }
        }
    }
}

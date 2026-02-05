use core::error::Error;
use core::fmt::{Display, Formatter, Result as FmtResult};

/// Returned if attempting to acquire a lock would block.
///
/// This error can be returned from the [`try_lock`] method of [`MaybeSyncMutex`], the
/// [`try_read`] and [`try_write`] methods of [`MaybeSyncRwLock`], and the `try_*_ignoring_poison`
/// variants of those methods.
///
/// [`try_lock`]: crate::mutex::MaybeSyncMutex::try_lock
/// [`MaybeSyncMutex`]: crate::mutex::MaybeSyncMutex
#[derive(Debug, Default, Clone, Copy)]
pub struct WouldBlockError;

impl Display for WouldBlockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "trying to acquire a lock failed because the operation would block")
    }
}

impl Error for WouldBlockError {}

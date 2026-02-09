use core::error::Error;
use core::fmt::{Display, Formatter, Result as FmtResult};

/// Returned if attempting to acquire a lock would block (or panic, abort, or similar).
///
/// This error can be returned from the [`try_lock`] method of [`MaybeSyncMutex`], the
/// [`try_read`] and [`try_write`] methods of [`MaybeSyncRwLock`], and the `try_*_ignoring_poison`
/// variants of those methods.
///
/// In particular, it is returned when one of the above methods would block (possibly in a
/// deadlock), would fail to return normally (by panicking, aborting, or similar) due to attempting
/// to acquire a lock on a thread which already holds an incompatible lock (e.g., acquiring a write
/// lock on a thread which holds a read lock), or would fail to return normally due to attempting
/// to acquire a read lock of a [`MaybeSyncRwLock`] when the maximum number of readers had already
/// been reached.
///
/// [`try_lock`]: crate::mutex::MaybeSyncMutex::try_lock
/// [`MaybeSyncMutex`]: crate::mutex::MaybeSyncMutex
/// [`try_read`]: crate::rwlock::MaybeSyncRwLock::try_read
/// [`try_write`]: crate::rwlock::MaybeSyncRwLock::try_write
/// [`MaybeSyncRwLock`]: crate::rwlock::MaybeSyncRwLock
#[derive(Debug, Default, Clone, Copy)]
pub struct WouldBlockError;

impl Display for WouldBlockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "trying to acquire a lock failed because the operation would block (or panic or abort)")
    }
}

impl Error for WouldBlockError {}

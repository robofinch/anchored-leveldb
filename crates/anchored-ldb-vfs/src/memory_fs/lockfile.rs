use std::{cell::RefCell, error::Error as StdError};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    path::{Path, PathBuf},
};

use crate::util_traits::{FSError as _, FSLockError};
use super::error::Error;


#[derive(Default, Debug)]
pub(super) struct Locks(RefCell<Vec<PathBuf>>);

impl Locks {
    /// Create an empty `Locks` struct (with nothing locked).
    #[expect(dead_code, reason = "consistency (impl both `Default` and `new`)")]
    #[inline]
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Attempt to lock the indicated path, succeeding if and only if the path was not
    /// already locked.
    pub(super) fn try_lock(&self, path: PathBuf) -> Result<Lockfile, LockError> {
        let mut inner = self.0.borrow_mut();

        if inner.contains(&path) {
            Err(LockError::AlreadyLocked(path))
        } else {
            inner.push(path.clone());
            Ok(Lockfile::new(path))
        }
    }

    /// Unlock the given lockfile.
    ///
    /// If multiple [`MemoryFS`] or [`Locks`] structs are created, and a [`Lockfile`] is created in
    /// one [`Locks`] struct and is attempted to be unlocked in a different [`Locks`] struct,
    /// then that attempt (or a following attempt with a [`Lockfile`] at the same path) may fail.
    /// This method only fails for such pathological uses.
    ///
    /// [`MemoryFS`]: super::fs::MemoryFS
    pub(super) fn unlock(&self, lockfile: Lockfile) -> Result<(), LockError> {
        let mut inner = self.0.borrow_mut();

        let locked_idx = inner
            .iter()
            .position(|locked_path| locked_path == &lockfile.path);

        if let Some(locked_idx) = locked_idx {
            inner.swap_remove(locked_idx);
            Ok(())
        } else {
            // This should be unreachable, unless you lock a path in one `MemoryFS` or `Locks`
            // struct and try to unlock the resulting `Lockfile` in a *different* `MemoryFS` or
            // `Locks` struct.
            Err(LockError::NotLocked(lockfile.path))
        }
    }
}

#[derive(Debug)]
pub struct Lockfile {
    path: PathBuf,
}

impl Lockfile {
    #[allow(clippy::missing_const_for_fn, reason = "not reachable in `const` contexts")]
    #[inline]
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    #[allow(clippy::missing_const_for_fn, reason = "not reachable in `const` contexts")]
    pub(super) fn inner_path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockError {
    AlreadyLocked(PathBuf),
    NotLocked(PathBuf),
    FSError(Error),
}

impl From<Error> for LockError {
    #[inline]
    fn from(err: Error) -> Self {
        Self::FSError(err)
    }
}

impl Display for LockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::AlreadyLocked(path) => {
                write!(f, "lockfile was already acquired at path {}", path.display())
            }
            Self::NotLocked(path) => {
                write!(
                    f,
                    "attempted to unlock a lockfile at path {}, \
                     but it had not been locked in this MemoryFS",
                    path.display(),
                )
            }
            Self::FSError(err) => {
                write!(
                    f,
                    "filesystem error while attempting to acquire or release a lockfile: {err}",
                )
            }
        }
    }
}

impl StdError for LockError {}

impl FSLockError for LockError {
    #[inline]
    fn is_already_locked(&self) -> bool {
        matches!(self, Self::AlreadyLocked(_))
    }

    #[inline]
    fn is_not_found(&self) -> bool {
        if let Self::FSError(err) = self {
            err.is_not_found()
        } else {
            false
        }
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        if let Self::FSError(err) = self {
            err.is_interrupted()
        } else {
            false
        }
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        if let Self::FSError(err) = self {
            err.is_poison_error()
        } else {
            false
        }
    }
}

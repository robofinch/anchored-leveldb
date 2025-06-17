use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};

use hashbrown::HashSet;

use crate::util_traits::{FSError, FSLockError};

use super::path::NormalizedPathBuf;


#[derive(Default, Debug)]
pub(super) struct Locks(Vec<NormalizedPathBuf>);

impl Locks {
    /// Create an empty `Locks` struct (with nothing locked).
    #[inline]
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Attempt to lock the indicated path, succeeding if and only if the path was not
    /// already locked.
    ///
    /// # Errors
    ///
    /// Returns an `AlreadyLocked` error if the path was already locked.
    pub(super) fn try_lock<FSErr>(
        &mut self,
        path:      NormalizedPathBuf,
    ) -> Result<Lockfile, LockError<FSErr>> {
        if self.0.contains(&path) {
            Err(LockError::AlreadyLocked(path))

        } else {
            self.0.push(path.clone());
            Ok(Lockfile::new(path))
        }
    }

    /// Unlock the given lockfile.
    ///
    /// If multiple `MemoryFS` or [`Locks`] structs are created, and a [`Lockfile`] is created in
    /// one [`Locks`] struct and is attempted to be unlocked in a different [`Locks`] struct,
    /// then that attempt (or a following attempt with a [`Lockfile`] at the same path) may fail.
    /// This method only fails for such pathological uses.
    ///
    /// # Errors
    ///
    /// May return a `NotLocked` error, due to the reason described above.
    pub(super) fn unlock<FSErr>(&mut self, lockfile: Lockfile) -> Result<(), LockError<FSErr>> {
        let locked_idx = self.0
            .iter()
            .position(|locked_path| locked_path == &lockfile.path);

        if let Some(locked_idx) = locked_idx {
            self.0.swap_remove(locked_idx);
            Ok(())
        } else {
            // This should be unreachable, unless you lock a path in one `MemoryFS` or `Locks`
            // struct and try to unlock the resulting `Lockfile` in a *different* `MemoryFS` or
            // `Locks` struct.
            Err(LockError::NotLocked(lockfile.path))
        }
    }
}

impl PartialEq for Locks {
    /// Check whether two `Locks` structs indicate that the same files are locked.
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            false

        } else if self.0.len() <= 1 {
            // Optimize the small case. Probably only going to have 1 lock in most cases,
            // and this also catches 0.
            self.0.first() == other.0.first()

        } else if self.0.len() < 10 {
            // Might as well give one more step up before the `HashSet` solution
            self.0
                .iter()
                .all(|path| other.0.contains(path))

        } else {
            let other = other.0.iter().collect::<HashSet<_>>();

            self.0
                .iter()
                .all(|path| other.contains(path))
        }
    }
}

impl Eq for Locks {}

#[derive(Debug)]
pub struct Lockfile {
    path: NormalizedPathBuf,
}

impl Lockfile {
    #[expect(clippy::missing_const_for_fn, reason = "not reachable in `const` contexts")]
    #[inline]
    fn new(path: NormalizedPathBuf) -> Self {
        Self { path }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LockError<FSErr> {
    AlreadyLocked(NormalizedPathBuf),
    NotLocked(NormalizedPathBuf),
    FSError(FSErr),
}

impl<FSErr> LockError<FSErr> {
    pub fn is_fs_error_and<F: FnOnce(&FSErr) -> bool>(&self, f: F) -> bool {
        if let Self::FSError(err) = self {
            f(err)
        } else {
            false
        }
    }
}

impl<FSErr> From<FSErr> for LockError<FSErr> {
    #[inline]
    fn from(err: FSErr) -> Self {
        Self::FSError(err)
    }
}

impl<FSErr: Display> Display for LockError<FSErr> {
    // Note that `unnecessary_debug_formatting` was added in 1.87.0
    #[rustversion::attr(
        since(1.87.0),
        expect(
            clippy::unnecessary_debug_formatting,
            reason = "Same as below reason for `use_debug`",
        ),
    )]
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        #![expect(
            clippy::use_debug,
            reason = "For error messages, it seems good to show any full, escaped weird paths with \
                      the `Debug` impl, rather than only showing a lossy version with \
                      `path.display()`",
        )]
        match self {
            Self::AlreadyLocked(path) => {
                write!(
                    f,
                    "lockfile was already acquired at path `{}` (Debug format: {:?}) in a MemoryFS",
                    path.display(),
                    path.as_path(),
                )
            }
            Self::NotLocked(path) => {
                write!(
                    f,
                    "attempted to unlock a lockfile at path `{}` (Debug format: {:?}), \
                     but it had not been locked in the MemoryFS",
                    path.display(),
                    path.as_path(),
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

impl<FSErr: StdError> StdError for LockError<FSErr> {}

impl<FSErr: FSError> FSLockError for LockError<FSErr> {
    #[inline]
    fn is_already_locked(&self) -> bool {
        matches!(self, Self::AlreadyLocked(_))
    }

    #[inline]
    fn is_not_found(&self) -> bool {
        self.is_fs_error_and(FSErr::is_not_found)
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        self.is_fs_error_and(FSErr::is_interrupted)
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        self.is_fs_error_and(FSErr::is_poison_error)
    }
}

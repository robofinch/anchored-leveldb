use std::fs;
use std::{error::Error, fs::File, path::Path};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    io::{BufWriter, Error as IoError},
};

use fs4::fs_std::FileExt as FileLockExt;

use crate::util_traits::{FSError as _, FSLockError};
use crate::fs_traits::{ReadableFilesystem, WritableFilesystem};
use super::std_fs_core::{readable_core, writable_core};
use super::std_fs_core::DirectoryChildren;


// ================================================================
//  The filesystem
// ================================================================

/// The standard library's file system.
#[derive(Default, Debug, Clone, Copy)]
pub struct StandardFS;

impl ReadableFilesystem for StandardFS {
    readable_core!();

    type Lockfile  = Lockfile;
    type LockError = LockError;

    fn open_and_lock(&self, path: &Path) -> Result<Self::Lockfile, Self::LockError> {
        let lockfile = File::open(path)?;

        match FileLockExt::try_lock_exclusive(&lockfile) {
            Ok(true)  => Ok(Lockfile(lockfile)),
            Ok(false) => Err(LockError::AlreadyLocked),
            Err(err)  => Err(LockError::Io(err)),
        }
    }

    #[inline]
    fn unlock_and_close(&self, lockfile: Self::Lockfile) -> Result<(), Self::LockError> {
        FileLockExt::unlock(&lockfile.0)?;
        Ok(())
    }
}

impl WritableFilesystem for StandardFS {
    writable_core!();

    fn create_and_lock(
        &self,
        path:       &Path,
        create_dir: bool,
    ) -> Result<Self::Lockfile, Self::LockError> {
        if create_dir {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
        }

        // Open with `create` in order to create the lock if it doesn't exist,
        // and open with `append` in order to avoid overwriting any previous contents of the file.
        let lockfile = File::options()
            .append(true)
            .create(true)
            .open(path)?;

        match FileLockExt::try_lock_exclusive(&lockfile) {
            Ok(true)  => Ok(Lockfile(lockfile)),
            Ok(false) => Err(LockError::AlreadyLocked),
            Err(err)  => Err(LockError::Io(err)),
        }
    }
}

// ================================================================
//  Other structs
// ================================================================

#[derive(Debug)]
pub struct Lockfile(File);

#[derive(Debug)]
pub enum LockError {
    AlreadyLocked,
    Io(IoError),
}

impl From<IoError> for LockError {
    #[inline]
    fn from(err: IoError) -> Self {
        Self::Io(err)
    }
}

impl Display for LockError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::AlreadyLocked => write!(f, "lockfile was already acquired by something else"),
            Self::Io(err)       => write!(f, "error while attempting to acquire lockfile: {err}"),
        }
    }
}

impl Error for LockError {}

impl FSLockError for LockError {
    #[inline]
    fn is_already_locked(&self) -> bool {
        matches!(self, Self::AlreadyLocked)
    }

    #[inline]
    fn is_not_found(&self) -> bool {
        if let Self::Io(err) = self {
            err.is_not_found()
        } else {
            false
        }
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        if let Self::Io(err) = self {
            err.is_interrupted()
        } else {
            false
        }
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        if let Self::Io(err) = self {
            err.is_poison_error()
        } else {
            false
        }
    }
}

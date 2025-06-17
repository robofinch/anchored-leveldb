use std::fs;
use std::{error::Error as StdError, fs::File, path::Path};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    io::{BufWriter, Error as IoError, Read, Result as IoResult, Seek, SeekFrom},
    sync::{Arc, Mutex},
};

use crate::error::{MutexPoisoned, Never};
use crate::{
    fs_traits::{ReadableFilesystem, WritableFilesystem},
    util_traits::{FSLockError, RandomAccess, SyncRandomAccess},
};
use super::std_fs_core::{readable_core, writable_core};
use super::std_fs_core::IntoDirectoryIter;


// ================================================================
//  The filesystem
// ================================================================

/// The standard library's file system. Does not support locking (`StandardFS` only supports
/// lockfiles on Unix or Windows).
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardFS;

impl ReadableFilesystem for StandardFS {
    readable_core!();

    type Lockfile  = Never;
    type LockError = LockfileUnsupported;

    #[inline]
    fn open_and_lock(&mut self, _path: &Path) -> Result<Self::Lockfile, Self::LockError> {
        Err(LockfileUnsupported)
    }

    fn unlock_and_close(&mut self, lockfile: Self::Lockfile) -> Result<(), Self::LockError> {
        match lockfile {}
    }
}

impl WritableFilesystem for StandardFS {
    writable_core!();

    #[inline]
    fn create_and_lock(
        &mut self,
        _path:       &Path,
        _create_dir: bool,
    ) -> Result<Self::Lockfile, Self::LockError> {
        Err(LockfileUnsupported)
    }
}

// ================================================================
//  Other structs
// ================================================================

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockfileUnsupported;

impl Display for LockfileUnsupported {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "lockfiles are unsupported in StandardFS on non-Unix/Windows operating systems")
    }
}

impl StdError for LockfileUnsupported {}

impl FSLockError for LockfileUnsupported {
    #[inline]
    fn is_already_locked(&self) -> bool {
        false
    }

    #[inline]
    fn is_not_found(&self) -> bool {
        false
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        false
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        false
    }
}

// ================================================================
//  Other impls
// ================================================================

impl RandomAccess for File {
    #[inline]
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        let mut file: &File = self;

        // This is not threadsafe.
        file.seek(SeekFrom::Start(offset))?;
        file.read(buf)
    }
}

impl RandomAccess for Arc<Mutex<File>> {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        let mut file = self
            .lock()
            .map_err(MutexPoisoned::from)?;

        file.seek(SeekFrom::Start(offset))?;
        file.read(buf)
    }
}

impl SyncRandomAccess for Arc<Mutex<File>> {}

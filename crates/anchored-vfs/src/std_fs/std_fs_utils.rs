use std::{error::Error as StdError, path::PathBuf};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    fs::{File, ReadDir},
    io::{BufWriter, Error as IoError, Result as IoResult, Write as _},
};

use fs4::fs_std::FileExt as FileLockExt;

use crate::util_traits::{
    FSError as _, FSLockError, IntoChildFileIterator, RandomAccess, WritableFile,
};
use super::std_fs_sys;


#[derive(Debug)]
pub struct Lockfile(File);

impl Lockfile {
    #[inline]
    pub(super) fn new(file: File) -> Result<Self, LockError> {
        match FileLockExt::try_lock_exclusive(&file) {
            Ok(true)  => Ok(Self(file)),
            Ok(false) => Err(LockError::AlreadyLocked),
            Err(err)  => Err(LockError::Io(err)),
        }
    }
}

impl Drop for Lockfile {
    fn drop(&mut self) {
        let _res = FileLockExt::unlock(&self.0);
    }
}

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

impl StdError for LockError {}

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
}

#[derive(Debug)]
pub struct IntoChildFileIter {
    readdir_iter: ReadDir,
}

impl IntoChildFileIter {
    #[expect(
        clippy::missing_const_for_fn,
        reason = "`MemoryFS` cannot be constructed in const contexts",
    )]
    #[inline]
    pub(super) fn new(readdir_iter: ReadDir) -> Self {
        Self {
            readdir_iter,
        }
    }
}

impl IntoChildFileIterator for IntoChildFileIter {
    type IterError = IoError;

    #[inline]
    fn child_files(self) -> impl Iterator<Item = Result<(PathBuf, u64), Self::IterError>> {
        self.readdir_iter
            .filter_map(|dir_entry| {
                let dir_entry = match dir_entry {
                    Ok(dir_entry) => dir_entry,
                    Err(err)      => return Some(Err(err)),
                };
                match dir_entry.metadata() {
                    Ok(dir_entry_meta) => {
                       if dir_entry_meta.is_file() {
                            Some(Ok((
                                dir_entry.file_name().into(),
                                dir_entry_meta.len(),
                            )))
                        } else {
                            None
                        }
                    }
                    Err(err) => Some(Err(err)),
                }
            })
    }
}

impl RandomAccess for File {
    #[inline]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        std_fs_sys::read_at(self, offset, buf)
    }
}

impl WritableFile for BufWriter<File> {
    #[inline]
    fn sync_data(&mut self) -> IoResult<()> {
        self.flush()?;
        self.get_ref().sync_data()?;
        Ok(())
    }
}

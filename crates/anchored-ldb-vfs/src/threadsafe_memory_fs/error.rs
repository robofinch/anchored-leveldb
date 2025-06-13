use std::{error::Error as StdError, io::Error as IoError, path::PathBuf, sync::PoisonError};
use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::util_traits::FSError;


#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Error {
    // TODO: add documentation here as well
    NotFound(PathBuf),
    IsADirectory(PathBuf),
    IsAFile(PathBuf),
    DirectoryExists(PathBuf),
    FileExists(PathBuf),
    NonemptyDirectory(PathBuf),
    FileTooLong(usize),
    TreePartiallyCreated,
    MutexPoisoned,
}

impl<T> From<PoisonError<T>> for Error {
    #[inline]
    fn from(_err: PoisonError<T>) -> Self {
        Self::MutexPoisoned
    }
}

impl From<MutexPoisoned> for Error {
    #[inline]
    fn from(_err: MutexPoisoned) -> Self {
        Self::MutexPoisoned
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NotFound(path) => write!(
                f,
                "no file or directory could be found in a ThreadsafeMemoryFS at path `{}`",
                path.display(),
            ),
            Self::IsADirectory(path) => write!(
                f,
                "expected a file, but found a directory, in a ThreadsafeMemoryFS at path `{}`",
                path.display(),
            ),
            Self::IsAFile(path) => write!(
                f,
                "expected a directory, but found a file, in a ThreadsafeMemoryFS at path `{}`",
                path.display(),
            ),
            Self::DirectoryExists(path) => write!(
                f,
                "a directory unexpectedly existed in a ThreadsafeMemoryFS at path `{}`",
                path.display(),
            ),
            Self::FileExists(path) => write!(
                f,
                "a file unexpectedly existed in a ThreadsafeMemoryFS at path `{}`",
                path.display(),
            ),
            Self::NonemptyDirectory(path) => write!(
                f,
                "a directory, if present, was expected to be empty, \
                 but found a nonempty directory in a ThreadsafeMemoryFS at path `{}`",
                path.display(),
            ),
            Self::FileTooLong(file_len) => write!(
                f,
                "a file's length in bytes ({file_len}) could not fit in a u64 \
                 in a ThreadsafeMemoryFS",
            ),
            Self::TreePartiallyCreated => write!(
                f,
                "a call to ThreadsafeMemoryFS::create_dir_all \
                 failed to create all parent directories",
            ),
            Self::MutexPoisoned => write!(
                f,
                "a mutex was poisoned in a ThreadsafeMemoryFS"
            ),
        }
    }
}

impl StdError for Error {}

impl FSError for Error {
    #[inline]
    fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }

    /// `ThreadsafeMemoryFS` is in-memory, so even if a thread goes to sleep or deadlocks,
    /// no operation is noticeably interrupted. This method always returns `false`.
    #[inline]
    fn is_interrupted(&self) -> bool {
        false
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        matches!(self, Self::MutexPoisoned)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MutexPoisoned;

impl<T> From<PoisonError<T>> for MutexPoisoned {
    #[inline]
    fn from(_err: PoisonError<T>) -> Self {
        Self
    }
}

impl From<MutexPoisoned> for IoError {
    fn from(err: MutexPoisoned) -> Self {
        Self::other(err)
    }
}

impl Display for MutexPoisoned {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "a mutex was poisoned")
    }
}

impl StdError for MutexPoisoned {}

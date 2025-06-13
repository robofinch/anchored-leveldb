use std::{error::Error as StdError, path::PathBuf};
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
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NotFound(path) => write!(
                f,
                "no file or directory could be found in a MemoryFS at path `{}`",
                path.display(),
            ),
            Self::IsADirectory(path) => write!(
                f,
                "expected a file, but found a directory, in a MemoryFS at path `{}`",
                path.display(),
            ),
            Self::IsAFile(path) => write!(
                f,
                "expected a directory, but found a file, in a MemoryFS at path `{}`",
                path.display(),
            ),
            Self::DirectoryExists(path) => write!(
                f,
                "a directory unexpectedly existed in a MemoryFS at path `{}`",
                path.display(),
            ),
            Self::FileExists(path) => write!(
                f,
                "a file unexpectedly existed in a MemoryFS at path `{}`",
                path.display(),
            ),
            Self::NonemptyDirectory(path) => write!(
                f,
                "a directory, if present, was expected to be empty, \
                 but found a nonempty directory in a MemoryFS at path `{}`",
                path.display(),
            ),
            Self::FileTooLong(file_len) => write!(
                f,
                "a file's length in bytes ({file_len}) could not fit in a u64 in a MemoryFS",
            ),
            Self::TreePartiallyCreated => write!(
                f,
                "a call to MemoryFS::create_dir_all failed to create all parent directories",
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

    /// `MemoryFS` is single-threaded and in-memory, so it cannot be interrupted.
    #[inline]
    fn is_interrupted(&self) -> bool {
        false
    }

    /// `MemoryFS` is single-threaded, using no mutexes, so a poison error cannot occur.
    #[inline]
    fn is_poison_error(&self) -> bool {
        false
    }
}

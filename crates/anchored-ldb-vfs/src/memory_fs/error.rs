use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::util_traits::FSError;
use super::path::NormalizedPathBuf;


// TODO: documentation

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error<InnerFileError> {
    NotFound(NormalizedPathBuf),
    // NOTE: this is the path *whose parent* could not be found,
    // not the parent path which could not be found.
    ParentNotFound(NormalizedPathBuf),
    // NOTE: this is the path *whose parent* is a file,
    // not the parent path which is a file.
    ParentIsAFile(NormalizedPathBuf),
    IsADirectory(NormalizedPathBuf),
    IsAFile(NormalizedPathBuf),
    DirectoryExists(NormalizedPathBuf),
    FileExists(NormalizedPathBuf),
    RootDirectory,
    NonemptyDirectory(NormalizedPathBuf),
    MoveIntoSelf(NormalizedPathBuf),
    /// If you manage to build this on a 128-bit (or higher) system and make a file which has a size
    /// of 16 exabytes or more (or, provide a noncomformant [`MemoryFileInner`] implementation
    /// which lies about its length), you can win the "Hypothetical 16 Exabyte File Award"
    /// and receive this error.
    ///
    /// [`MemoryFileInner`]: super::file_inner::MemoryFileInner
    FileTooLong(usize),
    InnerFile(InnerFileError),
}

impl<InnerFileError> Error<InnerFileError> {
    pub fn is_inner_file_err_and<F: FnOnce(&InnerFileError) -> bool>(&self, f: F) -> bool {
        if let Self::InnerFile(err) = self {
            f(err)
        } else {
            false
        }
    }
}

impl<InnerFileError> From<InnerFileError> for Error<InnerFileError> {
    #[inline]
    fn from(err: InnerFileError) -> Self {
        Self::InnerFile(err)
    }
}

impl<InnerFileError: Display> Display for Error<InnerFileError> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // If the lint against `{:?}` gets triggered again, see `LockError` in `lockfile.rs`.
        macro_rules! write_at_path {
            ($err:literal $(,)? $path:expr $(,)?) => {
                write!(
                    f,
                    "{} path `{}` (Debug format: {:?}) in a MemoryFS",
                    $err,
                    $path.display(),
                    $path.as_path(),
                )
            };
        }

        match self {
            Self::NotFound(path) => write_at_path!(
                "no file or directory could be found at" path,
            ),
            Self::ParentNotFound(path) => write_at_path!(
                 "expected a parent directory, but nothing existed, at the parent of" path,
            ),
            Self::ParentIsAFile(path) => write_at_path!(
                "expected a parent directory, but found a file, at the parent of" path,
            ),
            Self::IsADirectory(path) => write_at_path!(
                "expected a file, but found a directory, at" path,
            ),
            Self::IsAFile(path) => write_at_path!(
                "expected a directory, but found a file, at" path,
            ),
            Self::DirectoryExists(path) => write_at_path!(
                "expected no entry, but found a directory, at" path,
            ),
            Self::FileExists(path) => write_at_path!(
                "expected no entry, but found a file, at" path,
            ),
            Self::RootDirectory => write!(
                f,
                "attempted to remove the root directory of a MemoryFS, which is not permitted",
            ),
            Self::NonemptyDirectory(path) => write_at_path!(
                "a directory, if present, was expected to be empty, \
                 but found a nonempty directory at" path,
            ),
            Self::MoveIntoSelf(path) => write_at_path!(
                "attempted to move a directory to a new path inside itself, from old" path,
            ),
            Self::FileTooLong(file_len) => write!(
                f,
                "you win the Hypothetical 16 Exabyte File Award: \
                 a file's length in bytes ({file_len}) could not fit in a u64 in a MemoryFS",
            ),
            Self::InnerFile(err) => write!(
                f,
                "an error occurred in an InnerFile of a MemoryFS: {err}",
            ),
        }
    }
}

impl<InnerFileError: StdError> StdError for Error<InnerFileError> {}

impl<InnerFileError: FSError> FSError for Error<InnerFileError> {
    #[inline]
    fn is_not_found(&self) -> bool {
        #[expect(
            clippy::wildcard_enum_match_arm,
            reason = "This crate controls the enum, and there's no other 'not found' variants",
        )]
        match self {
            Self::NotFound(_)    => true,
            Self::InnerFile(err) => err.is_not_found(),
            _                    => false,
        }
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        self.is_inner_file_err_and(InnerFileError::is_interrupted)
    }

    #[inline]
    fn is_poison_error(&self) -> bool {
        self.is_inner_file_err_and(InnerFileError::is_poison_error)
    }
}

use std::{error::Error as StdError, path::PathBuf};
use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::util_traits::FSError;


#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error<InnerFileError> {
    // TODO: add documentation here as well.
    // Note that all the `PathBuf`s here are normalized.
    NotFound(PathBuf),
    // NOTE: this is the path *whose parent* could not be found,
    // not the parent path which could not be found.
    ParentNotFound(PathBuf),
    // NOTE: this is the path *whose parent* is a file,
    // not the parent path which is a file.
    ParentIsAFile(PathBuf),
    IsADirectory(PathBuf),
    IsAFile(PathBuf),
    DirectoryExists(PathBuf),
    FileExists(PathBuf),
    RootDirectory,
    NonemptyDirectory(PathBuf),
    MoveIntoSelf(PathBuf),
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
        // TODO: use `#[rust_version::attr(.., ..)]` to make `expect(unnecessary_debug_formatting)`
        // only apply on Rust 1.87.0 or higher.
        #![expect(
            clippy::use_debug,
            clippy::unnecessary_debug_formatting,
            reason = "For error messages, it seems better to escape weird paths with \
                      the `Debug` impl rather than performing lossy conversion with path.display()",
        )]

        // Note that the `Debug` implementation of `Path` provides wrapping quotes,
        // so we do not need to provide quotes, at least with current Rust.
        // TODO: make `NormalizedPathBuf`'s Display implementation show both the `Debug`
        // and `Path::display` formatting options.
        match self {
            Self::NotFound(path) => write!(
                f,
                "no file or directory could be found at path {path:?} in a MemoryFS",
            ),
            Self::ParentNotFound(path) => write!(
                f,
                "expected a parent directory, but nothing existed, \
                 at the parent of path {path:?} in a MemoryFS",
            ),
            Self::ParentIsAFile(path) => write!(
                f,
                "expected a parent directory, but found a file, \
                 at the parent of path {path:?} in a MemoryFS",
            ),
            Self::IsADirectory(path) => write!(
                f,
                "expected a file, but found a directory, at path {path:?} in a MemoryFS",
            ),
            Self::IsAFile(path) => write!(
                f,
                "expected a directory, but found a file, at path {path:?} in a MemoryFS",
            ),
            Self::DirectoryExists(path) => write!(
                f,
                "expected no entry, but found a directory, at path {path:?} in a MemoryFS",
            ),
            Self::FileExists(path) => write!(
                f,
                "expected no entry, but found a file, at path {path:?} in a MemoryFS",
            ),
            Self::RootDirectory => write!(
                f,
                "attempted to remove the root directory of a MemoryFS, which is not permitted",
            ),
            Self::NonemptyDirectory(path) => write!(
                f,
                "a directory, if present, was expected to be empty, \
                 but found a nonempty directory at path {path:?} in a MemoryFS",
            ),
            Self::MoveIntoSelf(path) => write!(
                f,
                "attempted to move a directory to a new path inside itself, \
                 from old path {path:?} in a MemoryFS",
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

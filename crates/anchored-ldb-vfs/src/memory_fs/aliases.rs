use std::{cell::RefCell, rc::Rc};
use std::sync::{Arc, Mutex};

use super::{
    error::Error,
    file::MemoryFileWithInner,
    file_inner::MemoryFileInner,
    fs::MemoryFSWithInner,
};


/// Get the `InnerFile` generic parameter of a `MemoryFSWithInner<InnerFile>`,
/// via an associated type.
pub trait GetInner {
    /// The `InnerFile` generic of a `MemoryFSWithInner<InnerFile>`.
    type IF;
}

impl<InnerFile> GetInner for MemoryFSWithInner<InnerFile> {
    type IF = InnerFile;
}


/// The `MemoryFile` type corresponding to a given `MemoryFS` type.
pub type MemoryFSFile<MemoryFSWithInner> = MemoryFileWithInner<<MemoryFSWithInner as GetInner>::IF>;

/// The `Error` type corresponding to a given `MemoryFS` type.
pub type MemoryFSErr<MemoryFSWithInner>  =
    Error<<
        <MemoryFSWithInner as GetInner>::IF
        as
        MemoryFileInner
    >::InnerFileError>;

/// The `Result<T, Err>` type whose error is the [`Error<InnerFileError>`] type corresponding to a
/// given [`MemoryFS`].
///
/// [`Error<InnerFileError>`]: Error
/// [`MemoryFS`]: MemoryFSWithInner
pub type MemoryFSResult<T, MemoryFSWithInner> = Result<T, MemoryFSErr<MemoryFSWithInner>>;

// TODO: documentation for these aliases

pub type ThreadLocalMemoryFS    = MemoryFSWithInner <Rc<RefCell<Vec<u8>>>>;
pub type ThreadLocalMemoryFile  = MemoryFSFile      <ThreadLocalMemoryFS>;
pub type ThreadLocalMemoryFSErr = MemoryFSErr       <ThreadLocalMemoryFS>;

pub type ThreadsafeMemoryFS     = MemoryFSWithInner <Arc<Mutex<Vec<u8>>>>;
pub type ThreadsafeMemoryFile   = MemoryFSFile      <ThreadsafeMemoryFS>;
pub type ThreadsafeMemoryFSErr  = MemoryFSErr       <ThreadsafeMemoryFS>;

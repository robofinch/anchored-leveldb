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
pub trait GetInnerFile {
    /// The `InnerFile` generic of a `MemoryFSWithInner<InnerFile>`.
    type IF;
}

impl<InnerFile> GetInnerFile for MemoryFSWithInner<InnerFile> {
    type IF = InnerFile;
}


/// The `MemoryFile` type corresponding to a given `MemoryFS` type.
pub type MemoryFSFile<MemoryFSWithInner>
    = MemoryFileWithInner<
        <MemoryFSWithInner as GetInnerFile>::IF
    >;

/// The `Error` type corresponding to a given `MemoryFS` type.
pub type MemoryFSErr<MemoryFSWithInner>
    = Error<<
        <MemoryFSWithInner as GetInnerFile>::IF
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


#[cfg(doctest)]
pub mod _compile_fail_tests {
    /// ```compile_fail
    /// use anchored_vfs::memory_fs::ThreadLocalMemoryFS;
    ///
    /// fn is_not_send() -> impl Send {
    ///     ThreadLocalMemoryFS::new()
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// use anchored_vfs::memory_fs::ThreadLocalMemoryFS;
    ///
    /// fn is_not_sync() -> impl Sync {
    ///     ThreadLocalMemoryFS::new()
    /// }
    /// ```
    pub const fn _test_threadsafety() {}
}

#[cfg(all(test, not(tests_with_leaks)))]
mod tests {
    use super::*;

    const fn _this_should_compile() {
        fn outputs_inner_file() -> impl MemoryFileInner {
            let inner: Rc<RefCell<Vec<u8>>> = Rc::default();
            inner
        }

        fn also_outputs_inner_file() -> impl MemoryFileInner {
            let inner: Arc<Mutex<Vec<u8>>> = Arc::default();
            inner
        }
    }

    const fn _test_threadsafety() {
        fn outputs_send_sync() -> impl Send + Sync {
            ThreadsafeMemoryFS::new()
        }
    }
}

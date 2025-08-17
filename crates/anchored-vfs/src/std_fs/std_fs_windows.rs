use std::{fs::File, io::Result as IoResult, os::windows::fs::FileExt};
use std::sync::{Arc, Mutex};

use crate::error::MutexPoisoned;
use crate::util_traits::RandomAccess;


impl RandomAccess for File {
    /// The file cursor does not affect Windows' implementation of `read_at`. However, the
    /// implementation _does_ change the file cursor, but is threadsafe because it does not
    /// depend on the value of the cursor.
    #[inline]
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        FileExt::seek_read(self, buf, offset)
    }
}

impl RandomAccess for Arc<Mutex<File>> {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        let file = self
            .lock()
            .map_err(MutexPoisoned::from)?;

        FileExt::seek_read(&*file, buf, offset)
    }
}

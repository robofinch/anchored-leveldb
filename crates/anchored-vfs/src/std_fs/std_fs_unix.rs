use std::{fs::File, io::Result as IoResult, os::unix::fs::FileExt};
use std::sync::{Arc, Mutex};

use crate::error::MutexPoisoned;
use crate::util_traits::RandomAccess;


impl RandomAccess for File {
    /// The file cursor is not affected by (and does not affect) Unix's [`FileExt::read_at`],
    /// making it threadsafe.
    #[inline]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        FileExt::read_at(self, buf, offset)
    }
}

impl RandomAccess for Arc<Mutex<File>> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        let file = self
            .lock()
            .map_err(MutexPoisoned::from)?;

        FileExt::read_at(&*file, buf, offset)
    }
}

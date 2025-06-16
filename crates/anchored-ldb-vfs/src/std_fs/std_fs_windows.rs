use std::{fs::File, io::Result as IoResult, os::windows::fs::FileExt};
use std::sync::{Arc, Mutex};

use crate::error::MutexPoisoned;
use crate::util_traits::{RandomAccess, SyncRandomAccess};


impl RandomAccess for File {
    #[inline]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        FileExt::seek_read(self, buf, offset)
    }
}

// The file cursor does not affect Windows' `read_at`. It *does* change the file cursor,
// but the only method we expose (i.e. `read_at`) does not depend on the file cursor,
// so that's fine; it's threadsafe.
impl SyncRandomAccess for File {}

impl RandomAccess for Arc<Mutex<File>> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
        let file = self
            .lock()
            .map_err(MutexPoisoned::from)?;

        FileExt::seek_read(&*file, buf, offset)
    }
}

impl SyncRandomAccess for Arc<Mutex<File>> {}

use std::fs;
use std::{fs::File, io::Result as IoResult, os::windows::fs::FileExt, path::Path};


/// See [`fs::create_dir_all`].
///
/// As per <https://ayende.com/blog/202660-B/fsync-ing-a-directory-on-linux-and-not-windows>,
/// we do not need to manually fsync any directories.
pub(super) fn create_dir_all(path: &Path, _sync: bool) -> IoResult<()> {
     fs::create_dir_all(path)
}

/// As per <https://ayende.com/blog/202660-B/fsync-ing-a-directory-on-linux-and-not-windows>,
/// we do not need to manually fsync any directories.
#[expect(clippy::missing_const_for_fn, clippy::unnecessary_wraps, reason = "match interface")]
#[inline]
pub(super) fn sync_dir_after_rename(_parent_path: &Path) -> IoResult<()> {
    Ok(())
}

pub(super) fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
    // The file cursor does not affect Windows' implementation of `read_at`. However, the
    // implementation _does_ change the file cursor, but is threadsafe because it does not
    // depend on the value of the cursor.
    FileExt::seek_read(file, buf, offset)
}

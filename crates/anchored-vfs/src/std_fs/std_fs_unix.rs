use std::fs;
use std::{fs::File, io::Result as IoResult, os::unix::fs::FileExt, path::Path};


/// See [`fs::create_dir_all`]. Additionally, this function optionally syncs the directory
/// entries of created directories.
pub(super) fn create_dir_all(path: &Path, _sync: bool) -> IoResult<()> {
    // TODO: use rustix or libc on unix to sync the directories
    fs::create_dir_all(path)
}

// /// See [`fs::create_dir_all`]. Additionally, this function optionally syncs the directory
// /// entries of created directories.
// pub(super) fn create_dir_all(path: &Path, sync_dir: bool) -> Result<(), IoError> {
//     if sync_dir {
//         let mut num_created_parents: u32 = 0;

//         let mut current_path = path;

//         while let Some(parent) = current_path.parent() {
//             if fs::exists(parent).is_ok_and(|exists| !exists) {
//                 // If we get here, the parent does not exist.
//                 num_created_parents += 1;
//             } else {
//                 // If there's a broken symlink in the path, we'll see an error later.
//                 // Otherwise, the parent exists (and should be a directory, else we'll
//                 // get an error.)
//                 break;
//             }

//             current_path = parent;
//         }

//         fs::create_dir_all(parent_path)?;

//         // Sync the created parents
//         current_path = path;

//         for _ in 0..num_created_parents {
//             #[expect(clippy::expect_used, reason = "See above `while let` loop")]
//             let current_path = current_path
//                 .parent()
//                 .expect("`num_created_parents` is at most the number of parents");
//             // TODO: use rustix or libc on unix to sync the directory
//         }
//     } else {
//         // This is the branch for `!sync_dir`; we don't need to do the tedious stuff
//         // above.
//         fs::create_dir_all(parent_path)?;
//     }
// }

#[expect(clippy::missing_const_for_fn, clippy::unnecessary_wraps, reason = "stub")]
pub(super) fn sync_dir_after_rename(_parent_path: &Path) -> IoResult<()> {
    // TODO: use rustix or libc on unix to sync the directory
    Ok(())
}

pub(super) fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> IoResult<usize> {
    // The file cursor is not affected by (and does not affect) Unix's `FileExt::read_at`,
    // making it threadsafe.
    FileExt::read_at(file, buf, offset)
}

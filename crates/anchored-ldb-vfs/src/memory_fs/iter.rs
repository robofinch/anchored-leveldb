use std::path::PathBuf;

use hashbrown::{HashMap, HashSet};

use crate::{error::Never, util_traits::IntoDirectoryIterator};


// TODO: documentation

#[derive(Debug)]
pub struct IntoDirectoryIter<'a, InnerFile> {
    dir_path:    PathBuf,
    files:       &'a HashMap<PathBuf, InnerFile>,
    directories: &'a HashSet<PathBuf>,
}

impl<'a, InnerFile> IntoDirectoryIter<'a, InnerFile> {
     #[expect(
        clippy::missing_const_for_fn,
        reason = "`MemoryFS` cannot be constructed in const contexts",
    )]
    #[inline]
    pub(super) fn new(
        dir_path:    PathBuf,
        files:       &'a HashMap<PathBuf, InnerFile>,
        directories: &'a HashSet<PathBuf>,
    ) -> Self {
        Self {
            dir_path,
            files,
            directories,
        }
    }
}

impl<InnerFile> IntoDirectoryIterator for IntoDirectoryIter<'_, InnerFile> {
    type DirIterError = Never;

    fn dir_iter(self) -> impl Iterator<Item = Result<PathBuf, Self::DirIterError>> {
        self.files.keys()
            .chain(self.directories.iter())
            .filter_map(move |entry_path| {
                entry_path
                    .strip_prefix(&self.dir_path)
                    .ok()
                    .map(|rel_path| Ok(rel_path.to_owned()))
            })
    }
}

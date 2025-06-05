use std::path::Path;

use crate::{error::Result, leveldb::CustomLevelDB};
use crate::options::{OpenOptionGenerics, OpenOptions};


impl<OOG: OpenOptionGenerics> CustomLevelDB<OOG> {
    pub fn open_with_opts(db_directory: &Path, opts: OpenOptions<OOG>) -> Result<Self> {
        let _ = db_directory;
        let _ = opts;
        todo!()
    }
}

// TODO: this is not mandatory to be a successful LevelDB implementation, but it be useful.
// I'm not going to work on this qiute yet, only laid out a few of the pieces.

use std::path::Path;

use crate::{error::Result, write_batch::WriteBatch};
use crate::{
    iter::{Entries, Keys},
};


// NOTE:


#[derive(Debug)]
pub struct ReadOnlyLevelDB {

}

impl ReadOnlyLevelDB {
    pub fn open<P: AsRef<Path>>(db_directory: P, opts: ReadOnlyOpenOptions) -> Result<Self> {
        let _ = db_directory;
        let _ = opts;
        todo!()
    }

    pub fn close(&mut self) -> Result<()> {
        todo!()
    }
}

impl ReadOnlyLevelDB {
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub fn get_into_buf(&mut self, key: &[u8], value_buf: &mut Vec<u8>) -> Result<bool> {
        todo!()
    }
}

impl ReadOnlyLevelDB {
    pub fn keys(&mut self) -> Result<Keys> {
        todo!()
    }

    pub fn entries(&mut self) -> Result<Entries> {
        todo!()
    }

    // pooled_keys

    // pooled_entries
}

#[derive(Debug, Clone)]
pub struct ReadOnlyOpenOptions {

}

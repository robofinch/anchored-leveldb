use crate::error::Result;
use crate::leveldb::{CustomLevelDB, LevelDBGenerics};


impl<LDBG: LevelDBGenerics> CustomLevelDB<LDBG> {
    pub fn compact_range(&mut self, from: &[u8], to: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn compact_all(&mut self) -> Result<()> {
        todo!()
    }

    pub fn wait_for_compactions(&mut self) -> Result<()> {
        todo!()
    }
}

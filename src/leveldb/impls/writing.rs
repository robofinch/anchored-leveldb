use crate::{error::Result, write_batch::WriteBatch};
use crate::leveldb::{CustomLevelDB, LevelDBGenerics};


impl<LDBG: LevelDBGenerics> CustomLevelDB<LDBG> {
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        todo!()
    }

    // TODO: does this function need ownership over the `WriteBatch`?
    // Could we take `&WriteBatch` and let the user reuse the allocation?
    // rusty-leveldb has it take ownership, so that's why I'm taking ownership here, for now.
    pub fn write_batch(&mut self, batch: WriteBatch, flush: bool) -> Result<()> {
        todo!()
    }

    pub fn flush(&mut self) -> Result<()> {
        todo!()
    }
}

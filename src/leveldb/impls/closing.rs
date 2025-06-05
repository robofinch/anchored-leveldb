use crate::error::Error;
use crate::leveldb::{CustomLevelDB, LevelDBGenerics};


#[derive(Debug, Clone, Copy)]
pub enum CloseSuccess {
    CompletelyClosed,
    ReferenceAlive,
}

impl<LDBG: LevelDBGenerics> CustomLevelDB<LDBG> {
    pub fn close(self) -> Result<CloseSuccess, (Error, Self)> {
        todo!()
    }
}

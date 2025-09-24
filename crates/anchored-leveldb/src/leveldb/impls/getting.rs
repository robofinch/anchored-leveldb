use crate::error::Result;
use crate::leveldb::{CustomLevelDB, LevelDBGenerics};


impl<LDBG: LevelDBGenerics> CustomLevelDB<LDBG> {
    // pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    //     todo!()
    // }

    // pub fn get_into_buf(&mut self, key: &[u8], value_buf: &mut Vec<u8>) -> Result<bool> {
    //     todo!()
    // }

    // pub fn get_at(
    //     &mut self,
    //     key:       &[u8],
    //     snapshot:  Snapshot,
    // ) -> Result<Option<Vec<u8>>> {
    //     todo!()
    // }

    // pub fn get_into_buf_at(
    //     &mut self,
    //     key:       &[u8],
    //     value_buf: &mut Vec<u8>,
    //     snapshot:  Snapshot,
    // ) -> Result<bool> {
    //     todo!()
    // }
}

use crate::{error::Result, Snapshot};
use crate::{
    iter::{Entries, Keys, PooledEntries, PooledKeys},
    leveldb::{CustomLevelDB, LevelDBGenerics},
};


impl<LDBG: LevelDBGenerics> CustomLevelDB<LDBG> {
    pub fn keys(&mut self, snapshot: Option<Snapshot>) -> Result<Keys<LDBG>> {
        todo!()
    }

    pub fn entries(&mut self, snapshot: Option<Snapshot>) -> Result<Entries<LDBG>> {
        todo!()
    }

    pub fn pooled_keys(
        &mut self,
        num_buffers: usize,
        snapshot:    Option<Snapshot>,
    ) -> Result<PooledKeys<LDBG>> {
        todo!()
    }

    pub fn pooled_entries(
        &mut self,
        num_buffers: usize,
        snapshot:    Option<Snapshot>,
    ) -> Result<PooledEntries<LDBG>> {
        todo!()
    }
}

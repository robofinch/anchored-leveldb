use anchored_vfs::LevelDBFilesystem;

use crate::{
    all_errors::aliases::RwResult,
    pub_typed_bytes::FlushWrites,
};
use crate::{
    // contention_queue::{ProcessTask, QueueHandle, VaryingWriteCommand, WriteCommand},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
};
use super::state::InternalDBState;


#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub fn flush(&self, _flush_writes: FlushWrites) -> RwResult<(), FS, Cmp, Codecs> {
        todo!()
    }

    // fn process(&self) {

    // }
}

struct ProcessWrites<'a, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    db_state: &'a InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
}





// put
// put_with
// delete
// delete_with
// write
// write_with
// flush
// get
// get_with

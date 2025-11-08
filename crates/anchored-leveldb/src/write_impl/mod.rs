use crate::{leveldb_generics::LevelDBGenerics, write_batch::WriteBatch};


pub trait DBWriteImpl<LDBG: LevelDBGenerics> {
    // condvar and memtable_needs_compaction atomicbool, or ()
    type Shared;
    // ongoing compaction data (like manual compaction), writer queue, pending compaction outputs,
    // channel sender
    type SharedMutable;

    fn split(self) -> (Self::Shared, Self::SharedMutable);

    // fn initialize(_, _)

    // fn write(_, _, opts, write_batch: &WriteBatch)

    // fn compact_memtable(_, _)

    // fn compact_range(_, _, range)

    // fn compact_full

    // fn maybe_start_compaction(_, _, range)

    // fn pending_compaction_outputs

    // fn wait_for_compaction_to_finish

    // fn start_close

    // fn close_and_wait

    // fn compaction_stats
}

use crate::{leveldb_generics::LevelDBGenerics, write_batch::WriteBatch};


pub trait DBWriter<LDBG: LevelDBGenerics> {
    type Shared;
    type SharedMutable;

    fn split(self) -> (Self::Shared, Self::SharedMutable);

    // fn initialize(_, _)

    // fn write(_, _, opts, write_batch: &WriteBatch)

    // fn compact_range(_, _, range)

    // fn maybe_start_compaction(_, _, range)
}

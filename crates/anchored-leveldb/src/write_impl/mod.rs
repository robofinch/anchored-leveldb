use std::collections::HashSet;

use crate::write_batch::WriteBatch;
use crate::{
    format::{FileNumber, UserKey},
    leveldb_generics::{LdbLockedFullShared, LdbFullShared, LevelDBGenerics},
};


pub(crate) trait DBWriteImpl<LDBG: LevelDBGenerics<WriteImpl = Self>>: Sized {
    // condvar and memtable_needs_compaction atomicbool, or ()
    type Shared;
    // ongoing compaction data (like manual compaction), writer queue, pending compaction outputs,
    // channel sender
    type SharedMutable;

    fn split(self) -> (Self::Shared, Self::SharedMutable);

    // TODO: figure out which methods should acquire the lock themselves.
    // Default to forcing them to acquire the lock, for now.

    fn initialize(shared: LdbLockedFullShared<'_, LDBG>);

    fn write(
        shared:      LdbFullShared<'_, LDBG>,
        options:     (),
        write_batch: &WriteBatch,
    ) -> Result<(), ()>;

    fn compact_memtable(shared: LdbFullShared<'_, LDBG>) -> Result<(), ()>;

    fn compact_range(
        shared:      LdbFullShared<'_, LDBG>,
        lower_bound: Option<UserKey<'_>>,
        upper_bound: Option<UserKey<'_>>,
    ) -> Result<(), ()>;

    fn compact_full(shared: LdbFullShared<'_, LDBG>) -> Result<(), ()>;

    fn maybe_start_compaction(shared: LdbLockedFullShared<'_, LDBG>);

    fn pending_compaction_outputs(shared: LdbFullShared<'_, LDBG>) -> HashSet<FileNumber>;

    // return result?
    fn wait_for_compaction_to_finish(shared: LdbFullShared<'_, LDBG>);

    fn close_writes(shared: LdbFullShared<'_, LDBG>) -> Result<(), ()>;

    fn close_writes_after_compaction(shared: LdbFullShared<'_, LDBG>) -> Result<(), ()>;

    // later, might add compaction statistics
    // fn compaction_statistics(shared: LdbFullShared<'_, LDBG>);
}

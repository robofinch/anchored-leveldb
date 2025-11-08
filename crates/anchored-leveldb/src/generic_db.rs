use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{Fast, MirroredClone, Speed};

use crate::write_impl::DBWriteImpl;
use crate::{
    containers::{DebugWrapper, RwCellFamily},
    db_data::{DBShared, DBSharedMutable},
    leveldb_generics::{LdbContainer, LdbPooledBuffer, LdbRwCell, LevelDBGenerics},
};


pub(crate) struct InnerGenericDB<LDBG: LevelDBGenerics, Writer: DBWriteImpl<LDBG>>(
    LdbContainer<LDBG, (
        DBShared<LDBG, Writer>,
        LdbRwCell<LDBG, DBSharedMutable<LDBG, Writer>>,
    )>
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, Writer: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, Writer> {
    // open
    // open_readonly
    // close_writes
    // close_writes_after_compaction
    // irreversibly_delete_db
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, Writer: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, Writer> {
    // put
    // put_with
    // delete
    // delete_with
    // write
    // write_with
    // flush
    // get
    // get_with
    // iter
    // iter_with
    // snapshot
    // compact_range
    // compact_full
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, Writer: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, Writer> {
    // check_corruption
    // approximate_sizes
    // later: approximate_ram_usage
    // later: compaction_statistics
    // num_files_at_level
    // file_summary_with_text_keys(&self, f) -> FmtResult
    // file_summary_with_numeric_keys(&self, f) -> FmtResult
    // file_summary_with<K>(&self, f, display_key: K) -> FmtResult
    // info_log
}

impl<LDBG: LevelDBGenerics, Writer: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, Writer> {
    #[inline]
    #[must_use]
    fn shared(&self) -> &DBShared<LDBG, Writer> {
        &self.0.0
    }

    #[inline]
    #[must_use]
    fn shared_mutable(&self) -> &LdbRwCell<LDBG, DBSharedMutable<LDBG, Writer>> {
        &self.0.1
    }
}

impl<LDBG: LevelDBGenerics, Writer: DBWriteImpl<LDBG>> Clone for InnerGenericDB<LDBG, Writer>
where
    Self: MirroredClone<Fast>,
{
    #[inline]
    fn clone(&self) -> Self {
        self.fast_mirrored_clone()
    }
}

impl<LDBG: LevelDBGenerics, Writer: DBWriteImpl<LDBG>, S: Speed> MirroredClone<S>
for InnerGenericDB<LDBG, Writer>
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.fast_mirrored_clone())
    }
}

impl<LDBG, Writer> Debug for InnerGenericDB<LDBG, Writer>
where
    LDBG:                  LevelDBGenerics,
    LDBG::FS:              Debug,
    LDBG::Skiplist:        Debug,
    LDBG::Policy:          Debug,
    LDBG::Cmp:             Debug,
    LDBG::Pool:            Debug,
    LdbPooledBuffer<LDBG>: Debug,
    Writer:                DBWriteImpl<LDBG>,
    Writer::Shared:        Debug,
    Writer::SharedMutable: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("DB")
            .field(&self.0.0)
            .field(LDBG::RwCell::debug(&self.0.1))
            .finish()
    }
}

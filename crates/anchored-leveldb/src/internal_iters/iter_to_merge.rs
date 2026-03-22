use std::sync::Arc;

use crate::{memtable::MemtableLendingIter, version::DisjointLevelIter};
use crate::{
    pub_traits::{cmp_and_policy::LevelDBComparator, pool::BufferPool},
    sstable::{TableIter, TableReader},
};


/// This iterator never acquires locks.
///
/// Usually 288 bytes in size.
pub(crate) enum IterToMerge<File, Cmp: LevelDBComparator, Policy, Pool: BufferPool> {
    // Usually 32 bytes in size.
    Memtable(MemtableLendingIter<Cmp>),
    // Usually 152 bytes in size.
    Table(TableIter<Pool>, Arc<TableReader<File, Policy, Pool>>),
    // Usually 288 bytes in size.
    Level(DisjointLevelIter<File, Policy, Pool>),
}

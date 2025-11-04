use clone_behavior::{Fast, MirroredClone};

use anchored_sstable::{
    ReadTableOptions, Table, TableBuilder, TableEntry, TableOptions, WriteTableOptions,
};
use anchored_sstable::{
    options::{BlockCacheKey, BufferPool, CompressorList, KVCache},
    iter::{TableIter, OptionalTableIter},
};
use anchored_vfs::traits::{ReadableFilesystem, WritableFilesystem};

use crate::{memtable::MemtableSkiplist, table_file::TableCacheKey};
use crate::{
    table_traits::{
        adapters::{InternalComparator, InternalFilterPolicy},
        trait_equivalents::{FilterPolicy, LevelDBComparator},
    },
    containers::{RefcountedFamily, RwCellFamily},
};


pub(crate) trait LevelDBGenerics {
    type Refcounted: RefcountedFamily;
    type RwCell:     RwCellFamily;

    type FS:         WritableFilesystem;
    type Skiplist:   MemtableSkiplist<Self::Cmp>;
    type Policy:     FilterPolicy + MirroredClone<Fast>;
    type Cmp:        LevelDBComparator + MirroredClone<Fast>;
    type BlockCache: KVCache<BlockCacheKey, <Self::Pool as BufferPool>::PooledBuffer>;
    type TableCache: KVCache<TableCacheKey, LdbTableContainer<Self>>;
    type Pool:       BufferPool + MirroredClone<Fast>;
}

// Sync only:
// TimeEnv (get timestamps which can yield a Duration from `end-start`)
// CompactorHandle (run compaction process in a background thread)

impl<
    Refcounted, RwCell, FS, Skiplist, Policy, Cmp, Logger,
    BlockCache, TableCache, Pool,
> LevelDBGenerics
for (
    Refcounted, RwCell, FS, Skiplist, Policy, Cmp, Logger,
    BlockCache, TableCache, Pool,
)
where
    Refcounted: RefcountedFamily,
    RwCell:     RwCellFamily,
    FS:         WritableFilesystem,
    Skiplist:   MemtableSkiplist<Cmp>,
    Policy:     FilterPolicy + MirroredClone<Fast>,
    Cmp:        LevelDBComparator + MirroredClone<Fast>,
    Logger:,
    BlockCache: KVCache<BlockCacheKey, <Pool as BufferPool>::PooledBuffer>,
    TableCache: KVCache<
        TableCacheKey,
        Refcounted::Container<Table<
            Refcounted::Container<CompressorList>,
            InternalFilterPolicy<Policy>,
            InternalComparator<Cmp>,
            FS::RandomAccessFile,
            BlockCache,
            Pool,
        >,
    >>,
    Pool:       BufferPool + MirroredClone<Fast>,
{
    type Refcounted = Refcounted;
    type RwCell     = RwCell;
    type FS         = FS;
    type Skiplist   = Skiplist;
    type Policy     = Policy;
    type Cmp        = Cmp;
    type BlockCache = BlockCache;
    type TableCache = TableCache;
    type Pool       = Pool;
}

pub(crate) type LdbContainer<LDBG, T>
    = <<LDBG as LevelDBGenerics>::Refcounted as RefcountedFamily>::Container<T>;
pub(crate) type LdbRwCell<LDBG, T>
    = <<LDBG as LevelDBGenerics>::RwCell as RwCellFamily>::Cell<T>;
pub(crate) type LdbMutContainer<LDBG, T> = LdbContainer<LDBG, LdbRwCell<LDBG, T>>;
pub(crate) type LdbFsCell<LDBG> = LdbRwCell<LDBG, <LDBG as LevelDBGenerics>::FS>;
pub(crate) type LdbCompressorList<LDBG> = LdbContainer<LDBG, CompressorList>;
pub(crate) type LdbPooledBuffer<LDBG>
    = <<LDBG as LevelDBGenerics>::Pool as BufferPool>::PooledBuffer;
pub(crate) type LdbFsError<LDBG> = <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::Error;
pub(crate) type Lockfile<LDBG> = <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::Lockfile;

pub(crate) type LdbTableContainer<LDBG> = LdbContainer<
    LDBG,
    Table<
        LdbCompressorList<LDBG>,
        InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
        InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
        <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::RandomAccessFile,
        <LDBG as LevelDBGenerics>::BlockCache,
        <LDBG as LevelDBGenerics>::Pool,
    >,
>;
pub(crate) type LdbTableBuilder<LDBG> = TableBuilder<
    LdbCompressorList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
    <<LDBG as LevelDBGenerics>::FS as WritableFilesystem>::WriteFile,
>;
pub(crate) type LdbTableIter<LDBG> = TableIter<
    LdbCompressorList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
    <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::RandomAccessFile,
    <LDBG as LevelDBGenerics>::BlockCache,
    <LDBG as LevelDBGenerics>::Pool,
    LdbTableContainer<LDBG>,
>;
pub(crate) type LdbOptionalTableIter<LDBG> = OptionalTableIter<
    LdbCompressorList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
    <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::RandomAccessFile,
    <LDBG as LevelDBGenerics>::BlockCache,
    <LDBG as LevelDBGenerics>::Pool,
    LdbTableContainer<LDBG>,
>;
pub(crate) type LdbTableEntry<LDBG> = TableEntry<LdbPooledBuffer<LDBG>>;
pub(crate) type LdbTableOptions<LDBG> = TableOptions<
    LdbCompressorList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
    <LDBG as LevelDBGenerics>::BlockCache,
    <LDBG as LevelDBGenerics>::Pool,
>;
pub(crate) type LdbReadTableOptions<LDBG> = ReadTableOptions<
    LdbCompressorList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
    <LDBG as LevelDBGenerics>::BlockCache,
    <LDBG as LevelDBGenerics>::Pool,
>;
pub(crate) type LdbWriteTableOptions<LDBG> = WriteTableOptions<
    LdbCompressorList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
>;

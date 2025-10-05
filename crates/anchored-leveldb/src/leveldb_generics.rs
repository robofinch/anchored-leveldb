use clone_behavior::{ConstantTime, MirroredClone};

use anchored_sstable::{ReadTableOptions, Table, TableBuilder, TableOptions, WriteTableOptions};
use anchored_sstable::options::{BlockCacheKey, BufferPool, CompressorList, KVCache};
use anchored_vfs::traits::{ReadableFilesystem, WritableFilesystem};

use crate::{memtable::MemtableSkiplist, table_file::TableCacheKey};
use crate::{
    table_traits::{
        adapters::{InternalComparator, InternalFilterPolicy},
        trait_equivalents::{FilterPolicy, LevelDBComparator},
    },
    containers::{ContainerKind, MutContainerKind, RwCell},
};


pub(crate) trait LevelDBGenerics {
    type Container:    ContainerKind;
    type MutContainer: MutContainerKind;
    type FSCell:       RwCell<Self::FS>; // TODO: get rid of this and revamp anchored-vfs

    type FS:           WritableFilesystem;
    type Skiplist:     MemtableSkiplist<Self::Cmp>;
    type Policy:       FilterPolicy + MirroredClone<ConstantTime>;
    type Cmp:          LevelDBComparator + MirroredClone<ConstantTime>;
    type Logger;
    type BlockCache:   KVCache<BlockCacheKey, <Self::Pool as BufferPool>::PooledBuffer>;
    type TableCache:   KVCache<TableCacheKey, LdbTableContainer<Self>>;
    type Pool:         BufferPool + MirroredClone<ConstantTime>;
    // LoggerConstructor <- best to just be `dyn`
    // CompactorHandle
}

impl<
    Container, MutContainer, FSCell, FS, Skiplist, Policy, Cmp, Logger,
    BlockCache, TableCache, Pool,
> LevelDBGenerics
for (
    Container, MutContainer, FSCell, FS, Skiplist, Policy, Cmp, Logger,
    BlockCache, TableCache, Pool,
)
where
    Container:    ContainerKind,
    MutContainer: MutContainerKind,
    FSCell:       RwCell<FS>,
    FS:           WritableFilesystem,
    Skiplist:     MemtableSkiplist<Cmp>,
    Policy:       FilterPolicy + MirroredClone<ConstantTime>,
    Cmp:          LevelDBComparator + MirroredClone<ConstantTime>,
    Logger:,
    BlockCache:   KVCache<BlockCacheKey, <Pool as BufferPool>::PooledBuffer>,
    TableCache:   KVCache<
        TableCacheKey,
        Container::Container<Table<
            Container::Container<CompressorList>,
            InternalFilterPolicy<Policy>,
            InternalComparator<Cmp>,
            FS::RandomAccessFile,
            BlockCache,
            Pool,
        >,
    >>,
    Pool:         BufferPool + MirroredClone<ConstantTime>,
{
    type Container    = Container;
    type MutContainer = MutContainer;
    type FSCell       = FSCell;
    type FS           = FS;
    type Skiplist     = Skiplist;
    type Policy       = Policy;
    type Cmp          = Cmp;
    type Logger       = Logger;
    type BlockCache   = BlockCache;
    type TableCache   = TableCache;
    type Pool         = Pool;
}

pub(crate) type LdbContainer<LDBG, T>
    = <<LDBG as LevelDBGenerics>::Container as ContainerKind>::Container<T>;
pub(crate) type LdbCompressorList<LDBG> = LdbContainer<LDBG, CompressorList>;
pub(crate) type LdbFsError<LDBG>  = <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::Error;
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

use clone_behavior::{ConstantTime, MirroredClone};

use anchored_sstable::{ReadTableOptions, Table, TableOptions, WriteTableOptions};
use anchored_sstable::options::{BlockCacheKey, BufferPool, CompressorList, KVCache};
use anchored_vfs::traits::{ReadableFilesystem, WritableFilesystem};

use crate::{memtable::MemtableSkiplist, table_file::TableCacheKey};
use crate::{
    table_traits::{
        adapters::{InternalComparator, InternalFilterPolicy},
        trait_equivalents::{FilterPolicy, LevelDBComparator},
    },
    containers::{ContainerKind, MutContainerKind, RwContainer},
};


pub(crate) trait LevelDBGenerics {
    type Container:    ContainerKind;
    type MutContainer: MutContainerKind;
    type FSContainer:  RwContainer<Self::FS>; // TODO: get rid of this and revamp anchored-vfs

    type FS:           WritableFilesystem;
    type Skiplist:     MemtableSkiplist<Self::Cmp>;
    type Policy:       FilterPolicy + MirroredClone<ConstantTime>;
    type Cmp:          LevelDBComparator + MirroredClone<ConstantTime>;
    type Logger;
    type BlockCache:   KVCache<BlockCacheKey, <Self::Pool as BufferPool>::PooledBuffer>;
    type TableCache:   KVCache<TableCacheKey, TableContainer<Self>>;
    type Pool:         BufferPool + MirroredClone<ConstantTime>;
    // LoggerConstructor <- best to just be `dyn`
    // CompactorHandle
}

impl<
    Container, MutContainer, FSContainer, FS, Skiplist, Policy, Cmp, Logger,
    BlockCache, TableCache, Pool,
> LevelDBGenerics
for (
    Container, MutContainer, FSContainer, FS, Skiplist, Policy, Cmp, Logger,
    BlockCache, TableCache, Pool,
)
where
    Container:    ContainerKind,
    MutContainer: MutContainerKind,
    FSContainer:  RwContainer<FS>,
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
    type FSContainer  = FSContainer;
    type FS           = FS;
    type Skiplist     = Skiplist;
    type Policy       = Policy;
    type Cmp          = Cmp;
    type Logger       = Logger;
    type BlockCache   = BlockCache;
    type TableCache   = TableCache;
    type Pool         = Pool;
}

pub(crate) type ContainerT<LDBG, T>
    = <<LDBG as LevelDBGenerics>::Container as ContainerKind>::Container<T>;
pub(crate) type CompList<LDBG> = ContainerT<LDBG, CompressorList>;
pub(crate) type FSError<LDBG>  = <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::Error;
pub(crate) type Lockfile<LDBG> = <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::Lockfile;
pub(crate) type TableContainer<LDBG> = ContainerT<
    LDBG,
    Table<
        CompList<LDBG>,
        InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
        InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
        <<LDBG as LevelDBGenerics>::FS as ReadableFilesystem>::RandomAccessFile,
        <LDBG as LevelDBGenerics>::BlockCache,
        <LDBG as LevelDBGenerics>::Pool,
    >,
>;
pub(crate) type TableOpts<LDBG> = TableOptions<
    CompList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
    <LDBG as LevelDBGenerics>::BlockCache,
    <LDBG as LevelDBGenerics>::Pool,
>;
pub(crate) type ReadTableOpts<LDBG> = ReadTableOptions<
    CompList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
    <LDBG as LevelDBGenerics>::BlockCache,
    <LDBG as LevelDBGenerics>::Pool,
>;
pub(crate) type WriteTableOpts<LDBG> = WriteTableOptions<
    CompList<LDBG>,
    InternalFilterPolicy<<LDBG as LevelDBGenerics>::Policy>,
    InternalComparator<<LDBG as LevelDBGenerics>::Cmp>,
>;

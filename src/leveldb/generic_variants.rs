use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::{
    comparator::DefaultComparator,
    container::Owned,
    filter::BloomPolicy,
    options::OpenOptionGenerics,
};
use crate::{
    compactor::{
        BlockingHandle, CloneableMpscHandle, FSError, Identity, StdThreadAndMpscChannels,
    },
    filesystem::{FileSystem, memory::MemoryFS, posix::PosixFS},
    logger::{FileLoggerCtor, LoggerConstructor, ThreadsafeFileLoggerCtor},
};

use super::{CustomLevelDB, LevelDBGenerics};


pub type LevelDB                   = CustomLevelDB<Standard>;
pub type LevelDBInMemory           = CustomLevelDB<InMemory>;
pub type ConcurrentLevelDB         = CustomLevelDB<Concurrent>;
pub type ConcurrentLevelDBInMemory = CustomLevelDB<ConcurrentInMemory>;

pub type Standard           = WithFSAndLogger<PosixFS, FileLoggerCtor>;
pub type InMemory           = WithFSAndLogger<MemoryFS, FileLoggerCtor>;
pub type Concurrent         = ConcurrentWithFSAndLogger<PosixFS, ThreadsafeFileLoggerCtor>;
pub type ConcurrentInMemory = ConcurrentWithFSAndLogger<MemoryFS, ThreadsafeFileLoggerCtor>;


#[derive(Debug)]
pub struct WithFSAndLogger<FS: FileSystem, Logger: LoggerConstructor<FS>> {
    _marker: PhantomData<(FS, Logger)>,
}

impl<FS: FileSystem, Logger: LoggerConstructor<FS>> LevelDBGenerics
for WithFSAndLogger<FS, Logger>
{
    type FS              = FS;
    type Container<T>    = Owned<T>;
    type MutContainer<T> = Owned<T>;
    type Logger          = Logger::Logger;
    type Comparator      = DefaultComparator;
    type FilterPolicy    = BloomPolicy;
    type CompactorHandle = BlockingHandle<Self>;
}

impl<FS: FileSystem, Logger: LoggerConstructor<FS>> OpenOptionGenerics
for WithFSAndLogger<FS, Logger>
{
    type LoggerConstructor      = Logger;
    type CompactorHandleCreator = Identity<Self::CompactorHandle>;
}


#[derive(Debug)]
pub struct ConcurrentWithFSAndLogger<FS: FileSystem, Logger: LoggerConstructor<FS>> {
    _marker: PhantomData<(FS, Logger)>,
}

impl<FS: FileSystem, Logger: LoggerConstructor<FS>> LevelDBGenerics
for ConcurrentWithFSAndLogger<FS, Logger>
where
    FS::Error: Send,
{
    type FS              = FS;
    type Container<T>    = Arc<T>;
    type MutContainer<T> = Arc<Mutex<T>>;
    type Logger          = Logger::Logger;
    type Comparator      = DefaultComparator;
    type FilterPolicy    = BloomPolicy;
    type CompactorHandle = CloneableMpscHandle<FSError<Self>>;
}

impl<FS, Logger> OpenOptionGenerics
for ConcurrentWithFSAndLogger<FS, Logger>
where
    FS:        FileSystem + Send + 'static,
    FS::Error: Send,
    Logger:    LoggerConstructor<FS> + 'static,
{
    type LoggerConstructor      = Logger;
    type CompactorHandleCreator = StdThreadAndMpscChannels<Self::CompactorHandle>;
}

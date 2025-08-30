use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use anchored_vfs::{ThreadLocalMemoryFS, ThreadsafeMemoryFS, traits::WritableFilesystem};
#[cfg(feature = "std-fs")]
#[cfg(any(unix, windows))]
use anchored_vfs::StandardFS;

use crate::{
    sstable_trait_implementations::{BloomPolicy, BytewiseComparator},
    options::OpenOptionGenerics,
};
use crate::{
    compactor::{
        BlockingHandle, CloneableMpscHandle, FSError, Identity, StdThreadAndMpscChannels,
    },
    logger::{FileLoggerCtor, LoggerConstructor, ThreadsafeFileLoggerCtor},
};

use super::{CustomLevelDB, LevelDBGenerics};


#[cfg(feature = "std-fs")]
#[cfg(any(unix, windows))]
pub type LevelDB                   = CustomLevelDB<Standard>;
pub type LevelDBInMemory           = CustomLevelDB<InMemory>;
#[cfg(feature = "std-fs")]
#[cfg(any(unix, windows))]
pub type ConcurrentLevelDB         = CustomLevelDB<Concurrent>;
pub type ConcurrentLevelDBInMemory = CustomLevelDB<ConcurrentInMemory>;

#[cfg(feature = "std-fs")]
#[cfg(any(unix, windows))]
pub type Standard           = WithFSAndLogger<StandardFS, FileLoggerCtor>;
pub type InMemory           = WithFSAndLogger<ThreadLocalMemoryFS, FileLoggerCtor>;
#[cfg(feature = "std-fs")]
#[cfg(any(unix, windows))]
pub type Concurrent         = ConcurrentWithFSAndLogger<StandardFS, ThreadsafeFileLoggerCtor>;
pub type ConcurrentInMemory = ConcurrentWithFSAndLogger<ThreadsafeMemoryFS, ThreadsafeFileLoggerCtor>;


#[derive(Debug)]
pub struct WithFSAndLogger<FS: WritableFilesystem, Logger: LoggerConstructor<FS>> {
    _marker: PhantomData<(FS, Logger)>,
}

impl<FS: WritableFilesystem, Logger: LoggerConstructor<FS>> LevelDBGenerics
for WithFSAndLogger<FS, Logger>
{
    type FS              = FS;
    type Container<T>    = T;
    type MutContainer<T> = T;
    type Logger          = Logger::Logger;
    type Comparator      = BytewiseComparator;
    type FilterPolicy    = BloomPolicy;
    type CompactorHandle = BlockingHandle<Self>;
}

impl<FS: WritableFilesystem, Logger: LoggerConstructor<FS>> OpenOptionGenerics
for WithFSAndLogger<FS, Logger>
{
    type LoggerConstructor      = Logger;
    type CompactorHandleCreator = Identity<Self::CompactorHandle>;
}


#[derive(Debug)]
pub struct ConcurrentWithFSAndLogger<FS: WritableFilesystem, Logger: LoggerConstructor<FS>> {
    _marker: PhantomData<(FS, Logger)>,
}

impl<FS: WritableFilesystem, Logger: LoggerConstructor<FS>> LevelDBGenerics
for ConcurrentWithFSAndLogger<FS, Logger>
where
    FS::Error: Send,
{
    type FS              = FS;
    type Container<T>    = Arc<T>;
    type MutContainer<T> = Arc<Mutex<T>>;
    type Logger          = Logger::Logger;
    type Comparator      = BytewiseComparator;
    type FilterPolicy    = BloomPolicy;
    type CompactorHandle = CloneableMpscHandle<FSError<Self>>;
}

impl<FS, Logger> OpenOptionGenerics
for ConcurrentWithFSAndLogger<FS, Logger>
where
    FS:        WritableFilesystem + Send + 'static,
    FS::Error: Send,
    Logger:    LoggerConstructor<FS> + 'static,
{
    type LoggerConstructor      = Logger;
    type CompactorHandleCreator = StdThreadAndMpscChannels<Self::CompactorHandle>;
}

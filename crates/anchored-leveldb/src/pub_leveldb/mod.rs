use std::sync::Arc;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_vfs::LevelDBFilesystem;

use crate::internal_leveldb::{PerHandleState, SharedState};
use crate::pub_traits::{
    cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
    compression::CompressionCodecs,
    pool::BufferPool,
};


pub struct DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    shared: Arc<SharedState<FS, Cmp, Policy, Codecs, Pool>>,
}

impl<FS, Cmp, Policy, Codecs, Pool> DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    #[inline]
    #[must_use]
    pub fn get_handle(&self) -> DB<FS, Cmp, Policy, Codecs, Pool> {
        let per_handle = PerHandleState {
            decoders: self.shared.opts.codecs.init_decoders(),
        };

        DB {
            shared: Arc::clone(&self.shared),
            per_handle,
        }
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Debug for DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     Debug + LevelDBFilesystem<
        RandomAccessFile: Debug,
        WriteFile: Debug,
        Lockfile: Debug,
        Error: Debug,
    >,
    Cmp:    Debug + LevelDBComparator<InvalidKeyError: Debug>,
    Policy: Debug + FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: Debug + CompressionCodecs<
        Encoders: Debug,
        CompressionError: Debug,
        DecompressionError: Debug,
    >,
    Pool:   Debug + BufferPool,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DBState")
            .field("shared", &self.shared)
            .finish()
    }
}

pub struct DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    shared:     Arc<SharedState<FS, Cmp, Policy, Codecs, Pool>>,
    per_handle: PerHandleState<Codecs::Decoders>,
}

impl<FS, Cmp, Policy, Codecs, Pool> DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    #[inline]
    #[must_use]
    pub fn state(&self) -> DBState<FS, Cmp, Policy, Codecs, Pool> {
        DBState {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Debug for DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     Debug + LevelDBFilesystem<
        RandomAccessFile: Debug,
        WriteFile: Debug,
        Lockfile: Debug,
        Error: Debug,
    >,
    Cmp:    Debug + LevelDBComparator<InvalidKeyError: Debug>,
    Policy: Debug + FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: Debug + CompressionCodecs<
        Encoders: Debug,
        Decoders: Debug,
        CompressionError: Debug,
        DecompressionError: Debug,
    >,
    Pool:   Debug + BufferPool,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DB")
            .field("shared",         &self.shared)
            .field("namper_handlee", &self.per_handle)
            .finish()
    }
}

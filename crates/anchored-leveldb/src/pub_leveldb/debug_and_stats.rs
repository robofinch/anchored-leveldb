use std::fmt::{Debug, Formatter, Result as FmtResult};

use anchored_vfs::LevelDBFilesystem;

use crate::pub_traits::{
    cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
    compression::CompressionCodecs,
    pool::BufferPool,
};
use super::structs::{DB, DBState};


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
    Pool:   Debug + BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let (shared, per_handle) = self.inner_ref();
        f.debug_struct("DB")
            .field("shared",     shared)
            .field("per_handle", per_handle)
            .finish()
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
    Pool:   Debug + BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DBState")
            .field("shared", self.db_state())
            .finish()
    }
}

use clone_behavior::{FastMirroredClone, MaybeSlow, MirroredClone};

use anchored_vfs::LevelDBFilesystem;

use crate::{
    all_errors::aliases::RecoveryResult,
    internal_leveldb::InternalDBState,
    options::pub_options::OpenOptions,
};
use crate::pub_traits::{
    cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
    compression::CompressionCodecs,
    pool::BufferPool,
};
use super::structs::{DB, DBState};


impl<FS, Cmp, Policy, Codecs, Pool> DB<FS, Cmp, Policy, Codecs, Pool>
where
    // TODO: Loosen `Send + Sync` requirements
    FS:                         LevelDBFilesystem + Send + Sync + 'static,
    FS::RandomAccessFile:       Send + Sync,
    FS::WriteFile:              Send + Sync,
    FS::Lockfile:               Send,
    FS::Error:                  Send,
    Cmp:                        LevelDBComparator + FastMirroredClone + Send + Sync + 'static,
    Cmp::InvalidKeyError:       Send,
    Policy:                     FilterPolicy + FastMirroredClone + Send + Sync + 'static,
    Policy::Eq:                 CoarserThan<Cmp::Eq>,
    Codecs:                     CompressionCodecs + Send + Sync + 'static,
    Codecs::Encoders:           Send,
    Codecs::Decoders:           Send,
    Codecs::CompressionError:   Send,
    Codecs::DecompressionError: Send,
    Pool:                       BufferPool<PooledBuffer: Send + Sync> + Send + Sync + 'static,
{
    /// Open an existing LevelDB database or create a new one, depending on settings.
    ///
    /// Note that a [`DBState`] doesn't store some of the resources used for reading from the
    /// databases; [`DB`] structs allow those resources to be reused, so they should be used if
    /// possible.
    pub fn open(
        options: OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
    ) -> RecoveryResult<Self, FS, Cmp, Codecs> {
        let (db_state, finisher, per_handle) = InternalDBState::open(options)?;

        // Correctness: the three arguments are fresh from `InternalDBState::open`.
        Ok(Self::finish_open(db_state, finisher, per_handle))
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    /// Get a reference-counted [`DBState`] handle to the database.
    ///
    /// (Use [`Clone::clone`] on `self` to get another `DB` handle.)
    #[inline]
    #[must_use]
    pub fn get_db_state(&self) -> DBState<FS, Cmp, Policy, Codecs, Pool> {
        self.get_db_state_impl()
    }

    /// Discard per-`DB` resources to convert this handle into a [`DBState`].
    #[inline]
    #[must_use]
    pub fn into_db_state(self) -> DBState<FS, Cmp, Policy, Codecs, Pool> {
        self.into_db_state_impl()
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Clone for DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    #[inline]
    fn clone(&self) -> Self {
        self.get_db_state().into_db()
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> MirroredClone<MaybeSlow> for DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> DBState<FS, Cmp, Policy, Codecs, Pool>
where
    // TODO: Loosen `Send + Sync` requirements
    FS:                         LevelDBFilesystem + Send + Sync + 'static,
    FS::RandomAccessFile:       Send + Sync,
    FS::WriteFile:              Send + Sync,
    FS::Lockfile:               Send,
    FS::Error:                  Send,
    Cmp:                        LevelDBComparator + FastMirroredClone + Send + Sync + 'static,
    Cmp::InvalidKeyError:       Send,
    Policy:                     FilterPolicy + FastMirroredClone + Send + Sync + 'static,
    Policy::Eq:                 CoarserThan<Cmp::Eq>,
    Codecs:                     CompressionCodecs + Send + Sync + 'static,
    Codecs::Encoders:           Send,
    Codecs::Decoders:           Send,
    Codecs::CompressionError:   Send,
    Codecs::DecompressionError: Send,
    Pool:                       BufferPool<PooledBuffer: Send + Sync> + Send + Sync + 'static,
{
    /// Open an existing LevelDB database or create a new one, depending on settings.
    ///
    /// Note that a [`DBState`] doesn't store some of the resources used for reading from the
    /// databases; [`DB`] structs allow those resources to be reused, so they should be used if
    /// possible.
    pub fn open(
        options: OpenOptions<FS, Cmp, Policy, Codecs, Pool>,
    ) -> RecoveryResult<Self, FS, Cmp, Codecs> {
        // There's no opportunity for greater efficiency; the per-handle resources are needed
        // for the opening process, if nothing else.
        DB::open(options).map(DB::into_db_state)
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    /// Get a reference-counted [`DB`] handle to the database.
    ///
    /// (Use [`clone`] on `self` to get another `DBState` handle.)
    ///
    /// [`clone`]: Clone::clone
    #[inline]
    #[must_use]
    pub fn get_db(&self) -> DB<FS, Cmp, Policy, Codecs, Pool> {
        self.clone().into_db()
    }

    /// Acquire per-`DB` resources to convert this handle into a [`DB`].
    #[inline]
    #[must_use]
    pub fn into_db(self) -> DB<FS, Cmp, Policy, Codecs, Pool> {
        self.into_db_impl()
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Clone for DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    #[inline]
    fn clone(&self) -> Self {
        self.clone_impl()
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> MirroredClone<MaybeSlow>
for DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        self.clone()
    }
}

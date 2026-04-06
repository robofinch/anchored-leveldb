use clone_behavior::{FastMirroredClone, MaybeSlow, MirroredClone};

use anchored_vfs::LevelDBFilesystem;

use crate::{all_errors::aliases::RecoveryResult, options::pub_options::OpenOptions};
use crate::{
    internal_leveldb::{InternalDBState, PerHandleState},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
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
    Codecs::Encoders:           Send + Sync,
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
        InternalDBState::open(options)
            .map(|(shared, per_handle)| Self {
                shared,
                per_handle,
            })
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
    pub fn get_state(&self) -> DBState<FS, Cmp, Policy, Codecs, Pool> {
        #![expect(clippy::missing_panics_doc, reason = "panic should never realistically happen")]

        let mut mut_state = self.shared.lock_mutable_state();
        let this = DBState {
            shared: self.shared.fast_mirrored_clone(),
        };
        #[expect(clippy::expect_used, reason = "panic should never realistically happen")]
        {
            mut_state.non_compactor_arc_refcounts = mut_state.non_compactor_arc_refcounts
                .checked_add(1)
                .expect("DBState refcount overflow");
        };
        drop(mut_state);
        this
    }

    /// Discard per-`DB` resources to convert this handle into a [`DBState`].
    #[inline]
    #[must_use]
    pub fn into_state(self) -> DBState<FS, Cmp, Policy, Codecs, Pool> {
        DBState {
            shared: self.into_inner().0,
        }
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
        self.get_state().into_db()
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
    Codecs::Encoders:           Send + Sync,
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
        InternalDBState::open(options)
            .map(|(shared, _per_handle)| Self { shared })
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
    #[inline]
    #[must_use]
    pub fn get_db(&self) -> DB<FS, Cmp, Policy, Codecs, Pool> {
        self.clone().into_db()
    }

    /// Acquire per-`DB` resources to convert this handle into a [`DB`].
    #[inline]
    #[must_use]
    pub fn into_db(self) -> DB<FS, Cmp, Policy, Codecs, Pool> {
        let per_handle = PerHandleState {
            decoders:     self.shared.opts.codecs.init_decoders(),
            iter_key_buf: Vec::new(),
        };
        DB {
            shared: self.into_inner(),
            per_handle,
        }
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
        let mut mut_state = self.shared.lock_mutable_state();
        let this = Self {
            shared: self.shared.fast_mirrored_clone(),
        };
        #[expect(clippy::expect_used, reason = "panic should never realistically happen")]
        {
            mut_state.non_compactor_arc_refcounts = mut_state.non_compactor_arc_refcounts
                .checked_add(1)
                .expect("DBState refcount overflow");
        };
        drop(mut_state);
        this
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

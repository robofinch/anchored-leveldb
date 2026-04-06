use anchored_vfs::LevelDBFilesystem;

use crate::all_errors::aliases::RwResult;
use crate::{
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{Close, CloseStatus},
    typed_bytes::{BlockOnWrites, ReleaseRefcount},
};
use super::structs::{DB, DBState};


impl<FS, Cmp, Policy, Codecs, Pool> DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// A checked alternative to simply dropping this [`DBState`].
    ///
    /// Release one reference count of the database. If `self` is the last reference count
    /// (excluding any internal reference counts), then this function will close the database and
    /// block until ongoing writes (including compactions) have stopped before returning. Note that
    /// each database iterator holds a reference count.
    ///
    /// If the database is closed, depending on the given [`Close`] argument, any ongoing
    /// compaction is either terminated as quickly as possible or is permitted to complete.
    /// No additional compactions are permitted.
    ///
    /// The [`CloseStatus`] of the database is returned, which is [`CloseStatus::Closed`] if
    /// `self` was the last reference count. Otherwise, if methods like [`force_close_all`]
    /// are avoided, the result is [`CloseStatus::Open`]. Using [`force_close_all`] and similar
    /// can result in any [`CloseStatus`] being returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    ///
    /// [`force_close_all`]: DBState::force_close_all
    pub fn close(
        self,
        when: Close,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        self.shared.close(when, BlockOnWrites::True, ReleaseRefcount::True)
    }

    /// A checked alternative to simply dropping this [`DBState`]. It is nonblocking insofar as it
    /// does not wait for other reads or writes to complete, but it does still acquire a mutex.
    ///
    /// If `self` is the last reference count of the database (excluding any internal reference
    /// counts), then this function will close the database. Note that each database iterator holds
    /// a reference count.
    ///
    /// If the database is closed, depending on the given [`Close`] argument, any ongoing compaction
    /// is either terminated as quickly as possible or is permitted to complete. No additional
    /// compactions are permitted. However, this function does not wait for compactions to complete.
    ///
    /// The [`CloseStatus`] of the database is returned, which is [`CloseStatus::Closed`] if
    /// `self` was the last reference count. Otherwise, if methods like [`force_close_all`]
    /// are avoided, the result is [`CloseStatus::Open`]. Using [`force_close_all`] and similar
    /// can result in any [`CloseStatus`] being returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    ///
    /// [`force_close_all`]: DBState::force_close_all
    pub fn close_nonblocking(
        &self,
        when: Close,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        self.shared.close(when, BlockOnWrites::False, ReleaseRefcount::False)
    }

    /// Forcefully close the database, regardless of whether `self` is the last reference count of
    /// the database.
    ///
    /// The database will not completely close until all reads have stopped, including via
    /// database iterators. If there are no ongoing reads, then this function will block until
    /// ongoing writes (including compactions) have stopped before returning.
    ///
    /// Depending on the given [`Close`] argument, any ongoing compaction is either terminated as
    /// quickly as possible or is permitted to complete. No additional compactions are permitted.
    ///
    /// The [`CloseStatus`] of the database is returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    pub fn force_close_all(
        &self,
        when: Close,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        self.shared.force_close(self.shared.lock_mutable_state(), when, BlockOnWrites::True)
    }

    /// Forcefully close the database, regardless of whether `self` is the last reference count of
    /// the database. It is nonblocking insofar as it does not wait for other reads or writes to
    /// complete, but it does still acquire a mutex.
    ///
    /// The database will not completely close until all reads have stopped, including via
    /// database iterators.
    ///
    /// Depending on the given [`Close`] argument, any ongoing compaction is either terminated as
    /// quickly as possible or is permitted to complete. No additional compactions are permitted.
    /// However, this function does not wait for compactions to complete.
    ///
    /// The [`CloseStatus`] of the database is returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    pub fn force_close_all_nonblocking(
        &self,
        when: Close,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        self.shared.force_close(self.shared.lock_mutable_state(), when, BlockOnWrites::False)
    }

    /// Get the current [`CloseStatus`] of the database, which determines whether additional reads
    /// to the database are permitted.
    ///
    /// Writes to the database may additionally be closed due to errors.
    #[must_use]
    pub fn close_status(&self) -> CloseStatus {
        self.shared.lock_mutable_state().close_status
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Drop for DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    fn drop(&mut self) {
        let _ignore_result = self.shared.close(
            Close::AsSoonAsPossible,
            BlockOnWrites::False,
            ReleaseRefcount::True,
        );
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// A checked alternative to simply dropping this [`DBState`].
    ///
    /// Release one reference count of the database. If `self` is the last reference count
    /// (excluding any internal reference counts), then this function will close the database and
    /// block until ongoing writes (including compactions) have stopped before returning. Note that
    /// each database iterator holds a reference count.
    ///
    /// If the database is closed, depending on the given [`Close`] argument, any ongoing
    /// compaction is either terminated as quickly as possible or is permitted to complete.
    /// No additional compactions are permitted.
    ///
    /// The [`CloseStatus`] of the database is returned, which is [`CloseStatus::Closed`] if
    /// `self` was the last reference count. Otherwise, if methods like [`force_close_all`]
    /// are avoided, the result is [`CloseStatus::Open`]. Using [`force_close_all`] and similar
    /// can result in any [`CloseStatus`] being returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    ///
    /// [`force_close_all`]: DBState::force_close_all
    pub fn close(
        self,
        when: Close,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        self.shared.close(when, BlockOnWrites::True, ReleaseRefcount::True)
    }

    /// A checked alternative to simply dropping this [`DBState`]. It is nonblocking insofar as it
    /// does not wait for other reads or writes to complete, but it does still acquire a mutex.
    ///
    /// If `self` is the last reference count of the database (excluding any internal reference
    /// counts), then this function will close the database. Note that each database iterator holds
    /// a reference count.
    ///
    /// If the database is closed, depending on the given [`Close`] argument, any ongoing compaction
    /// is either terminated as quickly as possible or is permitted to complete. No additional
    /// compactions are permitted. However, this function does not wait for compactions to complete.
    ///
    /// The [`CloseStatus`] of the database is returned, which is [`CloseStatus::Closed`] if
    /// `self` was the last reference count. Otherwise, if methods like [`force_close_all`]
    /// are avoided, the result is [`CloseStatus::Open`]. Using [`force_close_all`] and similar
    /// can result in any [`CloseStatus`] being returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    ///
    /// [`force_close_all`]: DBState::force_close_all
    pub fn close_nonblocking(
        &self,
        when: Close,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        self.shared.close(when, BlockOnWrites::False, ReleaseRefcount::False)
    }

    /// Forcefully close the database, regardless of whether `self` is the last reference count of
    /// the database.
    ///
    /// The database will not completely close until all reads have stopped, including via
    /// database iterators. If there are no ongoing reads, then this function will block until
    /// ongoing writes (including compactions) have stopped before returning.
    ///
    /// Depending on the given [`Close`] argument, any ongoing compaction is either terminated as
    /// quickly as possible or is permitted to complete. No additional compactions are permitted.
    ///
    /// The [`CloseStatus`] of the database is returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    pub fn force_close_all(
        &self,
        when: Close,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        self.shared.force_close(self.shared.lock_mutable_state(), when, BlockOnWrites::True)
    }

    /// Forcefully close the database, regardless of whether `self` is the last reference count of
    /// the database. It is nonblocking insofar as it does not wait for other reads or writes to
    /// complete, but it does still acquire a mutex.
    ///
    /// The database will not completely close until all reads have stopped, including via
    /// database iterators.
    ///
    /// Depending on the given [`Close`] argument, any ongoing compaction is either terminated as
    /// quickly as possible or is permitted to complete. No additional compactions are permitted.
    /// However, this function does not wait for compactions to complete.
    ///
    /// The [`CloseStatus`] of the database is returned.
    ///
    /// Attempting to close the database multiple times does not result in an error. Any error
    /// other than a close error (one for which [`RwErrorKind::is_closed_error`] returns `true`)
    /// may be returned.
    pub fn force_close_all_nonblocking(
        &self,
        when: Close,
    ) -> (CloseStatus, RwResult<(), FS, Cmp, Codecs>) {
        self.shared.force_close(self.shared.lock_mutable_state(), when, BlockOnWrites::False)
    }

    /// Get the current [`CloseStatus`] of the database, which determines whether additional reads
    /// to the database are permitted.
    ///
    /// Writes to the database may additionally be closed due to errors.
    #[must_use]
    pub fn close_status(&self) -> CloseStatus {
        self.shared.lock_mutable_state().close_status
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Drop for DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool
{
    fn drop(&mut self) {
        let _ignore_result = self.shared.close(
            Close::AsSoonAsPossible,
            BlockOnWrites::False,
            ReleaseRefcount::True,
        );
    }
}

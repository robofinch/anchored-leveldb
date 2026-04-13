use clone_behavior::FastMirroredClone;

use anchored_vfs::LevelDBFilesystem;

use crate::{
    internal_iters::InternalDBIter,
    options::pub_options::ReadOptions,
    pub_typed_bytes::TableEntry,
    typed_bytes::UserKey,
};
use crate::{
    all_errors::{
        aliases::RwResult,
        types::{ReadError, RwErrorKind},
    },
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
};
use super::structs::{DB, DBState};


impl<FS, Cmp, Policy, Codecs, Pool> DB<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator + FastMirroredClone,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Get a circular lending iterator over the entries of the database, in sorted order.
    ///
    /// Default [`ReadOptions`] are used.
    ///
    /// (By "circular", it is meant that `None` is treated as a phantom entry before the first
    /// element and after the last element; the iterator is not [fused].)
    ///
    /// Note that the iterator intentionally takes `self` by value.
    /// Since [`DB`] and [`DBState`] structs are reference-counted, you can clone `self` before
    /// calling this method if you want to keep `self` around. Additionally, a [`DB`] value
    /// can be reclaimed from the iterator.
    ///
    /// [fused]: std::iter::FusedIterator
    #[expect(clippy::iter_not_returning_iterator, reason = "fallibly returns a lending iterator")]
    pub fn iter(self) -> RwResult<DBIter<FS, Cmp, Policy, Codecs, Pool>, FS, Cmp, Codecs> {
        self.iter_with(&ReadOptions::default())
    }

    /// Get a circular lending iterator over the entries of the database, in sorted order.
    ///
    /// (By "circular", it is meant that `None` is treated as a phantom entry before the first
    /// element and after the last element; the iterator is not [fused].)
    ///
    /// Note that the iterator intentionally takes `self` by value.
    /// Since [`DB`] and [`DBState`] structs are reference-counted, you can clone `self` before
    /// calling this method if you want to keep `self` around. Additionally, a [`DB`] value
    /// can be reclaimed from the iterator.
    ///
    /// [fused]: std::iter::FusedIterator
    pub fn iter_with(
        self,
        read_opts: &ReadOptions,
    ) -> RwResult<DBIter<FS, Cmp, Policy, Codecs, Pool>, FS, Cmp, Codecs> {
        Ok(DBIter {
            inner: InternalDBIter::new(self, read_opts)?
        })
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> DBState<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator + FastMirroredClone,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Acquire per-[`DB`] resources and get a circular lending iterator over the entries of the
    /// database, in sorted order.
    ///
    /// Default [`ReadOptions`] are used.
    ///
    /// (By "circular", it is meant that `None` is treated as a phantom entry before the first
    /// element and after the last element; the iterator is not [fused].)
    ///
    /// Note that the iterator intentionally takes `self` by value.
    /// Since [`DB`] and [`DBState`] structs are reference-counted, you can clone `self` before
    /// calling this method if you want to keep `self` around. Additionally, a [`DB`] value
    /// can be reclaimed from the iterator.
    ///
    /// [fused]: std::iter::FusedIterator
    #[expect(clippy::iter_not_returning_iterator, reason = "fallibly returns a lending iterator")]
    pub fn iter(self) -> RwResult<DBIter<FS, Cmp, Policy, Codecs, Pool>, FS, Cmp, Codecs> {
        self.into_db().iter()
    }

    /// Acquire per-[`DB`] resources and get a circular lending iterator over the entries of the
    /// database, in sorted order.
    ///
    /// (By "circular", it is meant that `None` is treated as a phantom entry before the first
    /// element and after the last element; the iterator is not [fused].)
    ///
    /// Note that the iterator intentionally takes `self` by value.
    /// Since [`DB`] and [`DBState`] structs are reference-counted, you can clone `self` before
    /// calling this method if you want to keep `self` around. Additionally, a [`DB`] value
    /// can be reclaimed from the iterator.
    ///
    /// [fused]: std::iter::FusedIterator
    pub fn iter_with(
        self,
        read_opts: &ReadOptions,
    ) -> RwResult<DBIter<FS, Cmp, Policy, Codecs, Pool>, FS, Cmp, Codecs> {
        self.into_db().iter_with(read_opts)
    }
}

/// A `DBIter` is a circular lending iterator over the entries of a LevelDB database, in the order
/// they are sorted by the user-chosen `Cmp` comparator.
///
/// (By "circular", it is meant that `None` is treated as a phantom entry before the first
/// element and after the last element; the iterator is not [fused].)
///
/// # Errors
/// If an error is returned (including database corruption, or more minor errors), the position of
/// the iterator becomes unspecified. (That is, the return values of [`valid`], [`next`],
/// [`current`], and [`prev`] in that scenario should not be relied on.)
///
/// It is only guaranteed that no panics or unsoundness will occur in such a case.
///
/// The iterator may be able to be restored to a known state via [`reset`], [`seek`], or similar.
///
/// [fused]: std::iter::FusedIterator
/// [`valid`]: Self::valid
/// [`next`]: Self::next
/// [`current`]: Self::current
/// [`prev`]: Self::prev
/// [`reset`]: Self::reset
/// [`seek`]: Self::seek
#[expect(missing_debug_implementations, reason = "not a priority. TODO: debug impl")]
pub struct DBIter<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    inner: InternalDBIter<FS, Cmp, Policy, Codecs, Pool>,
}

impl<FS, Cmp, Policy, Codecs, Pool> DBIter<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    #[inline]
    #[must_use]
    pub fn into_db(self) -> DB<FS, Cmp, Policy, Codecs, Pool> {
        self.inner.into_db()
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> DBIter<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Determine whether the iterator is currently at any entry in the database.
    /// If the iterator is invalid, then it is conceptually one position before the first entry
    /// and one position after the last entry. (Or, there may be no entries.)
    ///
    /// [`current()`] will be `Some` if and only if the iterator is valid.
    ///
    /// [`current()`]: DBIter::current
    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.inner.valid()
    }

    /// Return the `(key, value)` entry at the iterator's current position in the sorted order
    /// of database entries.
    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<TableEntry<'_>> {
        self.inner.current().map(TableEntry::from_user_tuple)
    }

    /// Reset the iterator to its initial position.
    ///
    /// The iterator becomes `!valid()`, and is conceptually one position before the first entry
    /// and one position after the last entry (if there are any entries in the collection).
    pub fn reset(&mut self) {
        self.inner.activate().0.reset();
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> DBIter<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Fallibly return the next `(key, value)` entry in the database (if any).
    ///
    /// Returns `None` if the iterator was at the last entry.
    #[expect(clippy::should_implement_trait, reason = "this is a *lending* iterator")]
    pub fn next(&mut self) -> RwResult<Option<TableEntry<'_>>, FS, Cmp, Codecs> {
        let (mut iter, extra_state) = self.inner.activate();
        iter.next(extra_state)?;
        Ok(self.inner.current().map(TableEntry::from_user_tuple))
    }

    /// Fallibly return the previous `(key, value)` entry in the database (if any).
    ///
    /// Returns `None` if the iterator was at the first entry.
    ///
    /// # Speed Warning
    /// Backwards iteration is noticeably slower than forwards iteration.
    pub fn prev(&mut self) -> RwResult<Option<TableEntry<'_>>, FS, Cmp, Codecs> {
        let (mut iter, extra_state) = self.inner.activate();
        iter.prev(extra_state).map(|opt| opt.map(TableEntry::from_user_tuple))
    }

    /// Move the iterator to the first entry whose key is greater than or equal to the provided
    /// `lower_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the database).
    pub fn seek(&mut self, lower_bound: &[u8]) -> RwResult<(), FS, Cmp, Codecs> {
        let lower_bound = UserKey::new(lower_bound)
            .ok_or(self.inner.rw_error(RwErrorKind::Read(ReadError::KeyTooLong)))?;

        let (mut iter, extra_state) = self.inner.activate();
        iter.seek(extra_state, lower_bound)
    }

    /// Move the iterator to the last entry whose key is strictly less than the provided
    /// `strict_upper_bound`.
    ///
    /// If there is no such key, the iterator becomes `!valid()`, and is conceptually
    /// one position before the first entry and one position after the last entry (if there are
    /// any entries in the database).
    ///
    /// # Speed Warning
    /// Backwards iteration is noticeably slower than forwards iteration.
    pub fn seek_before(&mut self, strict_upper_bound: &[u8]) -> RwResult<(), FS, Cmp, Codecs> {
        let strict_upper_bound = UserKey::new(strict_upper_bound)
            .ok_or(self.inner.rw_error(RwErrorKind::Read(ReadError::KeyTooLong)))?;

        let (mut iter, extra_state) = self.inner.activate();
        iter.seek_before(extra_state, strict_upper_bound)
    }

    /// Move the iterator to the first database entry in the sorted order.
    ///
    /// If the database is empty, the iterator becomes `!valid()`.
    pub fn seek_to_first(&mut self) -> RwResult<(), FS, Cmp, Codecs> {
        let (mut iter, extra_state) = self.inner.activate();
        iter.seek_to_first(extra_state)
    }

    /// Move the iterator to the last database entry in the sorted order.
    ///
    /// If the database is empty, the iterator becomes `!valid()`.
    ///
    /// # Speed Warning
    /// Backwards iteration is noticeably slower than forwards iteration.
    pub fn seek_to_last(&mut self) -> RwResult<(), FS, Cmp, Codecs> {
        let (mut iter, extra_state) = self.inner.activate();
        iter.seek_to_last(extra_state)
    }
}

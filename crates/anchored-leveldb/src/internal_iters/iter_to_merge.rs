use std::sync::Arc;

use clone_behavior::FastMirroredClone;

use anchored_vfs::{LevelDBFilesystem, RandomAccess};

use crate::{
    all_errors::aliases::RwErrorKindAlias,
    memtable::MemtableLendingIter,
    internal_leveldb::InternalDBState,
    options::InternalReadOptions,
    version::Version,
};
use crate::{
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    sstable::{TableIter, TableReader},
    typed_bytes::{EncodedInternalEntry, InternalKey},
};
use super::level_iter::DisjointLevelIter;


pub(super) type IterResult<'a, FS, Cmp, Codecs> = Result<
    Option<EncodedInternalEntry<'a>>,
    RwErrorKindAlias<FS, Cmp, Codecs>,
>;

/// This iterator never acquires database-wide locks. (Though, it does use the buffer pool,
/// caches, and so on.)
///
/// Usually 288 bytes in size.
// TODO: Debug impl
pub(crate) enum IterToMerge<File, Cmp: LevelDBComparator, Policy, Pool: BufferPool> {
    // Usually 32 bytes in size.
    Memtable(MemtableLendingIter<Cmp>),
    // Usually 152 bytes in size.
    Table(TableIter<Pool>, Arc<TableReader<File, Policy, Pool>>),
    // Usually 288 bytes in size.
    Level(DisjointLevelIter<File, Policy, Pool>),
}

impl<File, Cmp, Policy, Pool> IterToMerge<File, Cmp, Policy, Pool>
where
    File:   RandomAccess,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Pool:   BufferPool,
{
    pub(super) fn next<'a, FS, Codecs>(
        &'a mut self,
        version:   &'a Version,
        db_state:  &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:  &mut Codecs::Decoders,
        read_opts: InternalReadOptions,
    ) -> IterResult<'a, FS, Cmp, Codecs>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        Policy: FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        let opts = &db_state.opts;
        let mut_opts = &db_state.mut_opts;

        match self {
            Self::Memtable(iter) => Ok(iter.next()),
            Self::Table(iter, table) => {
                iter.next(table, opts, mut_opts, read_opts, decoders)
            }
            Self::Level(iter) => {
                iter.with_opts(version, opts, mut_opts, read_opts, decoders).next()?;
                Ok(iter.current())
            }
        }
    }

    #[inline]
    #[must_use]
    pub(super) fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        match self {
            Self::Memtable(iter) => iter.current(),
            Self::Table(iter, _) => iter.current(),
            Self::Level(iter)    => iter.current(),
        }
    }

    pub(super) fn prev<'a, FS, Codecs>(
        &'a mut self,
        version:   &'a Version,
        db_state:  &'a InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:  &'a mut Codecs::Decoders,
        read_opts: InternalReadOptions,
    ) -> IterResult<'a, FS, Cmp, Codecs>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        Policy: FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        let opts = &db_state.opts;
        let mut_opts = &db_state.mut_opts;

        match self {
            Self::Memtable(iter) => Ok(iter.prev()),
            Self::Table(iter, table) => {
                iter.prev(table, opts, mut_opts, read_opts, decoders)
            }
            Self::Level(iter) => {
                iter.with_opts(version, opts, mut_opts, read_opts, decoders).prev()?;
                Ok(iter.current())
            }
        }
    }

    pub(super) fn reset(&mut self, version: &Version) {
        match self {
            Self::Memtable(iter) => iter.reset(),
            Self::Table(iter, _) => iter.reset(),
            Self::Level(iter)    => iter.reset(version),
        }
    }

    pub(super) fn seek<FS, Codecs>(
        &mut self,
        version:     &Version,
        db_state:    &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:    &mut Codecs::Decoders,
        read_opts:   InternalReadOptions,
        lower_bound: InternalKey<'_>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        Policy: FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        let opts = &db_state.opts;
        let mut_opts = &db_state.mut_opts;

        match self {
            Self::Memtable(iter) => {
                iter.seek(lower_bound);
                Ok(())
            }
            Self::Table(iter, table) => {
                iter.seek(table, opts, mut_opts, read_opts, decoders, lower_bound)
            }
            Self::Level(iter) => {
                iter.with_opts(version, opts, mut_opts, read_opts, decoders).seek(lower_bound)
            }
        }
    }

    pub(super) fn seek_before<FS, Codecs>(
        &mut self,
        version:            &Version,
        db_state:           &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:           &mut Codecs::Decoders,
        read_opts:          InternalReadOptions,
        strict_upper_bound: InternalKey<'_>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        Policy: FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        let opts = &db_state.opts;
        let mut_opts = &db_state.mut_opts;

        match self {
            Self::Memtable(iter) => {
                iter.seek_before(strict_upper_bound);
                Ok(())
            }
            Self::Table(iter, table) => {
                iter.seek_before(table, opts, mut_opts, read_opts, decoders, strict_upper_bound)
            }
            Self::Level(iter) => {
                iter.with_opts(version, opts, mut_opts, read_opts, decoders).seek_before(strict_upper_bound)
            }
        }
    }

    pub(super) fn seek_to_first<FS, Codecs>(
        &mut self,
        version:   &Version,
        db_state:  &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:  &mut Codecs::Decoders,
        read_opts: InternalReadOptions,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        Policy: FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        let opts = &db_state.opts;
        let mut_opts = &db_state.mut_opts;

        match self {
            Self::Memtable(iter) => {
                iter.seek_to_first();
                Ok(())
            }
            Self::Table(iter, table) => {
                iter.seek_to_first(table, opts, mut_opts, read_opts, decoders)
            }
            Self::Level(iter) => {
                iter.with_opts(version, opts, mut_opts, read_opts, decoders).seek_to_first()
            }
        }
    }

    pub(super) fn seek_to_last<FS, Codecs>(
        &mut self,
        version:   &Version,
        db_state:  &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:  &mut Codecs::Decoders,
        read_opts: InternalReadOptions,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        Policy: FastMirroredClone,
        Codecs: CompressionCodecs,
    {
        let opts = &db_state.opts;
        let mut_opts = &db_state.mut_opts;

        match self {
            Self::Memtable(iter) => {
                iter.seek_to_last();
                Ok(())
            }
            Self::Table(iter, table) => {
                iter.seek_to_last(table, opts, mut_opts, read_opts, decoders)
            }
            Self::Level(iter) => {
                iter.with_opts(version, opts, mut_opts, read_opts, decoders).seek_to_last()
            }
        }
    }
}

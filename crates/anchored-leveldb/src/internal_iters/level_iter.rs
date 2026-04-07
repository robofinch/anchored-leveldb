#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style early returns of borrows"),
)]

use std::sync::Arc;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::FastMirroredClone;

use anchored_vfs::LevelDBFilesystem;

use crate::{all_errors::aliases::RwErrorKindAlias, table_file::read_sstable};
use crate::{
    options::{InternallyMutableOptions, InternalOptions, InternalReadOptions},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{FileNumber, NonZeroLevel},
    sstable::{TableIter, TableReader},
    typed_bytes::{EncodedInternalEntry, InternalKey},
    version::Version,
};
use super::file_iter::DisjointLevelFileIter;


/// Concatenating iterator over all the table files in a certain nonzero [`Level`]
/// (whose files do not have overlapping key ranges).
pub(crate) struct DisjointLevelIter<File, Policy, Pool: BufferPool> {
    /// # Invariants
    /// - If `sstable` is `Some(_)`, then `sstable_iter` should be set to that table and be
    ///   `valid()`.
    ///
    /// If `sstable_iter` becomes `!valid()`, then a new table file should be retrieved from
    /// `level_file_iter`, if possible. Note that `sstable_iter` should be cleared before attempting
    /// to open a new table file, for performance reasons.
    sstable_iter:    TableIter<Pool>,
    sstable:         Option<Arc<TableReader<File, Policy, Pool>>>,
    /// # Invariants
    /// - `level_file_iter.current()` should be the file metadata of `sstable`.
    level_file_iter: DisjointLevelFileIter,
    /// The file number of the `MANIFEST` which contains the `Version` used in
    /// `self.level_file_iter`.
    manifest_number: FileNumber,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<File, Policy, Pool: BufferPool> DisjointLevelIter<File, Policy, Pool> {
   #[must_use]
    pub fn new_disjoint(
        version:         &Version,
        level:           NonZeroLevel,
        manifest_number: FileNumber,
    ) -> Self {
        Self {
            sstable_iter:    TableIter::new_empty(),
            sstable:         None,
            level_file_iter: DisjointLevelFileIter::new(version, level),
            manifest_number,
        }
    }

    #[inline]
    #[must_use]
    pub const fn with_opts<'a, 'b, FS, Cmp, Codecs>(
        &'a mut self,
        version:   &'a Version,
        opts:      &'b InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:  &'b InternallyMutableOptions<FS, Policy, Pool>,
        read_opts: InternalReadOptions,
        decoders:  &'b mut Codecs::Decoders,
    ) -> DisjointLevelIterWithOpts<'a, 'b, FS, Cmp, Policy, Codecs, Pool>
    where
        FS:     LevelDBFilesystem<RandomAccessFile = File>,
        Codecs: CompressionCodecs,
    {
        DisjointLevelIterWithOpts {
            iter: self,
            version,
            opts,
            mut_opts,
            read_opts,
            decoders,
        }
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        // `self.sstable` is `Some(_)` if and only if both `self.sstable_iter` and
        // `self.level_file_iter` are `valid()`.
        self.sstable.is_some()
    }

    #[must_use]
    pub fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.sstable_iter.current()
    }

    pub fn reset(&mut self, version: &Version) {
        // After these calls, `self.sstable_iter` is not initialized and `self.level_file_iter`
        // is `!valid()`, so the invariants are satisfied.
        self.sstable_iter.clear();
        self.level_file_iter.reset(version);
    }
}


impl<File, Policy, Pool> Debug for DisjointLevelIter<File, Policy, Pool>
where
    File:   Debug,
    Policy: Debug,
    Pool:   BufferPool<PooledBuffer: Debug>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DisjointLevelIter")
            .field("sstable_iter",    &self.sstable_iter)
            .field("sstable",         &self.sstable)
            .field("level_file_iter", &self.level_file_iter)
            .field("manifest_number", &self.manifest_number)
            .finish()
    }
}

// `Debug` is **not** implemented for this struct, since implementing it would be tedious,
// and it should be transient.
pub(crate) struct DisjointLevelIterWithOpts<'a, 'b, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    iter:      &'a mut DisjointLevelIter<FS::RandomAccessFile, Policy, Pool>,
    version:   &'a Version,
    opts:      &'b InternalOptions<Cmp, Policy, Codecs>,
    mut_opts:  &'b InternallyMutableOptions<FS, Policy, Pool>,
    read_opts: InternalReadOptions,
    decoders:  &'b mut Codecs::Decoders,
}

/// Should be used after guaranteeing that `self.iter.sstable.is_some()`.
///
/// This macro calls `next` or `prev` on `self.iter.sstable_iter()`, and if the result is `Some`,
/// that entry is returned.
///
/// This uses a small amount of `unsafe` code for Polonius, so this macro should be kept internal
/// to this code.
macro_rules! maybe_return_entry {
    ($self:expr, $sstable:expr) => {
        {
            let entry = if NEXT {
                $self.iter.sstable_iter
                    .next($sstable, $self.opts, $self.mut_opts, $self.read_opts, $self.decoders)?
            } else {
                $self.iter.sstable_iter
                    .prev($sstable, $self.opts, $self.mut_opts, $self.read_opts, $self.decoders)?
            };

            if let Some(entry) = entry {
                // In this branch, `self.level_file_iter` and `self.sstable_iter` are `valid()`.

                #[cfg(not(feature = "polonius"))]
                #[allow(
                    clippy::undocumented_unsafe_blocks,
                    reason = "stripped by macro application",
                )]
                // SAFETY: the code compiles under Polonius, so Rust's aliasing and ownership rules
                // are satisfied.
                let entry = unsafe {
                    ::std::mem::transmute::<
                        EncodedInternalEntry<'_>,
                        EncodedInternalEntry<'_>,
                    >(entry)
                };

                return Ok(Some(entry));
            }
        }
    };
}

macro_rules! set_table {
    ($self:expr, $sstable:expr) => {
        {
            let sstable = read_sstable(
                $self.opts,
                $self.mut_opts,
                $self.read_opts,
                $self.decoders,
                $self.iter.manifest_number,
                $sstable.file_number(),
                $sstable.file_size(),
            )?;
            $self.iter.sstable_iter.set(&sstable);
            $self.iter.sstable.insert(sstable)
        }
    };
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, FS, Cmp, Policy, Codecs, Pool>
    DisjointLevelIterWithOpts<'a, '_, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    fn next_or_prev<const NEXT: bool>(
        &mut self,
    ) -> Result<Option<EncodedInternalEntry<'a>>, RwErrorKindAlias<FS, Cmp, Codecs>> {
        if let Some(sstable) = &self.iter.sstable {
            maybe_return_entry!(self, sstable);
        }

        // Either `self.sstable_iter` is not initialized, or calling `next` or `prev` made it
        // `!valid()`.
        self.next_or_prev_fallback::<NEXT>()
    }

    /// Assuming that `self.sstable_iter` is either not initialized or not `valid()`, get either the
    /// next entry of the next nonempty table, or the previous entry of the previous nonempty
    /// table, depending on whether `NEXT` is true or false.
    ///
    /// After this call, `self.sstable_iter` is either not initialized, or is initialized
    /// and `valid()`. Additionally, `self.level_file_iter` is `valid()` iff `self.sstable_iter`
    /// is initialized and valid.
    fn next_or_prev_fallback<const NEXT: bool>(
        &mut self,
    ) -> Result<Option<EncodedInternalEntry<'a>>, RwErrorKindAlias<FS, Cmp, Codecs>> {
        loop {
            let new_file = if NEXT {
                self.iter.level_file_iter.next(self.version)
            } else {
                self.iter.level_file_iter.prev(self.version)
            };

            let Some(table_file) = new_file else { break };
            let sstable = set_table!(self, table_file);

            maybe_return_entry!(self, sstable);
        }

        // In this branch, `self.index_iter` is `!valid()`.
        self.iter.sstable_iter.clear();
        self.iter.sstable = None;
        Ok(None)
    }

    fn seek_bound<const GEQ: bool>(
        &mut self,
        bound: InternalKey<'_>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        if GEQ {
            self.iter.level_file_iter.seek(self.version, &self.opts.cmp, bound);
        } else {
            self.iter.level_file_iter.seek_before(self.version, &self.opts.cmp, bound);
        }

        let mut current_file = self.iter.level_file_iter.current(self.version);

        while let Some(table_file) = current_file {
            let sstable = set_table!(self, table_file);

            if GEQ {
                self.iter.sstable_iter.seek(
                    sstable,
                    self.opts,
                    self.mut_opts,
                    self.read_opts,
                    self.decoders,
                    bound,
                )?;
            } else {
                self.iter.sstable_iter.seek_before(
                    sstable,
                    self.opts,
                    self.mut_opts,
                    self.read_opts,
                    self.decoders,
                    bound,
                )?;
            }

            if self.iter.sstable_iter.valid() {
                // In this branch, `self.level_file_iter` and `self.sstable_iter` are `valid()`.
                return Ok(());
            } else {
                current_file = if GEQ {
                    self.iter.level_file_iter.next(self.version)
                } else {
                    self.iter.level_file_iter.prev(self.version)
                };
            }
        }

        // In this branch, we seeked too far forwards or backwards;
        // `self.level_file_iter` is `!valid()`, and we make `self.sstable_iter` be not initialized.
        self.iter.sstable_iter.clear();
        self.iter.sstable = None;
        Ok(())
    }

    #[expect(dead_code, reason = "this part of the interface is unused")]
    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.iter.valid()
    }

    pub fn next(
        &mut self,
    ) -> Result<Option<EncodedInternalEntry<'a>>, RwErrorKindAlias<FS, Cmp, Codecs>> {
        self.next_or_prev::<true>()
    }

    pub fn prev(
        &mut self,
    ) -> Result<Option<EncodedInternalEntry<'a>>, RwErrorKindAlias<FS, Cmp, Codecs>> {
        self.next_or_prev::<false>()
    }

    pub fn seek(
        &mut self,
        lower_bound: InternalKey<'_>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        self.seek_bound::<true>(lower_bound)
    }

    pub fn seek_before(
        &mut self,
        strict_upper_bound: InternalKey<'_>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        self.seek_bound::<false>(strict_upper_bound)
    }

    pub fn seek_to_first(&mut self) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        self.iter.reset(self.version);
        self.next()?;
        Ok(())
    }

    pub fn seek_to_last(&mut self) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>> {
        self.iter.reset(self.version);
        self.prev()?;
        Ok(())
    }
}

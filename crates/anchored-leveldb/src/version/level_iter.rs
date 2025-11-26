#![cfg_attr(
    not(feature = "polonius"),
    expect(unsafe_code, reason = "needed to perform Polonius-style lifetime extension"),
)]

use std::path::PathBuf;
use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::{
    file_tracking::Level,
    leveldb_iter::InternalIterator,
    table_file::read_table::InternalOptionalTableIter,
};
use crate::{
    format::{EncodedInternalEntry, LookupKey},
    leveldb_generics::{
        LdbContainer, LdbFsCell, LdbPooledBuffer, LdbReadTableOptions,
        LdbTableContainer, LevelDBGenerics,
    },
    inner_leveldb::{DBSharedAccess, DBWriteImpl},
};
use super::{file_iter::DisjointLevelFileIter, version_struct::Version};


/// Concatenating iterator over all the table files in a certain nonzero [`Level`]
/// (whose files do not have overlapping key ranges).
pub(crate) struct DisjointLevelIter<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> {
    /// Invariants:
    /// - if the `table_iter` is valid, then this whole iterator must be `valid()`.
    /// - the `table_iter` should be valid (and has a table file open) if and only if
    ///   `level_file_iter` is valid (and at the position of the opened table file).
    ///
    /// If `table_iter` becomes `!valid()`, then a new table file should be retrieved from
    /// `level_file_iter`, if possible. Note that `table_iter` should be cleared before attempting
    /// to open a new table file, for performance reasons.
    table_iter:      InternalOptionalTableIter<LDBG, WriteImpl>,
    level_file_iter: DisjointLevelFileIter<LDBG::Refcounted>,
}

/// Should be used after guaranteeing that `self.table_iter.is_set()`.
///
/// This macro calls `next` or `prev` on `self.table_iter()`, and if the result is `Some`,
/// that entry is returned.
///
/// This uses a small amount of `unsafe` code for Polonius, so this macro should be kept internal
/// to this code.
macro_rules! maybe_return_entry {
    ($self:expr) => {
        let entry = if NEXT {
            $self.table_iter.next()
        } else {
            $self.table_iter.prev()
        };

        if let Some(entry) = entry {
            // In this branch, `self.level_file_iter` and `self.table_iter` are `valid()`.

            // SAFETY: the code compiles under Polonius, so Rust's aliasing and ownership rules are
            // satisfied.
            #[cfg(not(feature = "polonius"))]
            #[allow(clippy::undocumented_unsafe_blocks, reason = "stripped by macro application")]
            let entry = unsafe { entry.extend_lifetime() };

            return Some(entry);
        }
    };
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> DisjointLevelIter<LDBG, WriteImpl> {
   #[must_use]
    pub fn new_disjoint(
        shared_data: DBSharedAccess<LDBG, WriteImpl>,
        version:     LdbContainer<LDBG, Version<LDBG::Refcounted>>,
        level:       Level,
    ) -> Self {
        Self {
            table_iter:      InternalOptionalTableIter::<LDBG, WriteImpl>::new_empty(shared_data),
            level_file_iter: DisjointLevelFileIter::new(version, level),
        }
    }

    #[must_use]
    fn next_or_prev<const NEXT: bool>(&mut self) -> Option<EncodedInternalEntry<'_>> {
        if self.table_iter.is_set() {
            maybe_return_entry!(self);
        }

        // Either `self.table_iter` is not initialized, or calling `next` or `prev` made it
        // `!valid()`.
        self.next_or_prev_fallback::<NEXT>()
    }

    /// Assuming that `self.table_iter` is either not initialized or not `valid()`, get either the
    /// next entry of the next nonempty table, or the previous entry of the previous nonempty
    /// table, depending on whether `NEXT` is true or false.
    ///
    /// After this call, `self.table_iter` is either not initialized, or is initialized
    /// and `valid()`. Additionally, `self.level_file_iter` is `valid()` iff `self.table_iter`
    /// is initialized and valid.
    #[must_use]
    fn next_or_prev_fallback<const NEXT: bool>(&mut self) -> Option<EncodedInternalEntry<'_>> {
        loop {
            let new_file = if NEXT {
                self.level_file_iter.next()
            } else {
                self.level_file_iter.prev()
            };

            let Some(table_file) = new_file else { break };
            self.table_iter.set(table_file.file_number(), table_file.file_size());

            maybe_return_entry!(self);

            // TODO: if `entry` is `None`, then the table file referenced by `self.table_iter`
            // is empty, which likely indicates corruption.
        }

        // In this branch, `self.index_iter` is `!valid()`.
        self.table_iter.clear();
        None
    }

    fn seek_bound<const GEQ: bool>(&mut self, bound: LookupKey<'_>) {
        if GEQ {
            self.level_file_iter.seek(self.table_iter.comparator(), bound);
        } else {
            self.level_file_iter.seek_before(self.table_iter.comparator(), bound);
        }

        let mut current_file = self.level_file_iter.current();

        while let Some(table_file) = current_file {
            self.table_iter.set(table_file.file_number(), table_file.file_size());

            if GEQ {
                self.table_iter.seek(bound);
            } else {
                self.table_iter.seek_before(bound);
            }

            if self.table_iter.valid() {
                // In this branch, `self.level_file_iter` and `self.table_iter` are `valid()`.
                return;
            } else {
                current_file = if GEQ {
                    self.level_file_iter.next()
                } else {
                    self.level_file_iter.prev()
                };
            }
        }

        // In this branch, we seeked too far forwards or backwards;
        // `self.level_file_iter` is `!valid()`, and we make `self.table_iter` be not initialized.
        self.table_iter.clear();
    }
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InternalIterator<LDBG::Cmp>
for DisjointLevelIter<LDBG, WriteImpl>
{
    fn valid(&self) -> bool {
        // `self.table_iter` is initialized if and only if `self.table_iter`
        // and `self.level_file_iter` are both `valid()`.
        self.table_iter.is_set()
    }

    fn next(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.next_or_prev::<true>()
    }

    fn current(&self) -> Option<EncodedInternalEntry<'_>> {
        self.table_iter.current()
    }

    fn prev(&mut self) -> Option<EncodedInternalEntry<'_>> {
        self.next_or_prev::<false>()
    }

    fn reset(&mut self) {
        // After these calls, `self.table_iter` is not initialized and `self.level_file_iter`
        // is `!valid()`, so the invariants are satisfied.
        self.table_iter.clear();
        self.level_file_iter.reset();
    }

    fn seek(&mut self, min_bound: LookupKey<'_>) {
        self.seek_bound::<true>(min_bound);
    }

    fn seek_before(&mut self, strict_upper_bound: LookupKey<'_>) {
        self.seek_bound::<false>(strict_upper_bound);
    }

    fn seek_to_first(&mut self) {
        self.reset();
        self.next();
    }

    fn seek_to_last(&mut self) {
        self.reset();
        self.prev();
    }
}

impl<LDBG, WriteImpl> Debug for DisjointLevelIter<LDBG, WriteImpl>
where
    LDBG:                    LevelDBGenerics,
    LDBG::Cmp:               Debug,
    LdbPooledBuffer<LDBG>:   Debug,
    LdbTableContainer<LDBG>: Debug,
    WriteImpl:               DBWriteImpl<LDBG>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DisjointLevelIter")
            .field("table_iter",      &self.table_iter)
            .field("level_file_iter", &self.level_file_iter)
            .finish()
    }
}

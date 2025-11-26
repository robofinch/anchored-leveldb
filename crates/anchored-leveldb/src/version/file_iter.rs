use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::{containers::RefcountedFamily, format::LookupKey};
use crate::{
    file_tracking::{Level, RefcountedFileMetadata},
    table_traits::{InternalComparator, LevelDBComparator},
};
use super::version_struct::Version;


pub(super) struct DisjointLevelFileIter<Refcounted: RefcountedFamily> {
    version:         Refcounted::Container<Version<Refcounted>>,
    /// This is not mutated during iteration.
    level:           Level,
    /// The length of the list of files in the indicated `Level` serves as a `None` niche.
    /// Note that _no greater value_ than that length should be in this field.
    index:           usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> DisjointLevelFileIter<Refcounted> {
    #[must_use]
    pub fn new(version: Refcounted::Container<Version<Refcounted>>, level: Level) -> Self {
        let level_files_len = version.level_files(level).inner().len();
        Self {
            version,
            level,
            index: level_files_len,
        }
    }

    #[must_use]
    pub fn valid(&self) -> bool {
        self.index < self.version.level_files(self.level).inner().len()
    }

    #[must_use]
    pub fn next(&mut self) -> Option<&RefcountedFileMetadata<Refcounted>> {
        let level_files = self.version.level_files(self.level).inner();

        if self.index < level_files.len() {
            self.index += 1;
        } else {
            self.index = 0;
        }

        level_files.get(self.index)
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<&RefcountedFileMetadata<Refcounted>> {
        self.version.level_files(self.level).inner().get(self.index)
    }

    #[must_use]
    pub fn prev(&mut self) -> Option<&RefcountedFileMetadata<Refcounted>> {
        let level_files = self.version.level_files(self.level).inner();

        if let Some(decremented) = self.index.checked_sub(1) {
            self.index = decremented;
            // Note that this is guaranteed to be `Some`, but there's no real need to convince
            // the compiler of that.
            level_files.get(self.index)
        } else {
            self.index = level_files.len();
            None
        }
    }

    pub fn reset(&mut self) {
        self.index = self.version.level_files(self.level).inner().len();
    }

    /// Seek to the least file which contains keys at or after the provided `min_bound`.
    pub fn seek<Cmp: LevelDBComparator>(
        &mut self,
        cmp:       &InternalComparator<Cmp>,
        min_bound: LookupKey<'_>,
    ) {
        #![expect(clippy::or_fun_call, reason = "`.inner()` and `.len()` are extremely cheap")]

        let level_files = self.version.level_files(self.level);

        self.index = level_files.find_file_disjoint(cmp, min_bound.internal_key())
            .unwrap_or(level_files.inner().len());
    }

    /// Seek to the greatest file which contains keys strictly before `strict_upper_bound`.
    pub fn seek_before<Cmp: LevelDBComparator>(
        &mut self,
        cmp:                &InternalComparator<Cmp>,
        strict_upper_bound: LookupKey<'_>,
    ) {
        let strict_upper_bound = strict_upper_bound.internal_key();
        let level_files = self.version.level_files(self.level);

        self.index = if let Some(file_idx) = level_files
            .find_file_disjoint(cmp, strict_upper_bound)
        {
            if level_files.inner()
                .get(file_idx)
                .is_some_and(|file| {
                    cmp.cmp_internal(file.smallest_key(), strict_upper_bound).is_lt()
                })
            {
                // This file is partially before (and partially after) `strict_upper_bound`.
                file_idx
            } else if let Some(prev_idx) = file_idx.checked_sub(1) {
                // `file_idx` was entirely at or after `strict_upper_bound`, so the previous
                // file would be the greatest file which has keys before `strict_upper_bound`.
                prev_idx
            } else {
                // Every file is entirely at or after `strict_upper_bound`, so seek to `None`.
                level_files.inner().len()
            }
        } else {
            // `strict_upper_bound` is after every file, so we should seek to the greatest file
            // (if there is one), whose index would be `level_files.inner().len() - 1`.
            level_files.inner().len().saturating_sub(1)
        };
    }
}

impl<Refcounted: RefcountedFamily> Debug for DisjointLevelFileIter<Refcounted> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DisjointLevelFileIter")
            .field("version_files", &self.version.level_files(self.level))
            .field("index",         &self.valid().then_some(self.index))
            .finish()
    }
}

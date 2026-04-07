use std::sync::Arc;

use anchored_skiplist::Comparator as _;

use crate::{
    file_tracking::FileMetadata,
    pub_traits::cmp_and_policy::LevelDBComparator,
    pub_typed_bytes::NonZeroLevel,
    table_format::InternalComparator,
    typed_bytes::InternalKey,
    version::Version,
};


#[derive(Debug)]
pub(super) struct DisjointLevelFileIter {
    /// This is not mutated during iteration.
    level:   NonZeroLevel,
    /// The length of the list of files in the indicated `level` serves as a `None` niche.
    /// Note that _no greater value_ than that length should be in this field.
    index:   usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl DisjointLevelFileIter {
    #[must_use]
    pub fn new(version: &Version, level: NonZeroLevel) -> Self {
        let level_files_len = version.level_files(level.as_level()).inner().len();
        Self {
            level,
            index: level_files_len,
        }
    }

    /// `version` must be the same `Version` provided to [`Self::new`].
    #[expect(dead_code, reason = "this part of the interface is unused")]
    #[must_use]
    pub fn valid(&self, version: &Version) -> bool {
        self.index < version.level_files(self.level.as_level()).inner().len()
    }

    /// `version` must be the same `Version` provided to [`Self::new`].
    #[must_use]
    pub fn next<'a>(&mut self, version: &'a Version) -> Option<&'a Arc<FileMetadata>> {
        let level_files = version.level_files(self.level.as_level()).inner();

        if self.index < level_files.len() {
            self.index += 1;
        } else {
            self.index = 0;
        }

        level_files.get(self.index)
    }

    /// `version` must be the same `Version` provided to [`Self::new`].
    #[inline]
    #[must_use]
    pub fn current<'a>(&self, version: &'a Version) -> Option<&'a Arc<FileMetadata>> {
        version.level_files(self.level.as_level()).inner().get(self.index)
    }

    /// `version` must be the same `Version` provided to [`Self::new`].
    #[must_use]
    pub fn prev<'a>(&mut self, version: &'a Version) -> Option<&'a Arc<FileMetadata>> {
        let level_files = version.level_files(self.level.as_level()).inner();

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

    /// `version` must be the same `Version` provided to [`Self::new`].
    pub fn reset(&mut self, version: &Version) {
        self.index = version.level_files(self.level.as_level()).inner().len();
    }

    /// Seek to the least file which contains keys at or after the provided `lower_bound`.
    ///
    /// `version` must be the same `Version` provided to [`Self::new`].
    pub fn seek<Cmp: LevelDBComparator>(
        &mut self,
        version:     &Version,
        cmp:         &InternalComparator<Cmp>,
        lower_bound: InternalKey<'_>,
    ) {
        #![expect(clippy::or_fun_call, reason = "`.inner()` and `.len()` are extremely cheap")]

        let level_files = version.level_files(self.level.as_level());

        self.index = level_files.find_file_disjoint(cmp, lower_bound)
            .unwrap_or(level_files.inner().len());
    }

    /// Seek to the greatest file which contains keys strictly before `strict_upper_bound`.
    ///
    /// `version` must be the same `Version` provided to [`Self::new`].
    pub fn seek_before<Cmp: LevelDBComparator>(
        &mut self,
        version:            &Version,
        cmp:                &InternalComparator<Cmp>,
        strict_upper_bound: InternalKey<'_>,
    ) {
        let level_files = version.level_files(self.level.as_level());

        self.index = if let Some(file_idx) = level_files
            .find_file_disjoint(cmp, strict_upper_bound)
        {
            if level_files.inner()
                .get(file_idx)
                .is_some_and(|file| {
                    cmp.cmp(file.smallest_key(), strict_upper_bound).is_lt()
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

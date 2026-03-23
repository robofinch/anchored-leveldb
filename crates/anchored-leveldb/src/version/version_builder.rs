use std::sync::Arc;
use std::collections::{HashMap, HashSet};

use anchored_skiplist::Comparator as _;

use crate::{
    all_errors::types::CorruptedVersionError,
    compaction::OptionalCompactionPointer,
    pub_traits::cmp_and_policy::LevelDBComparator,
    table_format::InternalComparator,
};
use crate::{
    file_tracking::{FileMetadata, OwnedSortedFiles},
    pub_typed_bytes::{FileNumber, IndexLevel as _, Level, NonZeroLevel, NUM_LEVELS_USIZE},
};
use super::{edit::VersionEdit, version_struct::Version};


#[derive(Debug)]
pub(super) struct VersionBuilder<'a> {
    base_version:             Arc<Version>,
    vset_compaction_pointers: &'a mut [OptionalCompactionPointer; NUM_LEVELS_USIZE.get()],
    added_files:              [Vec<Arc<FileMetadata>>; NUM_LEVELS_USIZE.get()],
    deleted_files:            [HashSet<FileNumber>; NUM_LEVELS_USIZE.get()],
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> VersionBuilder<'a> {
    #[must_use]
    pub fn new(
        base_version:             Arc<Version>,
        vset_compaction_pointers: &'a mut [OptionalCompactionPointer; NUM_LEVELS_USIZE.get()],
    ) -> Self {
        Self {
            base_version,
            vset_compaction_pointers,
            added_files:   Default::default(),
            deleted_files: Default::default(),
        }
    }

    /// Apply `edit.deleted_files`, and `edit.added_files` to the under-construction [`Version`],
    /// and apply `edit.compaction_pointers` to the `vset_compaction_pointers` data provided
    /// to this builder.
    pub fn apply(&mut self, edit: &VersionEdit) {
        for (level, compaction_pointer) in &edit.compaction_pointers {
            self.vset_compaction_pointers
                .infallible_index_mut(*level)
                .set(compaction_pointer.internal_key());
        }

        for (level, deleted_file) in &edit.deleted_files {
            self.deleted_files.infallible_index_mut(*level).insert(*deleted_file);
        }

        for (level, added_file) in &edit.added_files {
            self.deleted_files.infallible_index_mut(*level).remove(&added_file.file_number());
            self.added_files.infallible_index_mut(*level).push(Arc::clone(added_file));
        }
    }

    pub fn finish<Cmp: LevelDBComparator>(
        &mut self,
        cmp:                 &InternalComparator<Cmp>,
        check_built_version: CheckBuiltVersion,
    ) -> Result<Version, CorruptedVersionError> {
        let version_files = Level::ALL_LEVELS.map(|level| {
            OwnedSortedFiles::merge(
                self.base_version.level_files(level),
                self.added_files.infallible_index_mut(level),
                self.deleted_files.infallible_index(level),
                cmp,
            )
        });

        if let CheckBuiltVersion::Check { next_file_number } = check_built_version {
            Self::check_corruption(&version_files, cmp, next_file_number)?;
        }

        Ok(Version::new(version_files))
    }

    fn check_corruption<Cmp: LevelDBComparator>(
        version_files:    &[OwnedSortedFiles; NUM_LEVELS_USIZE.get()],
        cmp:              &InternalComparator<Cmp>,
        next_file_number: FileNumber,
    ) -> Result<(), CorruptedVersionError> {
        let num_files = version_files.iter()
            .fold(0, |sum, files| sum + files.borrowed().inner().len());
        let mut seen_files = HashMap::with_capacity(num_files);

        let level0_files = version_files.infallible_index(Level::ZERO).borrowed().inner();

        for file in level0_files {
            let file_number = file.file_number();
            if let Some(old_level) = seen_files.insert(file.file_number(), Level::ZERO) {
                return Err(CorruptedVersionError::FileOccursTwice(
                    file_number,
                    old_level,
                    Level::ZERO,
                ));
            }

            if file_number >= next_file_number {
                return Err(CorruptedVersionError::TableFileNumberTooLarge(
                    file_number,
                    next_file_number,
                ));
            }
        }

        for nonzero_level in NonZeroLevel::NONZERO_LEVELS {
            let level = nonzero_level.as_level();
            // Currently, rust-analyzer cannot figure out the type of `level_n_files` without help
            // (thoughh rustc can).
            let level_n_files: &OwnedSortedFiles = version_files.infallible_index(level);
            let level_n_files = level_n_files.borrowed().inner();
            let mut level_n_windows = level_n_files.windows(2);

            while let Some([file, next_file]) = level_n_windows.next() {
                let file_number = file.file_number();
                if let Some(old_level) = seen_files.insert(file_number, level) {
                    return Err(CorruptedVersionError::FileOccursTwice(
                        file_number,
                        old_level,
                        Level::ZERO,
                    ));
                }

                if cmp.cmp(file.largest_key(), next_file.smallest_key()).is_le() {
                    return Err(CorruptedVersionError::OverlappingFileKeyRanges(
                        file_number,
                        next_file.file_number(),
                        nonzero_level,
                    ));
                }

                if file_number >= next_file_number {
                    return Err(CorruptedVersionError::TableFileNumberTooLarge(
                        file_number,
                        next_file_number,
                    ));
                }
            }

            if let Some(file) = level_n_files.last() {
                let file_number = file.file_number();
                if let Some(old_level) = seen_files.insert(file_number, level) {
                    return Err(CorruptedVersionError::FileOccursTwice(
                        file_number,
                        old_level,
                        Level::ZERO,
                    ));
                }

                if file_number >= next_file_number {
                    return Err(CorruptedVersionError::TableFileNumberTooLarge(
                        file_number,
                        next_file_number,
                    ));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CheckBuiltVersion {
    Check {
        next_file_number: FileNumber,
    },
    NoCheck,
}

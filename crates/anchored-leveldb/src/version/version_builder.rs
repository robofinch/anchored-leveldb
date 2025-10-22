use std::collections::HashSet;

use clone_behavior::MirroredClone as _;
use generic_container::FragileContainer as _;

use crate::containers::RefcountedFamily;
use crate::{
    compaction::OptionalCompactionPointer,
    file_tracking::{IndexLevel as _, Level, OwnedSortedFiles, RefcountedFileMetadata},
    format::{FileNumber, NUM_LEVELS_USIZE},
    table_traits::{adapters::InternalComparator, trait_equivalents::LevelDBComparator},
};
use super::{version_edit::VersionEdit, version_struct::Version};


// Because this internal struct is transient and implementing `Debug` (or similar) would be tedious,
// `Debug` is not implemented.
pub(super) struct VersionBuilder<'a, Refcounted: RefcountedFamily> {
    base_version:             Refcounted::Container<Version<Refcounted>>,
    vset_compaction_pointers: &'a mut [OptionalCompactionPointer; NUM_LEVELS_USIZE],
    added_files:              [Vec<RefcountedFileMetadata<Refcounted>>; NUM_LEVELS_USIZE],
    deleted_files:            [HashSet<FileNumber>; NUM_LEVELS_USIZE],
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, Refcounted: RefcountedFamily> VersionBuilder<'a, Refcounted> {
    #[must_use]
    pub fn new(
        base_version:             Refcounted::Container<Version<Refcounted>>,
        vset_compaction_pointers: &'a mut [OptionalCompactionPointer; NUM_LEVELS_USIZE],
    ) -> Self {
        Self {
            base_version,
            vset_compaction_pointers,
            added_files:   Default::default(),
            deleted_files: Default::default(),
        }
    }

    pub fn apply(&mut self, edit: &VersionEdit<Refcounted>) {
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
            self.added_files.infallible_index_mut(*level).push(added_file.mirrored_clone());
        }
    }

    #[expect(clippy::unnecessary_wraps, reason = "planned to optionally check for errors")]
    pub fn finish<Cmp: LevelDBComparator>(
        &mut self,
        cmp: &InternalComparator<Cmp>,
    ) -> Result<Version<Refcounted>, ()> {
        let version_files = Level::ALL_LEVELS.map(|level| {
            let base_version: &Version<Refcounted> = &self.base_version.get_ref();
            OwnedSortedFiles::merge(
                base_version.level_files(level),
                self.added_files.infallible_index_mut(level),
                self.deleted_files.infallible_index(level),
                cmp,
            )
        });

        // TODO: perform paranoid error checking on the version
        Ok(Version::new(version_files))
    }
}

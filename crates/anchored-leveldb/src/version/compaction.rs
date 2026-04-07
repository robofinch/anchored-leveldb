#![expect(clippy::indexing_slicing, reason = "TODO: justify each case.")]

use std::{ops::Range, sync::Arc};
use std::slice::Iter as SliceIter;

use anchored_skiplist::Comparator as _;

use crate::{
    options::InternalOptions,
    pub_traits::cmp_and_policy::LevelDBComparator,
    table_format::InternalComparator,
    typed_bytes::InternalKey,
};
use crate::{
    file_tracking::{FileMetadata, StartSeekCompaction},
    pub_typed_bytes::{
        IndexMiddleLevel as _, IndexNonZeroLevel as _, Level, MiddleLevel, NonZeroLevel,
    },
};
use super::{
    edit::VersionEdit,
    version_struct::Version,
    set::VersionSet,
};


#[derive(Debug)]
pub(crate) enum CompactionInputsCow<'a> {
    Owned(Vec<&'a Arc<FileMetadata>>),
    /// Must be sorted and disjoint.
    Borrowed(&'a [Arc<FileMetadata>]),
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl CompactionInputsCow<'_> {
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(this)    => this.len(),
            Self::Borrowed(this) => this.len(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct StartCompaction<'a> {
    pub parent_level:            NonZeroLevel,
    // max_output_size:         FileSize,
    // input_version:           &'a Arc<Version>,
    // edit:                    VersionEdit,
    pub base_inputs:             CompactionInputsCow<'a>,
    pub parent_inputs:           &'a [Arc<FileMetadata>],
    pub grandparents:            &'a [Arc<FileMetadata>],
    // ancestor_iters:          [DisjointLevelFileIter; NUM_MIDDLE_LEVELS_USIZE.get()],
    // empty_output:            bool,
    // grandparent_overlap:     u64,
    /// The corresponding `max_grandparent_overlap` setting (or `u64::MAX` if `parent_level` is
    /// the highest level).
    pub max_grandparent_overlap: u64,
    pub smallest_key:            InternalKey<'a>,
    pub largest_key:             InternalKey<'a>,
    pub is_manual:               bool,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> StartCompaction<'a> {
    pub fn new_manual_compaction<File, Cmp: LevelDBComparator, Policy, Codecs>(
        opts:         &InternalOptions<Cmp, Policy, Codecs>,
        version_set:  &mut VersionSet<File>,
        version_edit: &mut VersionEdit,
        version:      &'a Version,
        parent_level: NonZeroLevel,
        lower_bound:  Option<InternalKey<'_>>,
        upper_bound:  Option<InternalKey<'_>>,
    ) -> Option<Self> {
        let base_level = parent_level.prev_level();

        // TODO: How to handle range compactions whose upper and lower bounds are equal?
        // TODO: where to perform this bounds checking? Should it be done earlier?
        // TODO: explicitly document on any method taking `lower_bound` and `upper_bound`
        // how they respond when `lower_bound > upper_bound`.
        if let (Some(lower_bound), Some(upper_bound)) = (lower_bound, upper_bound) {
            if opts.cmp.cmp(lower_bound, upper_bound).is_gt() {
                return None;
            }
        }

        if let Some(base_level) = base_level.try_as_nonzero_level() {
            let base_files = version.level_files(base_level.as_level());
            let file_range = base_files
                .get_overlapping_files_disjoint(&opts.cmp, lower_bound, upper_bound);

            // The range could be arbitrarily large; we need to avoid compacting too much in a
            // single compaction (though we don't have the freedom to do that for level 0).
            // Limit to around a single input file.
            let limit = opts.max_sstable_sizes.infallible_index_nonzero(base_level).0;

            let mut total_size = 0;
            let mut end_index = file_range.start;

            for file in &base_files.inner()[file_range.clone()] {
                let last_file = total_size > limit;
                end_index += 1;
                total_size = total_size.saturating_add(file.file_size().0);
                if last_file {
                    break;
                }
            }

            // We've truncated down to `file_range.start..end_index`, which we know is in-bounds.
            let base_input_range = file_range.start..end_index;

            if base_input_range.start == base_input_range.end {
                None
            } else {
                Some(Self::new_nonzero_compaction(
                    opts,
                    version_set,
                    version_edit,
                    version,
                    base_level,
                    parent_level,
                    base_input_range,
                    true,
                ))
            }
        } else {
            let mut base_inputs = Vec::new();

            version.level_files(base_level).get_overlapping_files(
                &opts.cmp,
                lower_bound.map(|key| key.0),
                upper_bound.map(|key| key.0),
                &mut base_inputs,
            );

            if base_inputs.is_empty() {
                None
            } else {
                Some(Self::new_zero_compaction(
                    opts,
                    version_set,
                    version_edit,
                    version,
                    base_inputs,
                    true,
                ))
            }
        }
    }

    pub fn new_size_compaction<File, Cmp: LevelDBComparator, Policy, Codecs>(
        opts:         &InternalOptions<Cmp, Policy, Codecs>,
        version_set:  &mut VersionSet<File>,
        version_edit: &mut VersionEdit,
        version:      &'a Version,
        parent_level: NonZeroLevel,
    ) -> Option<Self> {
        let base_level = parent_level.prev_level();
        let base_files = version.level_files(base_level);

        // For below code to be correct, we need to confirm that index `0` is in-bounds.
        // A **size compaction** should realistically never be triggered on an empty level,
        // but might as well check for the edge case.
        if base_files.inner().is_empty() {
            // TODO: Log this oddity.
            return None;
        }

        // The compaction pointer corresponding to the base level indicates where the next
        // size compaction should begin.
        let pointer = version_set.compaction_pointer(base_level);

        let base_input_index = if let Some(pointer) = pointer {
            if base_level == Level::ZERO {
                // Nothing faster than linear search.
                base_files.find_file_strict(&opts.cmp, pointer)
            } else {
                // Use binary search.
                base_files.find_file_strict_disjoint(&opts.cmp, pointer)
            }
        } else {
            None
        };

        // Wrap around to (or start at, if `pointer` was `None`) the beginning of the key space.
        // We checked that index `0` is in-bounds.
        let base_input_index = base_input_index.unwrap_or(0);

        Some(Self::new_single_compaction(
            opts,
            version_set,
            version_edit,
            version,
            parent_level,
            base_input_index,
        ))
    }

    pub fn new_seek_compaction<File, Cmp: LevelDBComparator, Policy, Codecs>(
        opts:            &InternalOptions<Cmp, Policy, Codecs>,
        version_set:     &mut VersionSet<File>,
        version_edit:    &mut VersionEdit,
        version:         &'a Version,
        seek_compaction: StartSeekCompaction,
    ) -> Self {
        Self::new_single_compaction(
            opts,
            version_set,
            version_edit,
            version,
            seek_compaction.level,
            seek_compaction.file,
        )
    }

    /// Finish setting up a compaction beginning with a single base input file.
    ///
    /// Used only for size and seek compactions, not manual compactions.
    ///
    /// # Correctness
    /// It is required that `base_input_index` is an in-bounds index of a file on level
    /// `parent_level.prev_level()`.
    fn new_single_compaction<File, Cmp: LevelDBComparator, Policy, Codecs>(
        opts:             &InternalOptions<Cmp, Policy, Codecs>,
        version_set:      &mut VersionSet<File>,
        version_edit:     &mut VersionEdit,
        version:          &'a Version,
        parent_level:     NonZeroLevel,
        base_input_index: usize,
    ) -> Self {
        let base_level = parent_level.prev_level();
        let base_files = version.level_files(base_level);

        if let Some(base_level) = base_level.try_as_nonzero_level() {
            Self::new_nonzero_compaction(
                opts,
                version_set,
                version_edit,
                version,
                base_level,
                parent_level,
                // Indexing by `[base_input_index..base_input_index+1]` gives precisely the file at
                // the in-bounds index `base_input_index`.
                base_input_index..base_input_index+1,
                false,
            )
        } else {
            // We're doing a compaction of level zero. We need to gather all overlapping files.
            let mut base_inputs = Vec::new();
            let base_input = &base_files.inner()[base_input_index];

            version.level_files(Level::ZERO).get_overlapping_files(
                &opts.cmp,
                Some(base_input.smallest_user_key()),
                Some(base_input.largest_user_key()),
                &mut base_inputs,
            );

            // Note that `base_inputs` will, at the very least, include `base_input`, so this
            // next call does not panic.
            Self::new_zero_compaction(
                opts,
                version_set,
                version_edit,
                version,
                base_inputs,
                false,
            )
        }
    }

    /// Finish setting up a compaction from level `0` to level `1`, given a `Vec` of initial
    /// inputs.
    ///
    /// The user key ranges of the provided level-0 inputs must be disjoint from the user key
    /// ranges of all excluded level-0 files. That is, a user key's level-0 data must either be
    /// entirely compacted or entirely untouched.
    ///
    /// # Correctness
    /// It is required that `base_inputs` is a nonempty `Vec` of level-0 file metadata.
    fn new_zero_compaction<File, Cmp: LevelDBComparator, Policy, Codecs>(
        opts:         &InternalOptions<Cmp, Policy, Codecs>,
        version_set:  &mut VersionSet<File>,
        version_edit: &mut VersionEdit,
        version:      &'a Version,
        base_inputs:  Vec<&'a Arc<FileMetadata>>,
        is_manual:    bool,
    ) -> Self {
        let base_files = version.level_files(Level::ZERO);
        // No need to add boundary inputs.

        let (parent_inputs, base_first, base_last) = Self::get_zero_parent_inputs(
            version,
            &opts.cmp,
            &base_inputs,
        );

        let mut compaction_base_inputs = base_inputs;
        let mut compaction_smallest = base_first;
        let mut compaction_largest = base_last;

        // Compute the full range of the compaction, and potentially expand it.
        if let ([parent_first, ..], [.., parent_last]) = (parent_inputs, parent_inputs) {
            let parent_first = parent_first.smallest_key();
            let parent_last = parent_last.largest_key();

            if opts.cmp.cmp(parent_first, base_first).is_lt() {
                compaction_smallest = parent_first;
            }
            if opts.cmp.cmp(base_last, parent_last).is_lt() {
                compaction_largest = parent_last;
            }

            // Try to expand the compaction without increasing the number of parent inputs.
            let mut expanded_inputs = Vec::new();
            base_files.get_overlapping_files(
                &opts.cmp,
                Some(compaction_smallest.0),
                Some(compaction_largest.0),
                &mut expanded_inputs,
            );

            let base_size = FileMetadata::total_file_size_ref(&compaction_base_inputs);
            let parent_size = FileMetadata::total_file_size(parent_inputs);
            let expanded_base_size = FileMetadata::total_file_size_ref(&expanded_inputs);

            let total_expanded_size = expanded_base_size.saturating_add(parent_size);
            let max_input_size = *opts.compaction
                .max_compaction_inputs
                .infallible_index_nonzero(NonZeroLevel::ONE);

            if expanded_base_size > base_size && total_expanded_size < max_input_size {
                let (
                    expanded_parents,
                    expanded_first,
                    expanded_last,
                ) = Self::get_zero_parent_inputs(
                    version,
                    &opts.cmp,
                    &expanded_inputs,
                );

                if parent_inputs.len() == expanded_parents.len() {
                    // No new parent inputs. Note that `base_inputs` is a subset of
                    // `expanded_inputs`, so everything overlapping `base_inputs` (and their
                    // boundary inputs) must be included in `expanded_parents`; therefore, these
                    // are literally the exact same slices.
                    compaction_base_inputs = expanded_inputs;
                    if opts.cmp.cmp(expanded_first, base_first).is_lt() {
                        compaction_smallest = expanded_first;
                    }
                    if opts.cmp.cmp(base_last, expanded_last).is_lt() {
                        compaction_largest = expanded_last;
                    }
                }
            }
        }

        let grandparent_level = MiddleLevel::ONE.next_level();

        let max_grandparent_overlap = *opts.compaction
            .max_grandparent_overlap
            .infallible_index_middle(MiddleLevel::ONE);

        let grandparent_files = version.level_files(grandparent_level.as_level());
        let grandparents = grandparent_files.get_overlapping_files_disjoint(
            &opts.cmp,
            Some(compaction_smallest),
            Some(compaction_largest),
        );
        let grandparents = &grandparent_files.inner()[grandparents];

        // Update the compaction pointer for the next size compaction. We match LevelDB's behavior
        // in always updating the pointer even for non-size compactions.
        // TODO: make this configurable, to let size compactions cycle on their own.
        version_set.set_compaction_pointer(Level::ZERO, compaction_largest);
        version_edit.compaction_pointers.push((Level::ZERO, compaction_largest.to_owned()));

        Self {
            parent_level: NonZeroLevel::ONE,
            base_inputs:  CompactionInputsCow::Owned(compaction_base_inputs),
            parent_inputs,
            grandparents,
            max_grandparent_overlap,
            smallest_key: compaction_smallest,
            largest_key:  compaction_largest,
            is_manual,
        }
    }

    /// Get parent inputs (including boundary inputs) of given base inputs, as well as the
    /// first and last base input key.
    ///
    /// (Yes, this is an ad-hoc helper function.)
    ///
    /// Note that adding boundary inputs for parents isn't necessary for correctness, but it is
    /// good for performance (reduce the fragmentation of keys across multiple files).
    ///
    /// # Correctness
    /// Requires that `base_inputs` consists of level-0 file metadata.
    ///
    /// # Panics
    /// Panics if `base_inputs` is empty.
    fn get_zero_parent_inputs<Cmp: LevelDBComparator>(
        version:     &'a Version,
        cmp:         &InternalComparator<Cmp>,
        base_inputs: &[&'a Arc<FileMetadata>],
    ) -> (&'a [Arc<FileMetadata>], InternalKey<'a>, InternalKey<'a>) {
        let mut base_input_iter = base_inputs.iter();

        let first_file = base_input_iter
            .next()
            .expect("`new_zero_compaction` must only be used for nonempty compactions");

        let mut first = first_file.smallest_key();
        let mut last = first_file.largest_key();

        for file in base_input_iter {
            let file_smallest = file.smallest_key();
            let file_largest = file.largest_key();

            if cmp.cmp(file_smallest, first).is_lt() {
                first = file_smallest;
            }
            if cmp.cmp(last, file_largest).is_lt() {
                last = file_largest;
            }
        }

        // Get parent inputs (including boundary inputs).
        // NOTE: Adding boundary inputs for parents isn't necessary for correctness, but it is
        // good for performance (reduce the fragmentation of keys across multiple files).
        let parent_files = version.level_files(NonZeroLevel::ONE.as_level());
        let parent_inputs = parent_files.get_overlapping_inputs_disjoint(cmp, first, last);
        (parent_inputs, first, last)
    }

    /// Finish setting up a compaction between two nonzero levels, given the range of initial
    /// base inputs.
    ///
    /// # Correctness
    /// It is required that `base_level` is the level before `parent_level` and that
    /// `input_range` is a nonempty in-bounds range of files on level `base_level` in `version`.
    #[expect(clippy::too_many_arguments, reason = "internal helper function")]
    fn new_nonzero_compaction<File, Cmp: LevelDBComparator, Policy, Codecs>(
        opts:             &InternalOptions<Cmp, Policy, Codecs>,
        version_set:      &mut VersionSet<File>,
        version_edit:     &mut VersionEdit,
        version:          &'a Version,
        base_level:       NonZeroLevel,
        parent_level:     NonZeroLevel,
        base_input_range: Range<usize>,
        is_manual:        bool,
    ) -> Self {
        let base_files = version.level_files(base_level.as_level());

        // Add boundary inputs
        let base_start = base_input_range.start;
        let base_end = base_files.add_boundary_inputs_disjoint(
            &opts.cmp,
            base_input_range.end,
        );
        // Compute the range of keys in the base inputs in order to get overlapping parents.
        // Since `base_input_range` is required to be nonempty, none of this panics.
        let base_inputs = &base_files.inner()[base_start..base_end];

        let (parent_inputs, base_first, base_last) = Self::get_nonzero_parent_inputs(
            version,
            &opts.cmp,
            base_inputs,
            parent_level,
        );

        let mut compaction_base_inputs = base_inputs;
        let mut compaction_smallest = base_first;
        let mut compaction_largest = base_last;

        // Compute the full range of the compaction, and potentially expand it.
        if let ([parent_first, ..], [.., parent_last]) = (parent_inputs, parent_inputs) {
            let parent_first = parent_first.smallest_key();
            let parent_last = parent_last.largest_key();

            if opts.cmp.cmp(parent_first, base_first).is_lt() {
                compaction_smallest = parent_first;
            }
            if opts.cmp.cmp(base_last, parent_last).is_lt() {
                compaction_largest = parent_last;
            }

            // Try to expand the compaction without increasing the number of parent inputs.
            let expanded_base_inputs = base_files.get_overlapping_inputs_disjoint(
                &opts.cmp,
                compaction_smallest,
                compaction_largest,
            );

            let base_size = FileMetadata::total_file_size(base_inputs);
            let parent_size = FileMetadata::total_file_size(parent_inputs);
            let expanded_base_size = FileMetadata::total_file_size(expanded_base_inputs);

            let total_expanded_size = expanded_base_size.saturating_add(parent_size);
            let max_input_size = *opts.compaction
                .max_compaction_inputs
                .infallible_index_nonzero(parent_level);

            if expanded_base_size > base_size && total_expanded_size < max_input_size {
                let (
                    expanded_parents,
                    expanded_first,
                    expanded_last,
                ) = Self::get_nonzero_parent_inputs(
                    version,
                    &opts.cmp,
                    expanded_base_inputs,
                    parent_level,
                );

                if parent_inputs.len() == expanded_parents.len() {
                    // No new parent inputs. Note that `base_inputs` is a subset of
                    // `expanded_inputs`, so everything overlapping `base_inputs` (and their
                    // boundary inputs) must be included in `expanded_parents`; therefore, these
                    // are literally the exact same slices.
                    compaction_base_inputs = expanded_base_inputs;
                    if opts.cmp.cmp(expanded_first, base_first).is_lt() {
                        compaction_smallest = expanded_first;
                    }
                    if opts.cmp.cmp(base_last, expanded_last).is_lt() {
                        compaction_largest = expanded_last;
                    }
                }
            }
        }

        let (
            grandparents,
            max_grandparent_overlap,
        ) = if let Some(parent_level) = parent_level.try_as_middle_level() {
            let grandparent_level = parent_level.next_level();

            let max_grandparent_overlap = *opts.compaction
                .max_grandparent_overlap
                .infallible_index_middle(parent_level);

            let grandparent_files = version.level_files(grandparent_level.as_level());
            let grandparents = grandparent_files.get_overlapping_files_disjoint(
                &opts.cmp,
                Some(compaction_smallest),
                Some(compaction_largest),
            );
            (&grandparent_files.inner()[grandparents], max_grandparent_overlap)
        } else {
            ([].as_slice(), u64::MAX)
        };

        // Update the compaction pointer for the next seek compaction. We match LevelDB's behavior
        // in always updating the pointer even for non-seek compactions.
        version_set.set_compaction_pointer(base_level.as_level(), compaction_largest);
        version_edit
            .compaction_pointers
            .push((base_level.as_level(), compaction_largest.to_owned()));

        // We can use the `Borrowed` variant because the base inputs are a subslice of
        // a nonzero level's `SortedFiles`, and they are therefore sorted and disjoint.
        Self {
            parent_level,
            base_inputs:  CompactionInputsCow::Borrowed(compaction_base_inputs),
            parent_inputs,
            grandparents,
            max_grandparent_overlap,
            smallest_key: compaction_smallest,
            largest_key:  compaction_largest,
            is_manual,
        }
    }

    /// Get parent inputs (including boundary inputs) of given base inputs, as well as the
    /// first and last base input key.
    ///
    /// (Yes, this is an ad-hoc helper function.)
    ///
    /// Note that adding boundary inputs for parents isn't necessary for correctness, but it is
    /// good for performance (reduce the fragmentation of keys across multiple files).
    ///
    /// # Correctness
    /// Requires that `base_inputs` is sorted and consists of files with disjoint key ranges.
    /// That is, it is required that the base level is nonzero.
    ///
    /// # Panics
    /// Panics if `base_inputs` is empty.
    fn get_nonzero_parent_inputs<Cmp: LevelDBComparator>(
        version:      &'a Version,
        cmp:          &InternalComparator<Cmp>,
        base_inputs:  &'a [Arc<FileMetadata>],
        parent_level: NonZeroLevel,
    ) -> (&'a [Arc<FileMetadata>], InternalKey<'a>, InternalKey<'a>) {
        let ([first, ..], [.., last]) = (base_inputs, base_inputs) else {
            panic!("`new_nonzero_compaction` must only be used for nonempty compactions");
        };
        let first = first.smallest_key();
        let last = last.largest_key();

        // Get parent inputs (including boundary inputs).
        // NOTE: Adding boundary inputs for parents isn't necessary for correctness, but it is
        // good for performance (reduce the fragmentation of keys across multiple files).
        let parent_files = version.level_files(parent_level.as_level());
        let parent_inputs = parent_files.get_overlapping_inputs_disjoint(cmp, first, last);
        (parent_inputs, first, last)
    }

    pub fn trivial_move(&self) -> Option<&'a Arc<FileMetadata>> {
        if !self.parent_inputs.is_empty() {
            return None;
        }

        let sole_input = match &self.base_inputs {
            CompactionInputsCow::Owned(owned) => {
                let &[sole_input] = &**owned else {
                    return None;
                };
                sole_input
            }
            CompactionInputsCow::Borrowed(borrowed) => {
                let &[sole_input] = borrowed else {
                    return None;
                };
                sole_input
            }
        };

        // Avoid a move if there is lots of overlapping grandparent data. Otherwise, the move
        // could create a parent file that will require a very expensive merge later on.
        let grandparent_overlap = FileMetadata::total_file_size(self.grandparents);
        if grandparent_overlap > self.max_grandparent_overlap {
            None
        } else {
            Some(sole_input)
        }
    }

    /// Add all inputs to this compaction as delete operations to `edit`.
    pub fn add_input_deletions(&self, edit: &mut VersionEdit) {
        let base_level = self.parent_level.prev_level();

        match &self.base_inputs {
            CompactionInputsCow::Owned(base_inputs) => {
                edit.deleted_files.extend(base_inputs.iter().map(|meta| {
                    (base_level, meta.file_number())
                }));
            }
            CompactionInputsCow::Borrowed(base_inputs) => {
                edit.deleted_files.extend(base_inputs.iter().map(|meta| {
                    (base_level, meta.file_number())
                }));
            }
        }

        let parent_level = self.parent_level.as_level();
        let parent_inputs = self.parent_inputs.iter().map(|meta| {
            (parent_level, meta.file_number())
        });
        edit.deleted_files.extend(parent_inputs);
    }

    pub fn into_active(self) -> ActiveCompaction<'a> {
        ActiveCompaction {
            parent_inputs:           self.parent_inputs,
            base_inputs:             self.base_inputs,
            parent_level:            self.parent_level,
            grandparents:            self.grandparents.iter(),
            smallest_key:            self.smallest_key,
            largest_key:             self.largest_key,
            has_nonempty_output:     false,
            current_overlap:         0,
            max_grandparent_overlap: self.max_grandparent_overlap,
        }
    }
}

pub(crate) struct ActiveCompaction<'a> {
    parent_level:  NonZeroLevel,
    // max_output_size:         FileSize,
    // input_version:           &'a Arc<Version>,
    // edit:                    VersionEdit,
    base_inputs:   CompactionInputsCow<'a>,
    parent_inputs: &'a [Arc<FileMetadata>],
    grandparents:  SliceIter<'a, Arc<FileMetadata>>,
    // ancestor_iters:          [DisjointLevelFileIter; NUM_MIDDLE_LEVELS_USIZE.get()],
    // empty_output:            bool,
    // grandparent_overlap:     u64,
    // max_grandparent_overlap: u64,
    smallest_key:  InternalKey<'a>,
    largest_key:   InternalKey<'a>,


    // parent_level:            NonZeroLevel,
    // max_output_size:         FileSize,
    // input_version:           &'a Version,
    // edit:                    VersionEdit,
    // base_inputs:             SortedFiles<'a>,
    // parent_inputs:           SortedFiles<'a>,
    // grandparents:            SliceIter<'a, Arc<FileMetadata>>,
    // // ancestor_iters:          [DisjointLevelFileIter; NUM_MIDDLE_LEVELS_USIZE.get()],
    has_nonempty_output:     bool,
    /// The size (in bytes) of the overlap between the current output file and grandparent files.
    current_overlap:         u64,
    max_grandparent_overlap: u64,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl ActiveCompaction<'_> {
    pub fn should_stop_before<Cmp: LevelDBComparator>(
        &mut self,
        cmp: &InternalComparator<Cmp>,
        key: InternalKey<'_>,
    ) -> bool {
        while let Some(grandparent) = self.grandparents.as_slice().first() {
            if cmp.cmp(grandparent.largest_key(), key).is_lt() {
                if self.has_nonempty_output {
                    self.current_overlap = self.current_overlap
                        .saturating_add(grandparent.file_size().0);
                }
            } else {
                break;
            }

            self.grandparents.next();
        }
        self.has_nonempty_output = true;

        if self.current_overlap > self.max_grandparent_overlap {
            // Too much grandparent overlap for the current output file; start a new one.
            self.current_overlap = 0;
            true
        } else {
            false
        }
    }

    // /// Whether the key is in any greater level than the destination level.
    // ///
    // /// (I.e., whether a grandparent level or deeper contains the given user key.)
    // pub fn ancestor_contains_key<Cmp: LevelDBComparator>(
    //     &self,
    //     cmp: &'a InternalComparator<Cmp>,
    //     key: UserKey<'_>,
    // ) -> bool {
    //     let mut level = self.parent_level;

    //     while let Some(next) = level.as_level().next_level() {
    //         level = next;
    //         let files = self.input_version.level_files(next.as_level());
    //         if files.key_overlaps_file_disjoint(cmp, key) {
    //             return true;
    //         }
    //     }

    //     false
    // }
}

use std::{cmp::Ordering, collections::HashSet};

use clone_behavior::MirroredClone as _;

use crate::{containers::RefcountedFamily, public_format::EntryType};
use crate::{
    format::{FileNumber, InternalKey, SequenceNumber, UserKey},
    table_traits::{adapters::InternalComparator, trait_equivalents::LevelDBComparator},
};
use super::{
    file_metadata::{FileMetadata, RefcountedFileMetadata},
    level::{IndexLevel, Level},
};


/// The table file metadata in [`OwnedSortedFiles`] must be loosely sorted in increasing order of
/// their smallest internal keys.
///
/// In case of a tie, order does not matter.
pub(super) struct OwnedSortedFiles<Refcounted: RefcountedFamily>(
    Vec<RefcountedFileMetadata<Refcounted>>,
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> OwnedSortedFiles<Refcounted> {
    #[inline]
    #[must_use]
    pub const fn new_empty() -> Self {
        Self(Vec::new())
    }

    /// Returns the sorted union of a sorted collection of files (to be precise, of table file
    /// metadata) with an unsorted list of added files, excluding any files in the `deleted` set.
    ///
    /// An unstable sort is used to sort the `added` slice.
    ///
    /// # Incorrect Behavior on Bad Input
    /// If a different comparator (than the provided `cmp`) was previously given to
    /// [`OwnedSortedFiles::merge`] to produce a [`OwnedSortedFiles`] struct from which `base`
    /// was derived, then the returned result is unspecified and meaningless.
    #[must_use]
    pub fn merge<Cmp: LevelDBComparator>(
        base:    SortedFiles<'_, Refcounted>,
        added:   &mut [RefcountedFileMetadata<Refcounted>],
        deleted: &HashSet<FileNumber>,
        cmp:     &InternalComparator<Cmp>,
    ) -> Self {
        // `added` needs to be sorted in increasing order by smallest internal key, and order in
        // case of a tie does not matter.
        // We sort here instead of having `added` be a `BTreeSet` in order to make it more
        // convenient to use a custom comparator.
        added.sort_unstable_by(move |lhs, rhs| {
            cmp.cmp_internal(lhs.smallest_key(), rhs.smallest_key())
        });

        // The deleted files are always a subset of the input files in actual usage.
        // Though, theoretically, this might allocate slightly too little and end up triggering a
        // reallocation, and then end up allocating slightly too much.
        let max_len = base.inner().len() + added.len();
        let mut merged_files = Vec::with_capacity(max_len.saturating_sub(deleted.len()));

        let mut base_files = base.inner().iter();
        // Loop invariant: `base_file`, if `Some`, has **not** been consumed yet.
        let mut base_file = base_files.next();

        for added_file in added.iter() {
            // Add anything in `base_files` which is strictly less than `added_file`.
            // Then, we proceed to the next element of `added`, up until `added` is exhausted;
            // `base_files` may or may not be exhausted at that point.
            'inner: while let Some(file) = base_file {
                if cmp.cmp_internal(
                    file.smallest_key(),
                    added_file.smallest_key(),
                ) == Ordering::Less {
                    if !deleted.contains(&file.file_number()) {
                        merged_files.push(file.mirrored_clone());
                    }

                    // We consumed `base_file`, so we need to grab the next element.
                    base_file = base_files.next();
                } else {
                    // We still need to attempt to add `base_file` to `merged_files`, so it
                    // hasn't been consumed.
                    break 'inner;
                }
            }

            // Since `added` is sorted, this is pushed after any lesser value in `added`.
            // Thanks to the above `'inner` while-loop, and since `base_files` is sorted, we
            // push `added_file` after any lesser value in `base_files`.
            if !deleted.contains(&added_file.file_number()) {
                merged_files.push(added_file.mirrored_clone());
            }
        }

        if let Some(base_file) = base_file {
            // We didn't consume `base_file` yet.
            if !deleted.contains(&base_file.file_number()) {
                merged_files.push(base_file.mirrored_clone());
            }

            // There can only be remaining base files if `base_file` was `Some`,
            // thus why the for-loop can be inside this `if` block.
            for remaining_base_file in base_files {
                if !deleted.contains(&remaining_base_file.file_number()) {
                    merged_files.push(remaining_base_file.mirrored_clone());
                }
            }
        }

        // We ensured that `merged_files` is sorted.
        Self(merged_files)
    }

    #[inline]
    #[must_use]
    pub fn borrowed(&self) -> SortedFiles<'_, Refcounted> {
        SortedFiles(&self.0)
    }
}

/// The table file metadata in [`SortedFiles`] must be loosely sorted in increasing order of their
/// smallest internal keys.
///
/// In case of a tie, order does not matter.
pub(super) struct SortedFiles<'a, Refcounted: RefcountedFamily>(
    &'a [RefcountedFileMetadata<Refcounted>],
);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a, Refcounted: RefcountedFamily> SortedFiles<'a, Refcounted> {
    #[inline]
    #[must_use]
    pub fn inner(self) -> &'a [RefcountedFileMetadata<Refcounted>] {
        self.0
    }

    #[must_use]
    pub fn total_file_size(self) -> u64 {
        // Note that the sum will surely not overflow a u64, no database has 16 exabytes.
        self.0.iter().map(|file| file.file_size()).sum()
    }

    /// If the table files referenced by this [`SortedFiles`] struct have key ranges which do not
    /// overlap, then this function returns the least `index` such that `key` is less than or equal
    /// to `self.inner()[index].largest_key()`.
    ///
    /// If `key` is strictly greater than the keys in any of the files of the [`SortedFiles`]
    /// struct, then `None` is returned.
    ///
    /// The phrase "key range" refers to the interval from the smallest key of a file to the
    /// largest key of that file, with respect to the provided comparator.
    ///
    /// # Incorrect Behavior on Bad Input
    /// If two or more of the files of the struct have overlapping key ranges, or if
    /// a different comparator (than the provided `cmp`) was given to [`OwnedSortedFiles::merge`]
    /// to produce a [`OwnedSortedFiles`] struct from which `self` was derived, then
    /// the returned result is unspecified and meaningless.
    #[must_use]
    pub fn find_file_disjoint<Cmp: LevelDBComparator>(
        self,
        cmp: &InternalComparator<Cmp>,
        key: InternalKey<'_>,
    ) -> Option<usize> {
        // The files are sorted in increasing order with respect to their smallest keys, but if the
        // files are completely disjoint, it follows that they are _also_ in sorted order with
        // respect to their largest keys.
        match self.0.binary_search_by(|file| cmp.cmp_internal(file.largest_key(), key)) {
            Ok(exact_match) => Some(exact_match),
            Err(next_file) => {
                if next_file < self.0.len() {
                    // We have that `next_file` is the smallest entry
                    Some(next_file)
                } else {
                    None
                }
            }
        }
    }

    /// Determine whether some table file referenced by this [`SortedFiles`] struct overlaps the
    /// range from `lower_bound` to `upper_bound`, inclusive, with respect to `cmp`.
    ///
    /// A `None` bound indicates either an absolute minimum lower bound or an absolute maximum
    /// upper bound.
    ///
    /// For this function to act properly, it is required that the files of this [`SortedFiles`]
    /// struct have key ranges which do not overlap.
    ///
    /// The phrase "key range" refers to the interval from the smallest key of a file to the
    /// largest key of that file, with respect to the provided comparator.
    ///
    /// # Incorrect Behavior on Bad Input
    /// If two or more of the files of the struct have overlapping key ranges, or if
    /// a different comparator (than the provided `cmp`) was given to [`OwnedSortedFiles::merge`]
    /// to produce a [`OwnedSortedFiles`] struct from which `self` was derived, then
    /// the returned result is unspecified and meaningless.
    pub fn range_overlaps_file_disjoint<Cmp: LevelDBComparator>(
        self,
        cmp:         &InternalComparator<Cmp>,
        lower_bound: Option<UserKey<'_>>,
        upper_bound: Option<UserKey<'_>>,
    ) -> bool {
        // Find the least `index` such that `lower_bound` is less than
        // `self.inner()[index].largest_key()`, or `None` if there is no such index.
        // There may be no such index if either `lower_bound` is `None` (indicating an absolute
        // minimum) but there are no files (otherwise index `0` would work), or if `lower_bound`
        // is `Some(_)` and is strictly greater than every file.
        let index = if let Some(lower) = lower_bound {
            // Make the longest possible range by choosing the least internal key
            let internal_lower = InternalKey {
                user_key: lower,
                sequence_number: SequenceNumber::MAX_SEQUENCE_NUMBER,
                entry_type:      EntryType::MAX_TYPE,
            };

            if let Some(index) = self.find_file_disjoint(cmp, internal_lower) {
                index
            } else {
                return false;
            }
        } else {
            if self.0.is_empty() {
                return false;
            } else {
                0
            }
        };

        // `lower_bound` is less than or equal to the largest key in the indicated file,
        // so as long as `upper_bound` (and thus the entire range) does not come strictly before
        // the file, there' overlap.
        !upper_bound_is_before_file(cmp, upper_bound, &self.0[index])
    }

    /// Determine whether some table file referenced by this [`SortedFiles`] struct overlaps the
    /// range from `lower_bound` to `upper_bound`, inclusive, with respect to `cmp`.
    ///
    /// A `None` bound indicates either an absolute minimum lower bound or an absolute maximum
    /// upper bound.
    ///
    /// # Incorrect Behavior on Bad Input
    /// If a different comparator (than the provided `cmp`) was given to [`OwnedSortedFiles::merge`]
    /// to produce a [`OwnedSortedFiles`] struct from which `self` was derived, then
    /// the returned result is unspecified and meaningless.
    pub fn range_overlaps_file<Cmp: LevelDBComparator>(
        self,
        cmp:         &InternalComparator<Cmp>,
        lower_bound: Option<UserKey<'_>>,
        upper_bound: Option<UserKey<'_>>,
    ) -> bool {
        self.0.iter().any(move |file| {
            if file_is_before_lower_bound(cmp, file, lower_bound)
                || upper_bound_is_before_file(cmp, upper_bound, file)
            {
                // No overlap; file is either completely before or completely after the range.
                false
            } else {
                // Overlap
                true
            }
        })
    }
}

impl<Refcounted: RefcountedFamily> Clone for SortedFiles<'_, Refcounted> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<Refcounted: RefcountedFamily> Copy for SortedFiles<'_, Refcounted> {}

#[must_use]
fn file_is_before_lower_bound<Cmp: LevelDBComparator>(
    cmp:         &InternalComparator<Cmp>,
    file:        &FileMetadata,
    lower_bound: Option<UserKey<'_>>,
) -> bool {
    if let Some(lower) = lower_bound {
        // Check if the file's upper bound comes strictly before the given lower bound
        cmp.cmp_user(file.largest_user_key(), lower) == Ordering::Less
    } else {
        // A `None` lower bound indicates an absolute minimum, so `file` cannot come before it.
        false
    }
}

#[must_use]
fn upper_bound_is_before_file<Cmp: LevelDBComparator>(
    cmp:         &InternalComparator<Cmp>,
    upper_bound: Option<UserKey<'_>>,
    file:        &FileMetadata,
) -> bool {
    if let Some(upper) = upper_bound {
        // Check if the given upper bound comes strictly before the file's lower bound
        cmp.cmp_user(upper, file.smallest_user_key()) == Ordering::Less
    } else {
        // A `None` upper bound indicates an absolute maximum, so `file` cannot come after it.
        false
    }
}

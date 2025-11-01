use std::{cmp::Reverse as ReverseOrder, ops::Deref, path::Path};

use clone_behavior::MirroredClone as _;
use generic_container::{FragileContainer as _, FragileTryContainer as _};


use crate::{containers::RefcountedFamily, table_file::get_table};
use crate::{
    file_tracking::{
        IndexLevel as _, Level, MaybeSeekCompaction,
        OwnedSortedFiles, RefcountedFileMetadata, SortedFiles,
    },
    format::{
        EncodedInternalKey, GRANDPARENT_OVERLAP_SIZE_FACTOR, InternalKey, L0_COMPACTION_TRIGGER,
        LookupKey, MAX_LEVEL_FOR_COMPACTION, NUM_LEVELS_USIZE, UserKey,
    },
    leveldb_generics::{LdbFsCell, LdbReadTableOptions, LdbTableEntry, LevelDBGenerics},
    table_traits::{adapters::InternalComparator, trait_equivalents::LevelDBComparator},
};


pub(crate) struct CurrentVersion<Refcounted: RefcountedFamily> {
    version:         Refcounted::Container<Version<Refcounted>>,
    /// If a certain level in the database is too large (that is, the total size in bytes of
    /// all files associated with a certain [`Level`] is too large), a "size compaction" needs to
    /// be performed in order to move data to a higher and larger level.
    size_compaction: Option<Level>,
    seek_compaction: MaybeSeekCompaction<Refcounted>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> CurrentVersion<Refcounted> {
    #[inline]
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            version:         Refcounted::Container::new_container(Version::new_empty()),
            size_compaction: None,
            seek_compaction: MaybeSeekCompaction::None,
        }
    }

    #[must_use]
    pub fn new(version: Version<Refcounted>) -> Self {
        let size_compaction = version.compute_size_compaction();
        Self {
            version:         Refcounted::Container::new_container(version),
            size_compaction,
            seek_compaction: MaybeSeekCompaction::None,
        }
    }

    pub fn set(&mut self, version: Version<Refcounted>) {
        self.size_compaction = version.compute_size_compaction();
        self.version = Refcounted::Container::new_container(version);
        self.seek_compaction = MaybeSeekCompaction::None;
    }

    #[must_use]
    pub const fn refcounted_version(&self) -> &Refcounted::Container<Version<Refcounted>> {
        &self.version
    }

    #[must_use]
    pub const fn size_compaction(&self) -> Option<Level> {
        self.size_compaction
    }

    #[must_use]
    pub const fn seek_compaction(&self) -> Option<(Level, &RefcountedFileMetadata<Refcounted>)> {
        match &self.seek_compaction {
            MaybeSeekCompaction::Some(level, file) => Some((*level, file)),
            MaybeSeekCompaction::None              => None,
        }
    }

    #[must_use]
    pub fn needs_seek_compaction(
        &mut self,
        maybe_current_version: &Refcounted::Container<Version<Refcounted>>,
        maybe_seek_compaction: MaybeSeekCompaction<Refcounted>,
    ) -> bool {
        if matches!(self.seek_compaction, MaybeSeekCompaction::None)
            && Refcounted::ptr_eq(&self.version, maybe_current_version)
        {
            // We didn't already note that we need a seek compaction, and it is indeed the current
            // version which needs a seek compaction.
            self.seek_compaction = maybe_seek_compaction;
        }

        matches!(self.seek_compaction, MaybeSeekCompaction::Some(_, _))
    }

    #[must_use]
    pub const fn needs_compaction(&self) -> bool {
        self.size_compaction.is_some()
            || matches!(self.seek_compaction, MaybeSeekCompaction::Some(_, _))
    }
}

impl<Refcounted: RefcountedFamily> Deref for CurrentVersion<Refcounted> {
    type Target = Version<Refcounted>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.version
    }
}

struct OldVersions<Refcounted: RefcountedFamily> {
    old_versions:       Vec<Refcounted::WeakContainer<Version<Refcounted>>>,
    collection_counter: usize,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> OldVersions<Refcounted> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            old_versions:       Vec::new(),
            collection_counter: 1,
        }
    }

    pub fn add_old_version(&mut self, version: &Refcounted::Container<Version<Refcounted>>) {
        self.maybe_collect_garbage();
        self.old_versions.push(Refcounted::downgrade(version));
        if self.collection_counter % 2 == 0 {
            self.collection_counter += 1;
        }
    }

    #[inline]
    pub fn live(&mut self) -> impl Iterator<Item = Refcounted::Container<Version<Refcounted>>> {
        self.maybe_collect_garbage();
        self.old_versions.iter().filter_map(Refcounted::upgrade)
    }

    fn maybe_collect_garbage(&mut self) {
        if let Some(decremented) = self.collection_counter.checked_sub(1) {
            self.collection_counter = decremented;
        } else {
            self.old_versions.retain(Refcounted::can_be_upgraded);
            {
                #![expect(clippy::integer_division, reason = "intentional")]
                self.collection_counter = self.old_versions.len() / 2;
            }
        }
    }
}

pub(crate) struct Version<Refcounted: RefcountedFamily> {
    files: [OwnedSortedFiles<Refcounted>; NUM_LEVELS_USIZE],
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> Version<Refcounted> {
    #[inline]
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            files: Default::default(),
        }
    }

    #[inline]
    #[must_use]
    pub(super) const fn new(files: [OwnedSortedFiles<Refcounted>; NUM_LEVELS_USIZE]) -> Self {
        Self {
            files,
        }
    }

    #[must_use]
    pub(super) fn level_files(&self, level: Level) -> SortedFiles<'_, Refcounted> {
        self.files.infallible_index(level).borrowed()
    }

    #[must_use]
    fn compute_size_compaction(&self) -> Option<Level> {
        #![expect(
            clippy::as_conversions,
            clippy::cast_precision_loss,
            clippy::float_arithmetic,
            reason = "precision is not critical for a heuristic",
        )]

        let num_l0_files = self.level_files(Level::ZERO).inner().len();

        let mut best_level = Level::ZERO;
        // Level 0 is bounded by number of files instead of size in bytes.
        let mut best_score = (num_l0_files as f64) / f64::from(L0_COMPACTION_TRIGGER);

        // The maximum for level 1 is 10 megabytes, for level 2 is 100 megabytes, and so on.
        let mut max_bytes_for_level = f64::from(1_u32 << 20_u8);

        for level in Level::nonzero_levels() {
            max_bytes_for_level *= 10_f64;

            let level_files = self.files.infallible_index(level).borrowed();
            let score = (level_files.total_file_size() as f64) / max_bytes_for_level;

            if score > best_score {
                best_level = level;
                best_score = score;
            }
        }

        if best_score >= 1_f64 {
            Some(best_level)
        } else {
            None
        }
    }

    pub fn get<LDBG: LevelDBGenerics>(
        &self,
        filesystem:   &LdbFsCell<LDBG>,
        db_directory: &Path,
        cmp:          &InternalComparator<LDBG::Cmp>,
        table_cache:  &LDBG::TableCache,
        read_opts:    &LdbReadTableOptions<LDBG>,
        lookup_key:   LookupKey<'_>,
    ) -> Result<(Option<LdbTableEntry<LDBG>>, MaybeSeekCompaction<Refcounted>), ()> {
        let mut seek_file: Option<(Level, &RefcountedFileMetadata<Refcounted>)> = None;
        let mut last_file_read: Option<(Level, &RefcountedFileMetadata<Refcounted>)> = None;

        // Called for each candidate file which might have the newest entry among those with the
        // lookup key's user key and a sequence number as old or older than the lookup key's
        // sequence number.
        macro_rules! try_get {
            ($level:expr, $file:expr) => {
                if seek_file.is_none() {
                    // If we read more than one file in the course of searching for the key,
                    // then record a seek on the first file read.
                    seek_file = last_file_read;
                }

                last_file_read = Some(($level, $file));
                {
                    let table_container = get_table::<LDBG>(
                        filesystem, db_directory, table_cache, read_opts.mirrored_clone(),
                        $file.file_number(), $file.file_size(),
                    )?;
                    let table_ref = table_container.get_ref();
                    let table_entry: Option<LdbTableEntry<LDBG>> = table_ref.get(
                        // TODO: I don't like the potential for a typo here
                        lookup_key.encoded_internal_key().0,
                    )?;
                    if let Some(table_entry) = table_entry {
                        let user_key = EncodedInternalKey(table_entry.key()).user_key()?;
                        if cmp.cmp_user(user_key, lookup_key.user_key()).is_eq() {
                            // TODO: check if the entry is for deletion.
                            // Will anything calling `Version::get` need to care about
                            // "not found at all" vs "found a tombstone"?
                            // If not, we should return Ok((None, _)) for deletion instead,
                            // so that the buffer is dropped immediately.
                            return Ok((
                                Some(table_entry),
                                MaybeSeekCompaction::record_seek(seek_file),
                            ));
                        }
                    }
                }
            };
        }

        let level_0_files = self.level_files(Level::ZERO).inner();
        let mut l0_candidates = Vec::with_capacity(level_0_files.len());
        for l0_file in level_0_files {
            // If `l0_file.largest_key() < lookup_key`, then nothing in the file would work;
            // either there's nothing with the correct user key, or the only entries for that user
            // key are too new.
            // Conversely, even if `lookup_key < l0_file.smallest_key()`,
            // if `l0_file.smallest_user_key() == lookup_key.user_key()`, then we'd want to read
            // the `l0_file.smallest_key()` entry since it's an older entry with the correct
            // user key; it might be the most-recent entry for that user key.
            // The last case is where `l0_file.smallest_user_key() < lookup_key.user_key()` and
            // `lookup_key <= l0_file.largest_key()`, and we'd also want to read the file.
            // Note that Google's leveldb compares user key for the upper bound, too, which
            // might perform unnecessary file reads.

            // Check that `l0_file.smallest_user_key() <= lookup_key.user_key()`
            // and `lookup_key <= l0_file.largest_key()`.
            if cmp.cmp_user(l0_file.smallest_user_key(), lookup_key.user_key()).is_le()
                && cmp.cmp_internal(lookup_key.internal_key(), l0_file.largest_key()).is_le()
            {
                l0_candidates.push(l0_file);
            }
        }

        // Sort with the largest (and newest) file number first (instead of smallest).
        l0_candidates.sort_unstable_by_key(|file| ReverseOrder(file.file_number()));

        for file in l0_candidates {
            try_get!(Level::ZERO, file);
        }

        for level in Level::nonzero_levels() {
            let files = self.files.infallible_index(level).borrowed();
            if let Some(file) = files.find_file_disjoint(cmp, lookup_key.internal_key()) {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "`find_file_disjoint` returns either a valid index or `None`",
                )]
                let file = &files.inner()[file];

                // We know that `file` is the first file in this level such that
                // `lookup_key.internal_key() <= file.largest_key()`.
                // We then only need to check that
                // `file.smallest_user_key() <= lookup_key.user_key()`. Note that any later file
                // in this level which managed to satisfy both conditions would need to have the
                // same user key and lower sequence numbers than `file.largest_key()`, in which
                // case we wouldn't need to check the old overwritten entries anyway.
                if cmp.cmp_user(file.smallest_user_key(), lookup_key.user_key()).is_le()
                {
                    try_get!(level, file);
                }
            }
        }

        Ok((None, MaybeSeekCompaction::record_seek(seek_file)))
    }

    pub fn record_read_sample<Cmp: LevelDBComparator>(
        &self,
        cmp: &InternalComparator<Cmp>,
        key: InternalKey<'_>,
    ) -> MaybeSeekCompaction<Refcounted> {
        let mut last_file_read: Option<(Level, &RefcountedFileMetadata<Refcounted>)> = None;

        // Called for each file in nonzero levels which might have the newest entry among
        // those with the lookup key's user key and a sequence number as old or older than the
        // lookup key's sequence number,
        // and for each file in level 0 which contains entries with the same user key as the
        // lookup key.
        macro_rules! maybe_record_seek {
            ($level:expr, $file:expr) => {
                // If we see that more than one file overlaps the key, then record a seek.
                if last_file_read.is_some() {
                    return MaybeSeekCompaction::record_seek(last_file_read);
                } else {
                    last_file_read = Some(($level, $file));
                }
            };
        }

        // We need not sort the level-0 files. Technically, it would affect which file would
        // have a seek recorded, but since all candidate files would overlap each other,
        // triggering a compaction would compact all of them anyway. `Version::get` does actually
        // care since it has to return the correct `LdbTableEntry`, and the order in which the
        // files are checked matters because of sequence numbers, but recording a read sample
        // isn't so sensitive.

        for l0_file in self.level_files(Level::ZERO).inner() {
            // Check if the user keys of the level-0 file overlap the sample key.
            if cmp.cmp_user(l0_file.smallest_user_key(), key.user_key).is_le()
                && cmp.cmp_user(key.user_key, l0_file.largest_user_key()).is_le()
            {
                maybe_record_seek!(Level::ZERO, l0_file);
            }
        }

        for level in Level::nonzero_levels() {
            let files = self.files.infallible_index(level).borrowed();
            if let Some(file) = files.find_file_disjoint(cmp, key) {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "`find_file_disjoint` returns either a valid index or `None`",
                )]
                let file = &files.inner()[file];

                // See `Version::get` for why this comparison is here.
                if cmp.cmp_user(file.smallest_user_key(), key.user_key).is_le()
                {
                    maybe_record_seek!(level, file);
                }
            }
        }

        MaybeSeekCompaction::None
    }

    pub fn levels_for_range_compaction<Cmp: LevelDBComparator>(
        &self,
        cmp:         &InternalComparator<Cmp>,
        lower_bound: Option<UserKey<'_>>,
        upper_bound: Option<UserKey<'_>>,
    ) -> impl ExactSizeIterator<Item = Level> + DoubleEndedIterator + 'static {
        // TODO(maybe-opt): we always compact the memtable and level 0. Compacting a few extra files
        // when a compaction is manually requested (thus indicating willingness to do a slow
        // background process, for the sake of improving space efficiency and read performance)
        // should be fine. But we may want to test the performance of checking for overlap
        // in the memtable and level-0 files.

        let mut nonzero_levels = Level::nonzero_levels();

        // Find the greatest level which overlaps the compaction range.
        // We'll compact everything from the memtable up to and including that level.
        // TODO(compact): maybe compact everything normally up to and excluding that level, and
        // then for that last level, compact it without putting the new files in a higher level.
        // The potential to quickly push every file into level-6 worries me; by having compactions
        // which don't push files to a higher level, the shape of the LSM tree could be better
        // preserved, I think.
        while let Some(level) = nonzero_levels.next_back() {
            let level_files = self.files.infallible_index(level).borrowed();
            // Note that this is O(log n) in the number of files on that level.
            // Also, `_disjoint` is allowed because files in nonzero levels do not overlap.
            if level_files.range_overlaps_file_disjoint(cmp, lower_bound, upper_bound) {
                return Level::ZERO.inclusive_range(level)
            }
        }

        Level::ZERO.inclusive_range(Level::ZERO)
    }

    /// Check which level a memtable with the indicated least and greatest user keys
    /// should preferably be placed into, after compaction.
    ///
    /// Using this function is optional; it is always acceptable to place a compacted memtable in
    /// level 0. The returned level is an upper bound on which levels the memtable may be
    /// compacted into.
    ///
    /// The `max_file_size` setting is assumed to not exceed [`MAXIMUM_MAX_FILE_SIZE_OPTION`].
    ///
    /// [`MAXIMUM_MAX_FILE_SIZE_OPTION`]: crate::format::MAXIMUM_MAX_FILE_SIZE_OPTION
    pub fn level_for_compacted_memtable<Cmp: LevelDBComparator>(
        &self,
        max_file_size:  u64,
        cmp:            &InternalComparator<Cmp>,
        memtable_lower: UserKey<'_>,
        memtable_upper: UserKey<'_>,
    ) -> Level {
        let lower = Some(memtable_lower);
        let upper = Some(memtable_upper);

        if self.level_files(Level::ZERO).range_overlaps_file(cmp, lower, upper) {
            Level::ZERO
        } else {
            // Push the memtable to the next level only if there's no overlap with the next level
            // and it doesn't overlap too many grandparents.
            let mut level = Level::ZERO;
            let mut overlaps = Vec::new();

            while let Some(next_level) = level.next_level() {
                if next_level.inner() > MAX_LEVEL_FOR_COMPACTION {
                    // Don't push it to the next level.
                    break;
                }

                let next_level_files = self.files.infallible_index(next_level).borrowed();
                // `_disjoint` is allowed because files in nonzero levels do not overlap.
                if next_level_files.range_overlaps_file_disjoint(cmp, lower, upper) {
                    // We can't push it to the next level.
                    break;
                }

                if let Some(grandparent_level) = next_level.next_level() {
                    overlaps.clear();
                    // `_disjoint` is allowed because files in nonzero levels do not overlap.
                    // And we clear `overlaps`.
                    self.files.infallible_index(grandparent_level).borrowed()
                        .get_overlapping_files_disjoint(cmp, lower, upper, &mut overlaps);
                    let total_file_size: u64 = overlaps.iter().map(|file| file.file_size()).sum();
                    if total_file_size > GRANDPARENT_OVERLAP_SIZE_FACTOR * max_file_size {
                        // Don't push it to the next level.
                        break;
                    }
                }

                // Push the memtable to the next level.
                level = next_level;
            }

            level
        }
    }

    // pub fn add_iterators<LDBG: LevelDBGenerics<Refcounted = Refcounted>>(
    //     &self,
    //     filesystem:   &LdbFsCell<LDBG>,
    //     db_directory: &Path,
    //     table_cache:  &LDBG::TableCache,
    //     read_opts:    LdbReadTableOptions<LDBG>,
    //     iters:        &mut Vec<InternalIter<LDBG>>,
    // ) {
    // // push a TableIter for each level-0 file, and a DisjointLevelIter for each nonzero level
    //     todo!()
    // }

    // TODO: debug impl
    // debug_with_text_keys(&self, f) -> FmtResult
    // debug_with_numeric_keys(&self, f) -> FmtResult
    // debug_with<K>(&self, f, debug_key: K) -> FmtResult
}

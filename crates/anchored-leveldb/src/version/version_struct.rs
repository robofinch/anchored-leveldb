use std::{cmp::Reverse as ReverseOrder, sync::Arc};

use clone_behavior::FastMirroredClone;

use anchored_skiplist::Comparator as _;
use anchored_vfs::LevelDBFilesystem;

use crate::{
    all_errors::aliases::RwErrorKindAlias,
    file_tracking::{FileMetadata, OwnedSortedFiles, SortedFiles, StartSeekCompaction},
    internal_iters::IterToMerge,
    options::{
        InternallyMutableOptions, InternalOptions, InternalReadOptions,
        pub_options::SizeCompactionOptions,
    },
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{
        FileNumber, FileOffset, IndexLevel as _, IndexMiddleLevel as _, Level, MiddleLevel,
        NonZeroLevel, NUM_LEVELS_USIZE,
    },
    sstable::{TableEntry, TableIter},
    table_file::read_sstable,
    table_format::InternalComparator,
    typed_bytes::{InternalKey, LookupKey, UserKey},
};
use super::level_iter::DisjointLevelIter;


/// A collection of table files (`.ldb` and `.sst` files).
///
/// Aside from [`AtomicU32`] data in [`FileMetadata`], a [`Version`] is immutable after
/// its construction. Which [`Version`] is the current version of the database can be changed
/// by database compactions, which operate on the current database version and produce a new
/// [`Version`].
#[derive(Debug)]
pub(crate) struct Version {
    files: [OwnedSortedFiles; NUM_LEVELS_USIZE.get()],
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl Version {
    #[inline]
    #[must_use]
    pub fn new_empty() -> Self {
        Self {
            files: Default::default(),
        }
    }

    #[inline]
    #[must_use]
    pub(super) const fn new(files: [OwnedSortedFiles; NUM_LEVELS_USIZE.get()]) -> Self {
        Self {
            files,
        }
    }

    #[inline]
    #[must_use]
    pub(super) const fn inner(&self) -> &[OwnedSortedFiles; NUM_LEVELS_USIZE.get()] {
        &self.files
    }

    #[must_use]
    pub(super) fn level_files(&self, level: Level) -> SortedFiles<'_> {
        self.files.infallible_index(level).borrowed()
    }

    #[must_use]
    pub(super) fn compute_size_compaction(
        &self,
        size_opts: SizeCompactionOptions,
    ) -> Option<Level> {
        #![expect(
            clippy::as_conversions,
            clippy::cast_precision_loss,
            clippy::float_arithmetic,
            reason = "precision is not critical for a heuristic",
        )]

        let num_l0_files = self.level_files(Level::ZERO).inner().len();

        let mut best_level = Level::ZERO;
        // Level 0 is bounded by number of files instead of size in bytes.
        let mut best_score = (num_l0_files as f64) / f64::from(size_opts.max_level0_files.get());

        for level in MiddleLevel::MIDDLE_LEVELS {
            let level_files = self.files.infallible_index(level.as_level()).borrowed();
            let max_level_size = *size_opts.max_level_sizes.infallible_index(level);
            let score = (level_files.total_file_size() as f64) / max_level_size as f64;

            if score > best_score {
                best_level = level.as_level();
                best_score = score;
            }
        }

        if best_score >= 1_f64 {
            Some(best_level)
        } else {
            None
        }
    }

    #[expect(clippy::type_complexity, reason = "the individual types have clear semantic meaning")]
    pub fn get<FS, Cmp, Policy, Codecs, Pool>(
        &self,
        opts:            &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:        &InternallyMutableOptions<FS, Policy, Pool>,
        read_opts:       InternalReadOptions,
        decoders:        &mut Codecs::Decoders,
        manifest_number: FileNumber,
        lookup_key:      LookupKey<'_>,
    ) -> Result<
        (Option<TableEntry<Pool::PooledBuffer>>, Option<StartSeekCompaction>),
        RwErrorKindAlias<FS, Cmp, Codecs>,
    >
    where
        FS:     LevelDBFilesystem,
        Cmp:    LevelDBComparator,
        Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
        Codecs: CompressionCodecs,
        Pool:   BufferPool,
    {
        let mut seek_file: Option<(Level, &Arc<FileMetadata>)> = None;
        let mut last_file_read: Option<(Level, &Arc<FileMetadata>)> = None;
        let mut existing_buf: Option<Pool::PooledBuffer> = None;

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
                    let sstable = read_sstable(
                        opts, mut_opts, read_opts, decoders, manifest_number,
                        $file.file_number(), $file.file_size(),
                    )?;

                    let table_entry: Option<TableEntry<_>> = sstable.get(
                        opts,
                        mut_opts,
                        read_opts,
                        decoders,
                        &mut existing_buf,
                        lookup_key,
                    ).map_err(|read_err| read_err.into_rw_error($file.file_number()))?;

                    if let Some(table_entry) = table_entry {
                        let user_key = table_entry.key().as_internal_key().0;
                        if opts.cmp.cmp_user(user_key, lookup_key.0).is_eq() {
                            // TODO: check if the entry is for deletion.
                            // Will anything calling `Version::get` need to care about
                            // "not found at all" vs "found a tombstone"?
                            // If not, we should return Ok((None, _)) for deletion instead,
                            // so that the buffer is dropped immediately.
                            return Ok((
                                Some(table_entry),
                                StartSeekCompaction::record_seek(seek_file, 1),
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
            if opts.cmp.cmp_user(l0_file.smallest_user_key(), lookup_key.0).is_le()
                && opts.cmp.cmp(lookup_key.as_internal_key(), l0_file.largest_key()).is_le()
            {
                l0_candidates.push(l0_file);
            }
        }

        // Sort with the largest (and newest) file number first (instead of smallest).
        l0_candidates.sort_unstable_by_key(|file| ReverseOrder(file.file_number()));

        for file in l0_candidates {
            try_get!(Level::ZERO, file);
        }

        for level in NonZeroLevel::NONZERO_LEVELS {
            let files = self.files.infallible_index(level.as_level()).borrowed();
            if let Some(file) = files.find_file_disjoint(&opts.cmp, lookup_key.as_internal_key()) {
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
                if opts.cmp.cmp_user(file.smallest_user_key(), lookup_key.0).is_le()
                {
                    try_get!(level.as_level(), file);
                }
            }
        }

        Ok((None, StartSeekCompaction::record_seek(seek_file, 1)))
    }

    /// Returns the approximate offset of the internal key `key` within the files of this `Version`.
    ///
    /// Even if the key is not actually contained in any of the files, the approximate offset the
    /// key _would_ have if it were present is returned.
    ///
    /// Table files which cannot be successfully opened may or may not be ignored.
    pub fn approximate_offset_of_key<FS, Cmp, Policy, Codecs, Pool>(
        &self,
        opts:            &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:        &InternallyMutableOptions<FS, Policy, Pool>,
        read_opts:       InternalReadOptions,
        decoders:        &mut Codecs::Decoders,
        manifest_number: FileNumber,
        key:             InternalKey<'_>,
    ) -> FileOffset
    where
        FS:     LevelDBFilesystem,
        Cmp:    LevelDBComparator,
        Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
        Codecs: CompressionCodecs,
        Pool:   BufferPool,
    {
        let mut approx_offset = 0_u64;

        for level_files in &self.files {
            for file in level_files.borrowed().inner() {
                if opts.cmp.cmp(file.largest_key(), key).is_le() {
                    // Entire file is at or before the key; add the full file size.
                    approx_offset += file.file_size().0;
                } else if opts.cmp.cmp(key, file.smallest_key()).is_lt() {
                    // Entire file is after the key. Moreover, since `level_files` is sorted
                    // by `smallest_key` in increasing order, we know that the same would hold
                    // of every later file. None of them contribute to the offset.
                    break;
                } else {
                    // Ignore the error, as documented
                    if let Ok(table) = read_sstable(
                        opts,
                        mut_opts,
                        read_opts,
                        decoders,
                        manifest_number,
                        file.file_number(),
                        file.file_size(),
                    ) {
                        approx_offset += table.approximate_offset_of_key(&opts.cmp, key).0;
                    }
                }
            }
        }

        FileOffset(approx_offset)
    }

    pub fn record_read_sample<Cmp: LevelDBComparator>(
        &self,
        cmp:    &InternalComparator<Cmp>,
        key:    InternalKey<'_>,
        weight: u32,
    ) -> Option<StartSeekCompaction> {
        let mut last_file_read: Option<(Level, &Arc<FileMetadata>)> = None;

        // Called for each file in nonzero levels which might have the newest entry among
        // those with the lookup key's user key and a sequence number as old or older than the
        // lookup key's sequence number,
        // and for each file in level 0 which contains entries with the same user key as the
        // lookup key.
        macro_rules! maybe_record_seek {
            ($level:expr, $file:expr) => {
                // If we see that more than one file overlaps the key, then record a seek.
                if last_file_read.is_some() {
                    return StartSeekCompaction::record_seek(last_file_read, weight);
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
            if cmp.cmp_user(l0_file.smallest_user_key(), key.0).is_le()
                && cmp.cmp_user(key.0, l0_file.largest_user_key()).is_le()
            {
                maybe_record_seek!(Level::ZERO, l0_file);
            }
        }

        for level in NonZeroLevel::NONZERO_LEVELS {
            let files = self.files.infallible_index(level.as_level()).borrowed();
            if let Some(file) = files.find_file_disjoint(cmp, key) {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "`find_file_disjoint` returns either a valid index or `None`",
                )]
                let file = &files.inner()[file];

                // See `Version::get` for why this comparison is here.
                if cmp.cmp_user(file.smallest_user_key(), key.0).is_le()
                {
                    maybe_record_seek!(level.as_level(), file);
                }
            }
        }

        None
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

        let mut nonzero_levels = NonZeroLevel::NONZERO_LEVELS.into_iter();

        // Find the greatest level which overlaps the compaction range.
        // We'll compact everything from the memtable up to and including that level.
        // TODO(compact): maybe compact everything normally up to and excluding that level, and
        // then for that last level, compact it without putting the new files in a higher level.
        // The potential to quickly push every file into level-6 worries me; by having compactions
        // which don't push files to a higher level, the shape of the LSM tree could be better
        // preserved, I think.
        while let Some(level) = nonzero_levels.next_back() {
            let level_files = self.files.infallible_index(level.as_level()).borrowed();
            // Note that this is O(log n) in the number of files on that level.
            // Also, `_disjoint` is allowed because files in nonzero levels do not overlap.
            if level_files.range_overlaps_file_disjoint(cmp, lower_bound, upper_bound) {
                return Level::ZERO.inclusive_range(level.as_level())
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
    /// The `file_size_limit` setting is assumed to not exceed [`MAXIMUM_FILE_SIZE_LIMIT`].
    ///
    /// [`MAXIMUM_FILE_SIZE_LIMIT`]: crate::config_constants::MAXIMUM_FILE_SIZE_LIMIT
    pub fn level_for_compacted_memtable<Cmp: LevelDBComparator, Policy, Pool>(
        &self,
        opts:            &InternalOptions<Cmp, Policy, Pool>,
        memtable_lower:  UserKey<'_>,
        memtable_upper:  UserKey<'_>,
    ) -> Level {
        let lower = Some(memtable_lower);
        let upper = Some(memtable_upper);

        if self.level_files(Level::ZERO).range_overlaps_file(&opts.cmp, lower, upper) {
            Level::ZERO
        } else {
            // Push the memtable to the next level only if there's no overlap with the next level
            // and it doesn't overlap too many grandparents.
            let mut level = Level::ZERO;
            let mut overlaps = Vec::new();

            while let Some(next_level) = level.next_level() {
                if next_level.as_level() > opts.compaction.max_level_for_memtable_flush {
                    // Don't push it to the next level.
                    break;
                }

                let next_level_files = self.files.infallible_index(next_level.as_level())
                    .borrowed();
                // `_disjoint` is allowed because files in nonzero levels do not overlap.
                if next_level_files.range_overlaps_file_disjoint(&opts.cmp, lower, upper) {
                    // We can't push it to the next level.
                    break;
                }

                if let Some(next_level) = next_level.into_middle_level() {
                    let grandparent_level = next_level.next_level();
                    overlaps.clear();
                    // `_disjoint` is allowed because files in nonzero levels do not overlap.
                    // And we clear `overlaps`.
                    self.files.infallible_index(grandparent_level.as_level()).borrowed()
                        .get_overlapping_files_disjoint(&opts.cmp, lower, upper, &mut overlaps);
                    let grandparent_overlap: u64 = overlaps.iter()
                        .fold(0, |sum, file| sum.saturating_add(file.file_size().0));
                    let max_grandparent_overlap = *opts.compaction.max_grandparent_overlap
                        .infallible_index(next_level);
                    if grandparent_overlap > max_grandparent_overlap {
                        // Don't push it to the next level.
                        break;
                    }
                }

                // Push the memtable to the next level.
                level = next_level.as_level();
            }

            level
        }
    }

    /// Append iterators over this version's files to the provided `iters` vector.
    ///
    /// In particular, an [`IterToMerge::Table`] iterator is added for each level-0 file, and a
    /// [`IterToMerge::Level`] iterator is added for each nonzero level.
    pub fn add_iterators<FS, Cmp, Policy, Codecs, Pool>(
        self:            &Arc<Self>,
        opts:            &InternalOptions<Cmp, Policy, Codecs>,
        mut_opts:        &InternallyMutableOptions<FS, Policy, Pool>,
        read_opts:       InternalReadOptions,
        decoders:        &mut Codecs::Decoders,
        manifest_number: FileNumber,
        iters:           &mut Vec<IterToMerge<FS::RandomAccessFile, Cmp, Policy, Pool>>,
    ) -> Result<(), RwErrorKindAlias<FS, Cmp, Codecs>>
    where
        FS:     LevelDBFilesystem,
        Cmp:    LevelDBComparator,
        Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
        Codecs: CompressionCodecs,
        Pool:   BufferPool,
    {
        for table_file in self.level_files(Level::ZERO).inner() {
            let sstable = read_sstable(
                opts,
                mut_opts,
                read_opts,
                decoders,
                manifest_number,
                table_file.file_number(),
                table_file.file_size(),
            )?;

            let sstable_iter = TableIter::new(&sstable);
            iters.push(IterToMerge::Table(sstable_iter, sstable));
        }

        for level in NonZeroLevel::NONZERO_LEVELS {
            iters.push(IterToMerge::Level(DisjointLevelIter::new_disjoint(
                self.fast_mirrored_clone(),
                level,
            )));
        }

        Ok(())
    }

    // TODO: summaries of files in a version
    // file_summary_with_text_keys(&self, f) -> FmtResult
    // file_summary_with_numeric_keys(&self, f) -> FmtResult
    // file_summary_with<K>(&self, f, display_key: K) -> FmtResult
}

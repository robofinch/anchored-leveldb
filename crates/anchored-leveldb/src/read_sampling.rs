use std::sync::Arc;

use clone_behavior::FastMirroredClone;
use oorandom::Rand32;

use anchored_vfs::LevelDBFilesystem;

use crate::{
    internal_leveldb::InternalDBState,
    options::pub_options::SeekCompactionOptions,
    version::Version,
};
use crate::{
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    typed_bytes::{ContinueSampling, InternalKey},
};


/// Uniformly choose a number in `0..period.saturating_mul(2)`.
///
/// Note that the `iter_sample_period` is clamped such that `iter_sample_period * 2` never
/// overflows, but it can't hurt to be careful.
#[must_use]
fn get_period(prng: &mut Rand32, period: u32) -> u32 {
    prng.rand_range(0..period.saturating_mul(2))
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IterReadSampler {
    prng:                 Rand32,
    period_remaining_len: u32,
    iter_sample_period:   u32,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl IterReadSampler {
    #[must_use]
    pub fn new(seek_opts: SeekCompactionOptions, iter_read_sample_seed: &mut u64) -> Self {
        let mut prng = Rand32::new(*iter_read_sample_seed);
        *iter_read_sample_seed += 1;

        let first_period_len = get_period(&mut prng, seek_opts.iter_sample_period);

        Self {
            prng,
            period_remaining_len: first_period_len,
            iter_sample_period:   seek_opts.iter_sample_period,
        }
    }

    /// Possibly call [`Version::record_read_sample`] on the given version and key. If a seek
    /// compaction may be needed on the given version, it is reported to the database.
    ///
    /// If the given version is still the database's current version, the database will
    /// attempt to start a compaction.
    ///
    /// This function may acquire a lock on the entire database.
    ///
    /// # Sampling behavior
    ///
    /// A higher number of bytes read will result in a larger weight being passed to
    /// [`Version::record_read_sample`], on average.
    ///
    /// The exact result is pseudorandom, in an effort to preemptively avoid any strange edge cases
    /// with sampling particular patterns of data.
    ///
    /// # Return
    /// Samples are needed if and only if the version is still the current version of the database;
    /// if this function attempts to start a seek compaction and notices that the given version is
    /// not the database's current version, `ContinueSampling::False` is returned. Otherwise, the
    /// function indicates that samples should continue to be taken.
    ///
    /// In other words, the reported value may have false positives, but never false negatives;
    /// if `ContinueSampling::False` is returned, `sample` does not need to be called on the
    /// given `version` ever again.
    ///
    /// [`Version::record_read_sample`]: crate::version::Version::record_read_sample
    #[must_use]
    pub fn sample<FS, Cmp, Policy, Codecs, Pool>(
        &mut self,
        db:         &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:   &mut Codecs::Decoders,
        version:    &Arc<Version>,
        key:        InternalKey<'_>,
        bytes_read: usize,
    ) -> ContinueSampling
    where
        FS:     LevelDBFilesystem,
        Cmp:    LevelDBComparator,
        Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
        Codecs: CompressionCodecs,
        Pool:   BufferPool,
    {
        let weight = <Self as SpecializedWeight<{
            size_of::<usize>() <= size_of::<u32>()
        }>>::sample_weight(self, bytes_read);

        if let Some(start_seek_compaction) = version.record_read_sample(&db.opts.cmp, key, weight) {
            let mut mut_state = db.lock_mutable_state();
            let needs_compaction = mut_state.version_set
                .needs_seek_compaction(version, start_seek_compaction);

            if needs_compaction.needs_seek_compaction {
                let _drop = db.maybe_start_compaction(mut_state, decoders);
            }

            if needs_compaction.version_is_current {
                ContinueSampling::True
            } else {
                ContinueSampling::False
            }
        } else {
            ContinueSampling::True
        }
    }
}

/// Compile-time specialization, sort of.
trait SpecializedWeight<const USIZE_IS_SMALLER_THAN_U32: bool> {
    /// Return the number of times that the newly-read bytes should be sampled. Larger reads
    /// are weighted more heavily on average.
    ///
    /// The result is pseudorandom, in an effort to preemptively avoid any strange edge cases
    /// with sampling.
    ///
    /// A sampling period is uniformly chosen from `0` to `2*iter_read_sample_period - 1`,
    /// inclusive, and the progress made in the current sampling period is persisted. The number
    /// of sampling periods completed as a result of reading `bytes_read` additional bytes is
    /// returned.
    #[must_use]
    fn sample_weight(&mut self, bytes_read: usize) -> u32;
}

impl SpecializedWeight<true> for IterReadSampler {
    /// Should be called if and only if `size_of::<usize>() <= size_of::<u32>()`.
    fn sample_weight(&mut self, bytes_read: usize) -> u32 {
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "in this specialization, `usize` is at most 32 bits, so this doesn't truncate",
        )]
        let mut bytes_read = bytes_read as u32;

        let mut periods_completed = 0;

        while let Some(remaining) = bytes_read.checked_sub(self.period_remaining_len) {
            // If `self.period_remaining_len <= bytes_read`, then
            // subtract the remaining sampling period len from `bytes_read`, and enter a new
            // sampling period.
            bytes_read = remaining;
            self.period_remaining_len = get_period(&mut self.prng, self.iter_sample_period);
            periods_completed += 1;
        }

        // We know that `self.period_remaining_len > bytes_read`. We can simply
        // decrement `self.period_remaining_len` by `bytes_read` without fear of
        // overflow.
        self.period_remaining_len -= bytes_read;

        periods_completed
    }
}

impl SpecializedWeight<false> for IterReadSampler {
    /// Should be called if and only if `size_of::<usize>() > size_of::<u32>()`.
    fn sample_weight(&mut self, mut bytes_read: usize) -> u32 {
        #[expect(
            clippy::as_conversions,
            reason = "`usize` is at least 32 bits here, so `u32->usize` doesn't truncate",
        )]
        let mut period_remaining_len = self.period_remaining_len as usize;

        let mut periods_completed = 0;

        #[expect(
            clippy::as_conversions,
            reason = "`usize` is at least 32 bits here, so `u32->usize` doesn't truncate",
        )]
        while let Some(remaining) = bytes_read.checked_sub(period_remaining_len) {
            // If `period_remaining_len <= bytes_read`, then subtract the remaining sampling period
            // len from `bytes_read`, and enter a new sampling period.
            bytes_read = remaining;
            period_remaining_len = get_period(&mut self.prng, self.iter_sample_period) as usize;
            periods_completed += 1;
        }

        // We know that `period_remaining_len > bytes_read`. We can simply
        // decrement `period_remaining_len` by `bytes_read` without fear of overflow.
        period_remaining_len -= bytes_read;

        {
            #![expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "`period_remaining_len` always comes from `get_period`, is only ever \
                          decremented from there or reset with `get_period`, and never underflows. \
                          Therefore, since `get_period` returns a `u32`, `period_remaining_len` is \
                          still less than `u32::MAX`.",
            )]
            self.period_remaining_len = period_remaining_len as u32;
        };

        periods_completed
    }
}

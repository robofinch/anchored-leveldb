use std::fmt::{Debug, Formatter, Result as FmtResult};

use oorandom::Rand32;

use crate::{
    format::InternalKey,
    table_traits::adapters::InternalComparator,
    version::version_struct::Version,
};
use crate::{
    inner_leveldb::{InnerGenericDB, DBWriteImpl},
    leveldb_generics::{LdbContainer, LevelDBGenerics},
};


#[derive(Debug, Clone, Copy)]
pub(crate) struct ContinueSampling {
    pub continue_sampling: bool,
}

/// Uniformly choose a number in `0..period.saturating_mul(2)`.
#[must_use]
fn get_period(prng: &mut Rand32, period: u32) -> u32 {
    prng.rand_range(0..period.saturating_mul(2))
}

pub(crate) struct IterReadSampler<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> {
    prng:                          Rand32,
    sampling_period_remaining_len: u32,
    db:                            InnerGenericDB<LDBG, WriteImpl>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> IterReadSampler<LDBG, WriteImpl> {
    /// This method never acquires a lock on the database.
    #[must_use]
    pub fn new(db: InnerGenericDB<LDBG, WriteImpl>, seed: u64) -> Self {
        let mut prng = Rand32::new(seed);
        let period_setting = db.shared().db_options.iter_read_sample_period;
        let first_period_len = get_period(&mut prng, period_setting);

        Self {
            prng,
            sampling_period_remaining_len: first_period_len,
            db,
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
    /// not the database's current version, `ContinueSampling { continue_sampling: false }` is
    /// returned. Otherwise, the function indicates that samples should continue to be taken.
    ///
    /// In other words, the reported value may have false positives, but never false negatives;
    /// if `continue_sampling: false` is returned, `sample` does not need to be called on the
    /// given `version` ever again.
    ///
    /// [`Version::record_read_sample`]: crate::version::version_struct::Version::record_read_sample
    #[must_use]
    pub fn sample(
        &mut self,
        cmp:        &InternalComparator<LDBG::Cmp>,
        version:    &LdbContainer<LDBG, Version<LDBG::Refcounted>>,
        key:        InternalKey<'_>,
        bytes_read: usize,
    ) -> ContinueSampling {
        let weight = <Self as SpecializedWeight<{
            size_of::<usize>() <= size_of::<u32>()
        }>>::sample_weight(self, bytes_read);

        if let Some(start_seek_compaction) = version.record_read_sample(cmp, key, weight) {
            let version_is_current = self.db.maybe_start_seek_compaction(
                version,
                start_seek_compaction,
            );

            ContinueSampling {
                continue_sampling: version_is_current,
            }
        } else {
            ContinueSampling {
                continue_sampling: true,
            }
        }
    }
}

/// Compile-time specialization.
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

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>>
    SpecializedWeight<true>
for IterReadSampler<LDBG, WriteImpl>
{
    /// Should be called if and only if `size_of::<usize>() <= size_of::<u32>()`.
    fn sample_weight(&mut self, bytes_read: usize) -> u32 {
        let period_setting = self.db.shared().db_options.iter_read_sample_period;
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "in this specialization, `usize` is at most 32 bits, so this doesn't truncate",
        )]
        let mut bytes_read = bytes_read as u32;

        let mut periods_completed = 0;

        while let Some(remaining) = bytes_read.checked_sub(self.sampling_period_remaining_len) {
            // If `self.sampling_period_remaining_len <= bytes_read`, then
            // subtract the remaining sampling period len from `bytes_read`, and enter a new
            // sampling period.
            bytes_read = remaining;
            self.sampling_period_remaining_len = get_period(&mut self.prng, period_setting);
            periods_completed += 1;
        }

        // We know that `self.sampling_period_remaining_len > bytes_read`. We can simply
        // decrement `self.sampling_period_remaining_len` by `bytes_read` without fear of
        // overflow.
        self.sampling_period_remaining_len -= bytes_read;

        periods_completed
    }
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>>
    SpecializedWeight<false>
for IterReadSampler<LDBG, WriteImpl>
{
    /// Should be called if and only if `size_of::<usize>() > size_of::<u32>()`.
    fn sample_weight(&mut self, mut bytes_read: usize) -> u32 {
        let period_setting = self.db.shared().db_options.iter_read_sample_period;
        #[expect(
            clippy::as_conversions,
            reason = "`usize` is at least 32 bits here, so `u32->usize` doesn't truncate",
        )]
        let mut period_remaining_len = self.sampling_period_remaining_len as usize;

        let mut periods_completed = 0;

        #[expect(
            clippy::as_conversions,
            reason = "`usize` is at least 32 bits here, so `u32->usize` doesn't truncate",
        )]
        while let Some(remaining) = bytes_read.checked_sub(period_remaining_len) {
            // If `period_remaining_len <= bytes_read`, then subtract the remaining sampling period
            // len from `bytes_read`, and enter a new sampling period.
            bytes_read = remaining;
            period_remaining_len = get_period(&mut self.prng, period_setting) as usize;
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
            self.sampling_period_remaining_len = period_remaining_len as u32;
        };

        periods_completed
    }
}

impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> Debug
for IterReadSampler<LDBG, WriteImpl>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("IterReadSampler")
            .field("prng",                          &self.prng)
            .field("sampling_period_remaining_len", &self.sampling_period_remaining_len)
            .field("db",                            &"<InnerGenericDB>")
            .finish()
    }
}

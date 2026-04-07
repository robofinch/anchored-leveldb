use std::{
    num::{NonZeroU32, NonZeroU8},
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

use crate::{pub_traits::compression::CompressorId, pub_typed_bytes::NUM_NONZERO_LEVELS_USIZE};


/// Options which can be changed while the database is running.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DynamicOptions {
    pub memtable_compressor:            Option<CompressorId>,
    pub table_compressors:              [Option<CompressorId>; NUM_NONZERO_LEVELS_USIZE.get()],
    pub memtable_compression_goal:      u8,
    pub table_compression_goals:        [u8; NUM_NONZERO_LEVELS_USIZE.get()],
    pub sstable_block_size:             usize,
    pub sstable_block_restart_interval: NonZeroU32,
}

#[derive(Debug)]
pub(crate) struct AtomicDynamicOptions {
    compressors:       AtomicU64,
    compression_goals: AtomicU64,
    block_size:        AtomicUsize,
    restart_interval:  AtomicU32,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl AtomicDynamicOptions {
    #[must_use]
    fn u64_compressors(frozen: DynamicOptions) -> u64 {
        let mut compressors = [None; 8];
        compressors[0] = frozen.memtable_compressor;
        #[expect(clippy::expect_used, reason = "cannot panic")]
        {
            *compressors.last_chunk_mut::<6>().expect("`6 <= 8`") = frozen.table_compressors;
        };
        u64::from_le_bytes(compressors.map(Self::compressor_to_u8))
    }

    #[must_use]
    const fn u64_compression_goals(frozen: DynamicOptions) -> u64 {
        let mut goals = [0; 8];
        goals[0] = frozen.memtable_compression_goal;
        #[expect(clippy::expect_used, reason = "cannot panic")]
        {
            *goals.last_chunk_mut::<6>().expect("`6 <= 8`") = frozen.table_compression_goals;
        };
        u64::from_le_bytes(goals)
    }

    #[must_use]
    fn compressor_to_u8(compressor: Option<CompressorId>) -> u8 {
        compressor.map_or(0, |id| id.0.get())
    }

    #[must_use]
    fn u8_to_compressor(compressor: u8) -> Option<CompressorId> {
        NonZeroU8::new(compressor).map(CompressorId)
    }

    #[inline]
    #[must_use]
    pub fn new(frozen: DynamicOptions) -> Self {
        Self {
            compressors:       AtomicU64::new(Self::u64_compressors(frozen)),
            compression_goals: AtomicU64::new(Self::u64_compression_goals(frozen)),
            block_size:        AtomicUsize::new(frozen.sstable_block_size),
            restart_interval:  AtomicU32::new(frozen.sstable_block_restart_interval.get()),
        }
    }

    pub fn set_all(&self, frozen: DynamicOptions) {
        let mut compressors = [None; 8];
        compressors[0] = frozen.memtable_compressor;
        #[expect(clippy::expect_used, reason = "cannot panic")]
        {
            *compressors.last_chunk_mut::<6>().expect("`6 <= 8`") = frozen.table_compressors;
        };
        let compressors = u64::from_le_bytes(compressors.map(Self::compressor_to_u8));
        let restart_interval = frozen.sstable_block_restart_interval.get();

        self.compressors.store(compressors, Ordering::Relaxed);
        self.block_size.store(frozen.sstable_block_size, Ordering::Relaxed);
        self.restart_interval.store(restart_interval, Ordering::Relaxed);
    }

    pub fn set_memtable_compressor(&self, memtable_compressor: Option<CompressorId>) {
        let memtable_compressor = Self::compressor_to_u8(memtable_compressor);
        let _old_val = self.compressors.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |compressors| {
                let mut compressors = compressors.to_le_bytes();
                compressors[0] = memtable_compressor;
                Some(u64::from_le_bytes(compressors))
            },
        );
    }

    pub fn set_memtable_compression_goal(&self, memtable_goal: u8) {
        let _old_val = self.compression_goals.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |goals| {
                let mut goals = goals.to_le_bytes();
                goals[0] = memtable_goal;
                Some(u64::from_le_bytes(goals))
            },
        );
    }

    pub fn set_table_compressors(
        &self,
        table_compressors: [Option<CompressorId>; NUM_NONZERO_LEVELS_USIZE.get()],
    ) {
        let table_compressors = table_compressors.map(Self::compressor_to_u8);
        let _old_val = self.compressors.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |compressors| {
                let mut compressors = compressors.to_le_bytes();
                #[expect(clippy::expect_used, reason = "cannot panic")]
                {
                    *compressors.last_chunk_mut::<6>().expect("`6 <= 8`") = table_compressors;
                };
                Some(u64::from_le_bytes(compressors))
            },
        );
    }

    pub fn set_table_compression_goals(
        &self,
        table_goals: [u8; NUM_NONZERO_LEVELS_USIZE.get()],
    ) {
        let _old_val = self.compression_goals.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |goals| {
                let mut goals = goals.to_le_bytes();
                #[expect(clippy::expect_used, reason = "cannot panic")]
                {
                    *goals.last_chunk_mut::<6>().expect("`6 <= 8`") = table_goals;
                };
                Some(u64::from_le_bytes(goals))
            },
        );
    }

    pub fn set_sstable_block_size(&self, block_size: usize) {
        self.block_size.store(block_size, Ordering::Relaxed);
    }

    pub fn set_sstable_block_restart_interval(&self, interval: NonZeroU32) {
        self.restart_interval.store(interval.get(), Ordering::Relaxed);
    }

    pub fn read(&self) -> DynamicOptions {
        let compressors = u64::to_le_bytes(self.compressors.load(Ordering::Relaxed));
        let compression_goals = u64::to_le_bytes(self.compression_goals.load(Ordering::Relaxed));
        let sstable_block_size = self.block_size.load(Ordering::Relaxed);
        let restart_interval = self.restart_interval.load(Ordering::Relaxed);

        let compressors = compressors.map(Self::u8_to_compressor);

        let memtable_compressor = compressors[0];
        #[expect(clippy::expect_used, reason = "cannot panic")]
        let table_compressors = *compressors.last_chunk::<6>().expect("`6 <= 8`");

        let memtable_compression_goal = compression_goals[0];
        #[expect(clippy::expect_used, reason = "cannot panic")]
        let table_compression_goals = *compression_goals.last_chunk::<6>().expect("`6 <= 8`");

        #[expect(clippy::expect_used, reason = "cannot panic")]
        let sstable_block_restart_interval = NonZeroU32::new(restart_interval)
            .expect("`DynamicOptions.restart_interval` is only ever set to `NonZeroU32` values");

        DynamicOptions {
            memtable_compressor,
            table_compressors,
            memtable_compression_goal,
            table_compression_goals,
            sstable_block_size,
            sstable_block_restart_interval,
        }
    }
}

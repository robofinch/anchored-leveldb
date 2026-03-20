use std::path::PathBuf;
use std::num::{NonZeroU32, NonZeroUsize};

use crate::{
    db_settings::CompressorId,
    pub_traits::pool::BufferPool,
    pub_typed_bytes::NUM_NONZERO_LEVELS_USIZE,
};
use crate::table_format::{InternalComparator, InternalFilterPolicy};


#[derive(Debug)]
pub(crate) struct InternalOptions<Cmp, Policy, Codecs, Pool: BufferPool> {
    pub db_directory:                   PathBuf,
    pub cmp:                            InternalComparator<Cmp>,
    pub policy:                         Option<InternalFilterPolicy<Policy>>,
    pub codecs:                         Codecs,
    /// TODO: will need to be dynamically changeable
    pub memtable_compressor:            Option<CompressorId>,
    /// TODO: will need to be dynamically changeable
    pub table_compressors:              [Option<CompressorId>; NUM_NONZERO_LEVELS_USIZE.get()],
    /// TODO: likely involves internal mutability. Should this be placed elsewhere?
    pub buffer_pool:                    Pool,
    /// TODO: will need to be dynamically changeable
    pub sstable_block_size:             usize,
    /// TODO: will need to be dynamically changeable
    pub sstable_block_restart_interval: NonZeroUsize,
    pub filter_chunk_size_log2:         u8,
    /// Aim to compress the source data by `(compression_goal / 256) * 100%`.
    ///
    /// Default: 32 (for 12.5% compression).
    pub compression_goal:               u8,
    pub web_scale:                      WebScale,
    pub seek_compactions:               SeekCompactionOptions,
}

#[derive(Debug)]
pub(crate) struct InternalOptionsPerRead {
    pub verify_checksums:  bool,
    pub block_cache_usage: CacheUsage,
    pub table_cache_usage: CacheUsage,
}

#[derive(Debug)]
pub(crate) struct InternalOptionsPerWrite {
    pub verify_checksums: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum WebScale {
    WebScale,
    NotWebScale,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CacheUsage {
    ReadAndFill,
    Read,
    Ignore,
}


// TODO: will probably be `pub`.
// TODO: document how to best choose the setting values (as described in comments of Google's
// leveldb code).
#[derive(Debug, Clone, Copy)]
pub(crate) struct SeekCompactionOptions {
    pub enable_automatic_seek_compactions: bool,
    /// Used to calculate how many times an unnecessary read to a file must occur before an
    /// automatic seek compaction may be triggered on that file (if enabled).
    ///
    /// Larger files permit a greater number of seeks before a compaction (as compaction is more
    /// expensive for larger files). The size-based limit is clamped to the inclusive range
    /// `[min_allowed_seeks, u32::MAX/2]`, with the `u32::MAX/2` maximum taking priority over
    /// the provided `min_allowed_seeks` minimum option. (Therefore, this option is effectively
    /// clamped to at most `u32::MAX/2`.)
    ///
    /// Defaults to 100.
    pub min_allowed_seeks: u32,
    /// Used to calculate how many times an unnecessary read to a file must occur before an
    /// automatic seek compaction may be triggered on that file.
    ///
    /// Larger files permit a greater number of seeks before a compaction (as compaction is more
    /// expensive for larger files); one additional seek is permitted per `file_bytes_per_seek`
    /// bytes. The size-based limit is clamped to the inclusive range
    /// `[min_allowed_seeks, u32::MAX/2]`.
    ///
    /// Defaults to 16 KiB.
    pub file_bytes_per_seek: NonZeroU32,
    /// In order to encourage the compaction of frequently-accessed files, iteration through the
    /// database will record a file seek approximately once every `iter_sample_period` bytes read
    /// (if automatic seek compactions are enabled).
    ///
    /// Clamped to at most `u32::MAX/2`.
    ///
    /// Defaults to 1 MiB.
    pub iter_sample_period: u32,
}

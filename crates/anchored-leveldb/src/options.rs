use std::num::NonZeroUsize;

use crate::{
    pub_traits::pool::BufferPool,
    table_caches::{BlockCache, TableCache},
    table_format::{InternalComparator, InternalFilterPolicy},
};


#[derive(Debug)]
pub(crate) struct InternalOptions<RandomAccessFile, Cmp, Policy, Codecs, Pool: BufferPool> {
    pub cmp:                            InternalComparator<Cmp>,
    pub policy:                         Option<InternalFilterPolicy<Policy>>,
    pub codecs:                         Codecs,
    pub buffer_pool:                    Pool,
    pub block_cache:                    BlockCache<Pool>,
    pub table_cache:                    TableCache<RandomAccessFile, Policy, Pool>,
    pub sstable_block_size:             usize,
    pub sstable_block_restart_interval: NonZeroUsize,
    pub filter_chunk_size_log2:         u8,
    /// Aim to compress the source data by `(compression_goal / 256) * 100%`.
    ///
    /// Default: 32 (for 12.5% compression).
    pub compression_goal:               u8,
    pub web_scale:                      WebScale,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum WebScale {
    WebScale,
    NotWebScale,
}

#[derive(Debug)]
pub(crate) struct InternalOptionsPerRead {
    pub verify_checksums: bool,
}

#[derive(Debug)]
pub(crate) struct InternalOptionsPerWrite {
    pub verify_checksums: bool,
}

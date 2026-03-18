use std::num::NonZeroUsize;

use crate::table_format::{InternalComparator, InternalFilterPolicy};


#[derive(Debug)]
pub(crate) struct InternalOptions<Cmp, Policy, Codecs, Pool> {
    pub cmp:                            InternalComparator<Cmp>,
    pub policy:                         Option<InternalFilterPolicy<Policy>>,
    pub codecs:                         Codecs,
    pub buffer_pool:                    Pool,
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

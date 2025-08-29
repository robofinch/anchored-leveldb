// TODO: provide builders and/or defaults


#[derive(Debug, Clone)]
pub struct ReadTableOptions<CompList, Policy, TableCmp, Cache, Pool> {
    pub compressor_list:  CompList,
    pub policy:           Option<Policy>,
    pub comparator:       TableCmp,
    pub verify_checksums: bool,
    pub block_cache:      Option<Cache>,
    pub buffer_pool:      Pool,
}

#[derive(Debug, Clone)]
pub struct WriteTableOptions<CompList, Policy, TableCmp> {
    pub compressor_list:        CompList,
    pub selected_compressor:    u8,
    pub filter_policy:          Option<Policy>,
    pub comparator:             TableCmp,
    /// The [`Block`]s of the table will have exactly one `restart` entry every
    /// `block_restart_interval` entries. These restart entries are used by iterators seeking
    /// through the `Block`, including moving backwards. (Forwards step-by-step iteration does not
    /// require `restart`s, but many operations do require them).
    ///
    /// Must be strictly greater than `0`.
    ///
    /// [`Block`]: super::block::Block
    pub block_restart_interval: usize,
    /// Loose upper bound on the maximum size in bytes that a [`Block`] in the table may have.
    ///
    /// Once the limit is exceeded, a `Block` is written. Therefore, a large value could cause
    /// a block's size to greatly overshoot this limit.
    ///
    /// [`Block`]: super::block::Block
    pub block_size:             usize,
}

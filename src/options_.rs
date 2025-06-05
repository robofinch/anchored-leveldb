use crate::{
    compressors::CompressorList,
    filesystem::FileSystem,
};

use crate::{
    comparator::{Comparator, DefaultComparator},
    filter::{BloomPolicy, FilterPolicy},
    logger::{InfoLogger, LogFileLogger},
};





#[derive(Debug)]
pub struct FunctionOptions<InfoLoggerConstructor, Comparator, FilterPolicy> {
    pub info_logger:   InfoLoggerConstructor,
    pub key_cmp:       Comparator,
    pub filter_policy: FilterPolicy,
}

type DefaultFunctionOptions = FunctionOptions<LogFileLogger, DefaultComparator, BloomPolicy>;

// impl<I, DefaultComparator, BloomPolicy> FunctionOptions<I, C, F> {
//     pub fn with_logger
// }



#[derive(Debug)]
pub struct OpenOptions {
    create_db_if_missing:       bool,
    error_if_db_exists:         bool,
    paranoid_corruption_checks: bool,
    write_buffer_size:          usize,
    max_open_files:             usize,
    max_file_size:              usize,
    block_cache_byte_capacity:  usize,
    block_size:                 usize,
    block_restart_interval:     usize,
    // TODO: is this needed? Is this for both writes and reads,
    // or only for writes?
    compressor:                 u8,
    compressor_list:            CompressorList
}




// pub fn default_options
// pub type DynOptions

use log::LevelFilter;

use anchored_sstable::CompressorList;

use crate::{
    compactor::CompactorHandleCreator,
    leveldb::LevelDBGenerics,
    logger::LoggerConstructor,
};


pub trait OpenOptionGenerics: LevelDBGenerics
{
    type LoggerConstructor:      LoggerConstructor<Self::FS, Logger = Self::Logger>;
    type CompactorHandleCreator: CompactorHandleCreator<Self, Handle = Self::CompactorHandle>;
}

#[derive(Debug, Clone)]
pub struct OpenOptions<OOG: OpenOptionGenerics> {
    pub filesystem:                     OOG::FS,
    pub create_db_if_missing:           bool,
    pub error_if_db_exists:             bool,

    pub logger_constructor:             OOG::LoggerConstructor,
    pub error_if_logger_creation_fails: bool,
    pub logger_level_filter:            LevelFilter,

    pub comparator:                     OOG::Comparator,
    pub filter_policy:                  OOG::FilterPolicy,
    pub compactor_handle_creator:       OOG::CompactorHandleCreator,

    pub compressors:                    CompressorList,
    // TODO: documentation
    pub compressor:                     u8,

    pub paranoid_corruption_checks:     bool,
    pub write_buffer_size:              usize,
    pub max_open_files:                 usize,
    pub max_file_size:                  usize,
    pub cache_file_contents:            bool,
    pub block_cache_byte_capacity:      usize,
    pub block_size:                     usize,
    pub block_restart_interval:         usize,
}

impl<OOG: OpenOptionGenerics> OpenOptions<OOG>
where
    OOG::FS:                     Default,
    OOG::LoggerConstructor:      Default,
    OOG::Comparator:             Default,
    OOG::FilterPolicy:           Default,
    OOG::CompactorHandleCreator: Default,
{
    #[inline]
    pub fn new() -> Self {
        Self::new_with_fs(OOG::FS::default())
    }
}

impl<OOG: OpenOptionGenerics> OpenOptions<OOG>
where
    OOG::LoggerConstructor:      Default,
    OOG::Comparator:             Default,
    OOG::FilterPolicy:           Default,
    OOG::CompactorHandleCreator: Default,
{
    pub fn new_with_fs(filesystem: OOG::FS) -> Self {
        Self::new_with_generics(
            filesystem,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
    }
}

impl<OOG: OpenOptionGenerics> OpenOptions<OOG> {
    #[inline]
    pub fn new_with_generics(
        filesystem:               OOG::FS,
        logger_constructor:       OOG::LoggerConstructor,
        comparator:               OOG::Comparator,
        filter_policy:            OOG::FilterPolicy,
        compactor_handle_creator: OOG::CompactorHandleCreator,
    ) -> Self {
        // This will not work well on 16-bit systems, but it'd be surprising if something else
        // didn't break at compile time.
        let mb: usize = 1 << 20;

        let block_size: usize = 4096;

        Self {
            filesystem,
            create_db_if_missing:           true,
            error_if_db_exists:             false,
            logger_constructor,
            error_if_logger_creation_fails: true,
            logger_level_filter:            LevelFilter::Info,
            comparator,
            filter_policy,
            compactor_handle_creator,
            compressors:                    CompressorList::with_default_compressors(),
            compressor:                     0,
            paranoid_corruption_checks:     false,
            write_buffer_size:              4 * mb,
            max_open_files:                 1024,
            max_file_size:                  1 * mb,
            cache_file_contents:            true,
            block_cache_byte_capacity:      1024 * block_size,
            block_size,
            block_restart_interval:         16,
        }
    }
}

impl<OOG: OpenOptionGenerics> Default for OpenOptions<OOG>
where
    OOG::FS:                     Default,
    OOG::LoggerConstructor:      Default,
    OOG::Comparator:             Default,
    OOG::FilterPolicy:           Default,
    OOG::CompactorHandleCreator: Default,
{
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

use crate::leveldb_generics::LevelDBGenerics;
use super::super::write_impl::DBWriteImpl;
use super::InnerGenericDB;


// #[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<LDBG: LevelDBGenerics, WriteImpl: DBWriteImpl<LDBG>> InnerGenericDB<LDBG, WriteImpl> {
    // check_corruption
    // approximate_sizes
    // later: approximate_ram_usage
    // later: compaction_statistics
    // num_files_at_level
    // file_summary_with_text_keys(&self, f) -> FmtResult
    // file_summary_with_numeric_keys(&self, f) -> FmtResult
    // file_summary_with<K>(&self, f, display_key: K) -> FmtResult
    // info_log
}

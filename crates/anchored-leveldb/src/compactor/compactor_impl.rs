use anchored_vfs::traits::ReadableFilesystem;

use super::{
    CompactionError, CompactionInstruction, CompactionResponse,
    CompactorGenerics, FSError,
};


#[derive(Debug)]
pub struct Compactor<CG: CompactorGenerics> {
    fs: CG::FS,
}

impl<CG: CompactorGenerics> Compactor<CG> {
    // maybe do_work or something
    pub fn do_compaction(
        &mut self,
        instruction: CompactionInstruction,
    ) -> Result<CompactionResponse, CompactionError<FSError<CG>>> {

        fn err<CG: CompactorGenerics>() -> <CG::FS as ReadableFilesystem>::Error {
            panic!()
        }

        Err(CompactionError::FileSystemError(err::<CG>()))
    }
}

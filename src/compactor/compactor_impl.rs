use crate::filesystem::ReadableFileSystem;

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

        fn err<CG: CompactorGenerics>() -> <CG::FS as ReadableFileSystem>::Error {
            panic!()
        }

        Err(CompactionError::FileSystemError(err::<CG>()))
    }
}

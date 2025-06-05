use std::{collections::VecDeque, convert::Infallible};

use super::{
    CompactionInstruction, CompactionResponse, CompactionResult,
    Compactor, CompactorGenerics, CompactorHandle, FSError,
};


#[derive(Debug)]
pub struct BlockingHandle<CG: CompactorGenerics> {
    compactor: Compactor<CG>,
    responses: VecDeque<CompactionResult<FSError<CG>>>,
}

impl<CG: CompactorGenerics> BlockingHandle<CG> {
    #[inline]
    fn new(compactor: Compactor<CG>) -> Self {
        Self {
            compactor,
            responses: VecDeque::new(),
        }
    }
}

impl<CG: CompactorGenerics> CompactorHandle<FSError<CG>> for BlockingHandle<CG> {
    type Error = Infallible;

    fn send(&mut self, instruction: CompactionInstruction) -> Result<(), Self::Error> {
        let result = self.compactor.do_compaction(instruction);
        self.responses.push_back(result);
        Ok(())
    }

    fn recv(&mut self) -> Result<CompactionResult<FSError<CG>>, Self::Error> {
        let oldest_response = self.responses.pop_front();
        Ok(oldest_response.unwrap_or(Ok(CompactionResponse::NoPendingCompactions)))
    }
}

impl<CG: CompactorGenerics> From<Compactor<CG>> for BlockingHandle<CG> {
    #[inline]
    fn from(compactor: Compactor<CG>) -> Self {
        Self::new(compactor)
    }
}

use std::{collections::VecDeque, convert::Infallible};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use super::{
    CompactionInstruction, CompactionResponse, CompactionResult,
    Compactor, CompactorGenerics, CompactorHandle, FSError,
};


pub struct BlockingHandle<CG: CompactorGenerics> {
    compactor: Compactor<CG>,
    responses: VecDeque<CompactionResult<FSError<CG>>>,
}

impl<CG> Debug for BlockingHandle<CG>
where
    CG:            CompactorGenerics,
    Compactor<CG>: Debug,
    FSError<CG>:   Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CloneableBlockingHandle")
            .field("compactor", &self.compactor)
            .field("responses", &self.responses)
            .finish()
    }
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

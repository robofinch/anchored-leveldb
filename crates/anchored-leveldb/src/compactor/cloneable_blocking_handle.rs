use std::{cell::RefCell, collections::VecDeque, convert::Infallible, rc::Rc};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use super::{
    CompactionInstruction, CompactionResponse, CompactionResult,
    Compactor, CompactorGenerics, CompactorHandle, FSError,
};


pub struct CloneableBlockingHandle<CG: CompactorGenerics> {
    compactor: Rc<RefCell<Compactor<CG>>>,
    responses: Rc<RefCell<VecDeque<CompactionResult<FSError<CG>>>>>,
}

impl<CG> Debug for CloneableBlockingHandle<CG>
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

impl<CG: CompactorGenerics> CloneableBlockingHandle<CG> {
    #[inline]
    fn new(compactor: Compactor<CG>) -> Self {
        Self {
            compactor: Rc::new(RefCell::new(compactor)),
            responses: Rc::new(RefCell::new(VecDeque::new())),
        }
    }
}

impl<CG: CompactorGenerics> CompactorHandle<FSError<CG>> for CloneableBlockingHandle<CG> {
    type Error = Infallible;

    fn send(&mut self, instruction: CompactionInstruction) -> Result<(), Self::Error> {
        let result = self.compactor.borrow_mut().do_compaction(instruction);
        self.responses.borrow_mut().push_back(result);
        Ok(())
    }

    fn recv(&mut self) -> Result<CompactionResult<FSError<CG>>, Self::Error> {
        let oldest_response = self.responses.borrow_mut().pop_front();
        Ok(oldest_response.unwrap_or(Ok(CompactionResponse::NoPendingCompactions)))
    }
}

impl<CG: CompactorGenerics> From<Compactor<CG>> for CloneableBlockingHandle<CG> {
    #[inline]
    fn from(compactor: Compactor<CG>) -> Self {
        Self::new(compactor)
    }
}

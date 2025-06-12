use std::{cell::RefCell, fmt::Debug, rc::Rc};
use std::sync::{Arc, Mutex};

use crate::error::MutexPoisoned;
use super::{CompactionInstruction, CompactionResult, CompactorHandle};


impl<FSError, Error: Debug> CompactorHandle<FSError>
for Box<dyn CompactorHandle<FSError, Error = Error>> {
    type Error = Error;

    #[inline]
    fn send(&mut self, instruction: CompactionInstruction) -> Result<(), Self::Error> {
        self.as_mut().send(instruction)
    }

    #[inline]
    fn recv(&mut self) -> Result<CompactionResult<FSError>, Self::Error> {
        self.as_mut().recv()
    }
}

impl<FSError, Error: Debug> CompactorHandle<FSError>
for Rc<RefCell<dyn CompactorHandle<FSError, Error = Error>>> {
    type Error = Error;

    #[inline]
    fn send(&mut self, instruction: CompactionInstruction) -> Result<(), Self::Error> {
        self.borrow_mut().send(instruction)
    }

    #[inline]
    fn recv(&mut self) -> Result<CompactionResult<FSError>, Self::Error> {
        self.borrow_mut().recv()
    }
}

impl<FSError, Error: Debug + From<MutexPoisoned>> CompactorHandle<FSError>
for Arc<Mutex<dyn CompactorHandle<FSError, Error = Error>>> {
    type Error = Error;

    #[inline]
    fn send(&mut self, instruction: CompactionInstruction) -> Result<(), Self::Error> {
        let mut inner = self.lock()
            .map_err(MutexPoisoned::from)
            .map_err(Error::from)?;

        inner.send(instruction)
    }

    #[inline]
    fn recv(&mut self) -> Result<CompactionResult<FSError>, Self::Error> {
        let mut inner = self.lock()
            .map_err(MutexPoisoned::from)
            .map_err(Error::from)?;

        inner.recv()
    }
}

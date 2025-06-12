use std::fmt::Debug;
use std::sync::{
    Arc, Mutex,
    mpsc::{Receiver, Sender, TryRecvError},
};

use crate::error::MutexPoisoned;

use super::{CompactionInstruction, CompactionResponse, CompactionResult, CompactorHandle};


#[derive(Debug, Clone)]
pub struct CloneableMpscHandle<FSError> {
    sender:   Sender<CompactionInstruction>,
    receiver: Arc<Mutex<Receiver<CompactionResult<FSError>>>>,
}

impl<FSError> CloneableMpscHandle<FSError> {
    #[inline]
    fn new(
        instruction_sender: Sender<CompactionInstruction>,
        result_receiver: Receiver<CompactionResult<FSError>>,
    ) -> Self {
        Self {
            sender:   instruction_sender,
            receiver: Arc::new(Mutex::new(result_receiver)),
        }
    }
}

impl<FSError: Debug> CompactorHandle<FSError> for CloneableMpscHandle<FSError> {
    type Error = CloneableMpscHandleError;

    fn send(&mut self, instruction: CompactionInstruction) -> Result<(), Self::Error> {
        self.sender
            .send(instruction)
            .map_err(|_| CloneableMpscHandleError::CompactorDropped)
    }

    fn recv(&mut self) -> Result<CompactionResult<FSError>, Self::Error> {
        let receiver = self
            .receiver
            .lock()
            .map_err(|_| CloneableMpscHandleError::MutexPoisoned)?;

        match receiver.try_recv() {
            Ok(response_result)             => Ok(response_result),
            Err(TryRecvError::Empty)        => Ok(Ok(CompactionResponse::NoPendingCompactions)),
            Err(TryRecvError::Disconnected) => Err(CloneableMpscHandleError::CompactorDropped),
        }
    }
}

impl<FSError> From<(Sender<CompactionInstruction>, Receiver<CompactionResult<FSError>>)>
for CloneableMpscHandle<FSError>
{
    #[inline]
    fn from(
        channels: (Sender<CompactionInstruction>, Receiver<CompactionResult<FSError>>),
    ) -> Self {
        Self::new(channels.0, channels.1)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CloneableMpscHandleError {
    CompactorDropped,
    MutexPoisoned,
}

impl From<MutexPoisoned> for CloneableMpscHandleError {
    #[inline]
    fn from(_err: MutexPoisoned) -> Self {
        Self::MutexPoisoned
    }
}

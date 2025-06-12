use std::fmt::Debug;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use super::{CompactionInstruction, CompactionResponse, CompactionResult, CompactorHandle};


#[derive(Debug)]
pub struct MpscHandle<FSError> {
    sender:   Sender<CompactionInstruction>,
    receiver: Receiver<CompactionResult<FSError>>,
}

impl<FSError: Debug> CompactorHandle<FSError> for MpscHandle<FSError> {
    type Error = MpscCompactorDropped;

    fn send(&mut self, instruction: CompactionInstruction) -> Result<(), Self::Error> {
        self.sender
            .send(instruction)
            .map_err(|_| MpscCompactorDropped)
    }

    fn recv(&mut self) -> Result<CompactionResult<FSError>, Self::Error> {
        match self.receiver.try_recv() {
            Ok(response_result)             => Ok(response_result),
            Err(TryRecvError::Empty)        => Ok(Ok(CompactionResponse::NoPendingCompactions)),
            Err(TryRecvError::Disconnected) => Err(MpscCompactorDropped),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MpscCompactorDropped;

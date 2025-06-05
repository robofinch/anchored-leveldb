use std::{sync::mpsc, thread};
use std::marker::PhantomData;
use std::sync::mpsc::{Receiver, Sender};

use super::{
    Compactor, CompactorGenerics, CompactorHandle,
    CompactionInstruction, CompactionResult, FSError,
};


pub trait CompactorHandleCreator<CG: CompactorGenerics> {
    type Handle: CompactorHandle<FSError<CG>>;

    fn create_handle(self, compactor: Compactor<CG>) -> Self::Handle;
}


#[derive(Debug, Clone, Copy)]
pub struct Identity<Handle> {
    _marker: PhantomData<fn() -> Handle>,
}

impl<CG, Handle> CompactorHandleCreator<CG> for Identity<Handle>
where
    CG: CompactorGenerics,
    Handle: CompactorHandle<FSError<CG>> + From<Compactor<CG>>,
{
    type Handle = Handle;

    #[inline]
    fn create_handle(self, compactor: Compactor<CG>) -> Handle {
        Handle::from(compactor)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StdThreadAndMpscChannels<Handle> {
    _marker: PhantomData<fn() -> Handle>,
}

impl<CG, Handle> CompactorHandleCreator<CG> for StdThreadAndMpscChannels<Handle>
where
    CG:              CompactorGenerics + 'static,
    Handle:          CompactorHandle<FSError<CG>>
                        + From<(
                            Sender<CompactionInstruction>,
                            Receiver<CompactionResult<FSError<CG>>>,
                        )>,
    CG::FS:          Send,
    FSError<CG>:     Send,
{
    type Handle = Handle;

    #[inline]
    fn create_handle(self, mut compactor: Compactor<CG>) -> Handle {

        let (inst_sender, inst_recvr) = mpsc::channel();
        let (res_sender,  res_recvr)  = mpsc::channel();

        thread::spawn(move || {
            // The receiver only returns an error if `inst_sender` was dropped
            while let Ok(instruction) = inst_recvr.recv() {
                let result = compactor.do_compaction(instruction);
                // The sender only returns an error if `res_recvr` was dropped
                if res_sender.send(result).is_err() {
                    return;
                }
            }
        });

        Handle::from((inst_sender, res_recvr))
    }
}

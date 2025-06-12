mod compactor_impl;
mod handle_creator;
mod dyn_impls;
mod blocking_handle;
mod cloneable_blocking_handle;
mod mpsc_handle;
mod cloneable_mpsc_handle;
// mod mpmc_handle;


pub use self::{
    blocking_handle::BlockingHandle,
    cloneable_blocking_handle::CloneableBlockingHandle,
    cloneable_mpsc_handle::{CloneableMpscHandle, CloneableMpscHandleError},
    compactor_impl::Compactor,
    handle_creator::{CompactorHandleCreator, Identity, StdThreadAndMpscChannels},
    mpsc_handle::{MpscHandle, MpscCompactorDropped},
};


use std::fmt::Debug;

use crate::leveldb::LevelDBGenerics;
use crate::filesystem::{FileSystem, ReadableFileSystem};


pub type CompactionResult<FSError> = Result<CompactionResponse, CompactionError<FSError>>;


// ================
//  Generics
// ================

pub trait CompactorGenerics: Debug {
    type FS:          FileSystem;
}

impl<LDBG: LevelDBGenerics> CompactorGenerics for LDBG {
    type FS          = LDBG::FS;
}

pub type FSError<CG> = <<CG as CompactorGenerics>::FS as ReadableFileSystem>::Error;

// ================
//  Interface
// ================

pub trait CompactorHandle<FSError>: Debug {
    type Error: Debug;

    fn send(&mut self, instruction: CompactionInstruction) -> Result<(), Self::Error>;
    fn recv(&mut self) -> Result<CompactionResult<FSError>, Self::Error>;
}

#[derive(Debug)]
pub enum CompactionInstruction {

}

#[derive(Debug)]
pub enum CompactionResponse {
    NoPendingCompactions,
}

#[derive(Debug)]
pub enum CompactionError<FSError> {
    FileSystemError(FSError),
}

use std::{sync::Condvar, thread::JoinHandle};


#[derive(Debug)]
pub(crate) struct BackgroundCompactorHandle {
    pub start_compaction: Condvar,
    pub compactor_thread: JoinHandle<()>,
}

#[derive(Debug)]
pub(crate) struct ForegroundCompactor<Encoders> {
    pub encoders: Encoders,
}

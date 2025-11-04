pub(crate) use crossbeam_channel::{Receiver, Sender};


pub(crate) fn bounded_channel<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    crossbeam_channel::bounded(size)
}

pub(crate) fn unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    crossbeam_channel::unbounded()
}

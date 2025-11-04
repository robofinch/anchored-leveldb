pub(crate) use kanal::{Receiver, Sender};


pub(crate) fn bounded_channel<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    kanal::bounded(size)
}

pub(crate) fn unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    kanal::unbounded()
}

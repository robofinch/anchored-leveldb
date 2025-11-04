#![allow(
    dead_code,
    clippy::missing_const_for_fn,
    clippy::unreachable,
    clippy::unused_self,
    reason = "want to match API to make sure that the compile_error! below is the error displayed",
)]

use std::{convert::Infallible, marker::PhantomData};


compile_error!("The `kanal` or `crossbeam-channel` feature of `anchored-pool` must be enabled");


#[derive(Debug)]
pub(crate) struct Sender<T> {
    _never: Infallible,
    _t:     PhantomData<fn(T) -> T>,
}

impl<T> Sender<T> {
    pub(crate) fn send(&self, _value: T) -> Result<(), ()> {
        unreachable!()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        unreachable!()
    }
}

#[derive(Debug)]
pub(crate) struct Receiver<T> {
    _never: Infallible,
    _t:     PhantomData<fn(T) -> T>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        unreachable!()
    }
}

impl<T> Receiver<T> {
    pub(crate) fn recv(&self) -> Result<T, ()> {
        unreachable!()
    }

    pub(crate) fn try_recv(&self) -> Result<Option<T>, ()> {
        unreachable!()
    }

    pub(crate) fn capacity(&self) -> usize {
        0
    }

    pub(crate) fn len(&self) -> usize {
        0
    }
}


pub(crate) fn bounded_channel<T>(_size: usize) -> (Sender<T>, Receiver<T>) {
    unreachable!()
}

pub(crate) fn unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    unreachable!()
}

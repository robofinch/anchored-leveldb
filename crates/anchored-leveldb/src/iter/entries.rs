use std::marker::PhantomData;

use super::IterGenerics;


#[derive(Debug)]
pub struct Entries<IG: IterGenerics> {
    _future_proofing: PhantomData<fn() -> IG>,
}

impl<IG: IterGenerics> Entries<IG> {
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        todo!()
    }

    pub fn prev(&mut self) -> Option<(&[u8], &[u8])> {
        todo!()
    }

    pub fn current(&self) -> Option<(&[u8], &[u8])> {
        todo!()
    }
}

impl<IG: IterGenerics> Entries<IG> {
    pub fn seek(&mut self, key: &[u8]) {
        todo!()
    }

    pub fn seek_before(&mut self, key: &[u8]) {
        todo!()
    }

    pub fn seek_to_first(&mut self) {
        todo!()
    }

    pub fn seek_before_first(&mut self) {
        todo!()
    }

    pub fn seek_to_end(&mut self) {
        todo!()
    }

    pub fn seek_before_end(&mut self) {
        todo!()
    }
}

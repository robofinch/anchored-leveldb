use std::marker::PhantomData;

use super::IterGenerics;


#[derive(Debug)]
pub struct PooledKeys<IG: IterGenerics> {
    _future_proofing: PhantomData<fn() -> IG>,
}

impl<IG: IterGenerics> PooledKeys<IG> {
    pub fn next(&mut self) -> Option<OwnedKeyRef> {
        todo!()
    }

    pub fn prev(&mut self) -> Option<OwnedKeyRef> {
        todo!()
    }

    // Not sure if this is worth including,
    // and not sure if this needs to be &mut instead
    pub fn current(&self) -> Option<OwnedKeyRef> {
        todo!()
    }

    pub fn would_block(&self) -> bool {
        todo!()
    }

    pub fn try_next(&mut self) -> Option<Option<OwnedKeyRef>> {
        todo!()
    }

    pub fn try_prev(&mut self) -> Option<Option<OwnedKeyRef>> {
        todo!()
    }

    // Not sure if this is worth including,
    // and not sure if this needs to be &mut instead
    pub fn try_current(&self) -> Option<Option<OwnedKeyRef>> {
        todo!()
    }

    pub fn buffer_pool_size(&self) -> usize {
        todo!()
    }
    pub fn available_buffers(&self) -> usize {
        todo!()
    }
}

impl<IG: IterGenerics> PooledKeys<IG> {
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

impl<IG: IterGenerics> Iterator for PooledKeys<IG> {
    type Item = OwnedKeyRef;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}


#[derive(Debug)]
pub struct OwnedKeyRef {

}

impl OwnedKeyRef {
    pub fn key_ref(&self) -> &[u8] {
        todo!()
    }

    // should probably provide some tools for taking the buffer, if wanted.
}

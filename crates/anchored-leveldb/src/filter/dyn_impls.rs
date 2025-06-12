use std::{rc::Rc, sync::Arc};

use super::FilterPolicy;


impl FilterPolicy for Box<dyn FilterPolicy> {
    #[inline]
    fn name(&self) -> &'static str {
        self.as_ref().name()
    }

    #[inline]
    fn create_filter(&self, keys: &[u8], key_offsets: &[usize]) -> Vec<u8> {
        self.as_ref().create_filter(keys, key_offsets)
    }

    #[inline]
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        self.as_ref().key_may_match(key, filter)
    }
}


impl FilterPolicy for Rc<dyn FilterPolicy> {
    #[inline]
    fn name(&self) -> &'static str {
        self.as_ref().name()
    }

    #[inline]
    fn create_filter(&self, keys: &[u8], key_offsets: &[usize]) -> Vec<u8> {
        self.as_ref().create_filter(keys, key_offsets)
    }

    #[inline]
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        self.as_ref().key_may_match(key, filter)
    }
}

impl FilterPolicy for Arc<dyn FilterPolicy> {
    #[inline]
    fn name(&self) -> &'static str {
        self.as_ref().name()
    }

    #[inline]
    fn create_filter(&self, keys: &[u8], key_offsets: &[usize]) -> Vec<u8> {
        self.as_ref().create_filter(keys, key_offsets)
    }

    #[inline]
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        self.as_ref().key_may_match(key, filter)
    }
}


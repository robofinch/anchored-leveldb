use std::{cmp::Ordering, rc::Rc, sync::Arc};

use super::Comparator;


impl Comparator for Box<dyn Comparator> {
    #[inline]
    fn id(&self) -> &'static str {
        self.as_ref().id()
    }

    #[inline]
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.as_ref().cmp(a, b)
    }

    #[inline]
    fn find_shortest_separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
        self.as_ref().find_shortest_separator(from, to)
    }

    #[inline]
    fn find_shortest_successor(&self, key: &[u8]) -> Vec<u8> {
        self.as_ref().find_shortest_successor(key)
    }
}

impl Comparator for Rc<dyn Comparator> {
    #[inline]
    fn id(&self) -> &'static str {
        self.as_ref().id()
    }

    #[inline]
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.as_ref().cmp(a, b)
    }

    #[inline]
    fn find_shortest_separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
        self.as_ref().find_shortest_separator(from, to)
    }

    #[inline]
    fn find_shortest_successor(&self, key: &[u8]) -> Vec<u8> {
        self.as_ref().find_shortest_successor(key)
    }
}

impl Comparator for Arc<dyn Comparator> {
    #[inline]
    fn id(&self) -> &'static str {
        self.as_ref().id()
    }

    #[inline]
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.as_ref().cmp(a, b)
    }

    #[inline]
    fn find_shortest_separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
        self.as_ref().find_shortest_separator(from, to)
    }

    #[inline]
    fn find_shortest_successor(&self, key: &[u8]) -> Vec<u8> {
        self.as_ref().find_shortest_successor(key)
    }
}

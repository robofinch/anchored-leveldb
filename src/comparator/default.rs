use std::cmp::Ordering;

use super::Comparator;


#[derive(Debug, Clone, Copy)]
pub struct DefaultComparator;

impl Comparator for DefaultComparator {
    fn id(&self) -> &'static str {
        todo!()
    }

    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        todo!()
    }

    fn find_shortest_separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
        todo!()
    }

    fn find_shortest_successor(&self, key: &[u8]) -> Vec<u8> {
        todo!()
    }
}

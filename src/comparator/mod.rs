mod dyn_impls;
mod default;
mod internal;


pub use self::default::DefaultComparator;
pub(crate) use self::internal::InternalComparator;


use std::{cmp::Ordering, fmt::Debug};


pub trait Comparator: Debug {
    fn id(&self) -> &'static str;
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering;

    fn find_shortest_separator(&self, from: &[u8], to: &[u8]) -> Vec<u8>;
    fn find_shortest_successor(&self, key: &[u8]) -> Vec<u8>;
}

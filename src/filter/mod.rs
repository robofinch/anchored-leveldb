mod dyn_impls;
mod bloom;
mod internal;


pub use self::bloom::BloomPolicy;
pub(crate) use self::internal::InternalFilterPolicy;


use std::fmt::Debug;


pub trait FilterPolicy: Debug {
    fn name(&self) -> &'static str;

    fn create_filter(&self, keys: &[u8], key_offsets: &[usize]) -> Vec<u8>;

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

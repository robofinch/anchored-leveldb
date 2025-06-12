use super::FilterPolicy;


#[derive(Debug)]
pub struct BloomPolicy {

}

impl BloomPolicy {
    pub fn new(bits_per_key: u32) -> Self {
        todo!()
    }
}

impl Default for BloomPolicy {
    #[inline]
    fn default() -> Self {
        Self::new(10)
    }
}

impl FilterPolicy for BloomPolicy {
    fn name(&self) -> &'static str {
        todo!()
    }

    fn create_filter(&self, keys: &[u8], key_offsets: &[usize]) -> Vec<u8> {
        todo!()
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        todo!()
    }
}

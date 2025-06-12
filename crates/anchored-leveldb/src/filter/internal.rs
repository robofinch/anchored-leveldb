use super::FilterPolicy;


#[derive(Debug, Clone)]
pub(crate) struct InternalFilterPolicy<FP>(FP);

impl<FP: FilterPolicy> InternalFilterPolicy<FP> {
    #[inline]
    fn name(&self) -> &'static str {
        self.0.name()
    }

    fn create_filter(&self, keys: &[u8], key_offsets: &[usize]) -> Vec<u8> {
        todo!()
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        todo!()
    }
}

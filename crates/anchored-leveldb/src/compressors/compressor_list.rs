use sorted_vector_map::SortedVectorMap;

use super::{Compressor, CompressorId};


#[derive(Debug, Clone)]
pub struct CompressorList(SortedVectorMap<u8, Box<dyn Compressor>>);

impl CompressorList {
    #[inline]
    pub fn new() -> Self {
        Self(SortedVectorMap::new())
    }

    pub fn set<T>(&mut self, compressor: T)
    where
        T: Compressor + CompressorId + 'static,
    {
        todo!()
    }

    pub fn set_with_id<T>(&mut self, id: u8, compressor: T)
    where
        T: Compressor + 'static,
    {
        todo!()
    }

    pub fn is_set(&self, id: u8) -> bool {
        todo!()
    }

    pub fn get(&self, id: u8) -> Option<&dyn Compressor> {
        todo!()
    }
}

impl Default for CompressorList {
    #[inline]
    fn default() -> Self {
        todo!()
    }
}

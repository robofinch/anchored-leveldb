use super::traits::{BufferAllocError, BufferPool, ByteBuffer};


#[derive(Default, Debug, Clone, Copy)]
pub struct NoPool;

#[derive(Default, Debug, Clone)]
pub struct NoPoolBuf(Box<[u8]>, usize);

impl BufferPool for NoPool {
    type PooledBuffer = NoPoolBuf;

    #[inline]
    fn get_buffer(&self, min_capacity: usize) -> Self::PooledBuffer {
        NoPoolBuf(vec![0; min_capacity].into_boxed_slice(), 0)
    }

    #[inline]
    fn try_get_buffer(&self, min_capacity: usize) -> Result<Self::PooledBuffer, BufferAllocError> {
        let mut buf = Vec::new();
        buf.try_reserve_exact(min_capacity)
            .map_err(|_alloc_err| BufferAllocError)?;
        buf.resize(buf.capacity(), 0);
        Ok(NoPoolBuf(buf.into_boxed_slice(), 0))
    }
}

#[expect(clippy::indexing_slicing, reason = "TODO: use `unsafe`")]
impl ByteBuffer for NoPoolBuf {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        &self.0[..self.1]
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0[..self.1]
    }

    #[inline]
    fn len(&self) -> usize {
        self.1
    }

    #[inline]
    fn set_len(&mut self, new_len: usize) {
        assert!(
            new_len <= self.0.len(),
            "`ByteBuffer::set_len` cannot set length to greater than capacity",
        );
        self.1 = new_len;
    }

    #[inline]
    fn as_entire_capacity_slice(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    fn as_entire_capacity_slice_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn as_remaining_capacity_slice(&self) -> &[u8] {
        &self.0[self.1..]
    }

    #[inline]
    fn as_remaining_capacity_slice_mut(&mut self) -> &mut [u8] {
        // TODO: use `unsafe`
        &mut self.0[self.1..]
    }

    #[inline]
    fn remaining_capacity(&self) -> usize {
        self.0.len() - self.1
    }
}

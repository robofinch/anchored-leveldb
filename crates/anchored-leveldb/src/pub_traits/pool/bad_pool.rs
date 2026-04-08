#![expect(unsafe_code, clippy::undocumented_unsafe_blocks, reason = "TODO: make a better pool")]

use std::{alloc, ptr};
use std::mem::ManuallyDrop;
use std::alloc::{Layout, LayoutError};

use kanal::{Sender, Receiver};

use super::traits::{BufferAllocError, BufferPool, ByteBuffer};


#[derive(Debug)]
pub struct BadPool {
    sender:   Sender<Box<[u8]>>,
    receiver: Receiver<Box<[u8]>>,
}

impl BadPool {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        let (sender, receiver) = kanal::unbounded();
        Self { sender, receiver }
    }
}

impl Default for BadPool {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl BufferPool for BadPool {
    type PooledBuffer = BadPoolBuf;

    #[inline]
    fn get_buffer(&self, min_capacity: usize) -> Self::PooledBuffer {
        // Look through the first four buffers
        for _ in 0..4_u8 {
            let Ok(Some(buf)) = self.receiver.try_recv() else {
                break;
            };
            if buf.len() >= min_capacity {
                return BadPoolBuf(ManuallyDrop::new(buf), 0, self.sender.clone());
            }
        }

        let buf = vec![0; min_capacity].into_boxed_slice();
        BadPoolBuf(ManuallyDrop::new(buf), 0, self.sender.clone())
    }

    #[inline]
    fn try_get_buffer(&self, min_capacity: usize) -> Result<Self::PooledBuffer, BufferAllocError> {
        fn try_new_zeroed_slice_polyfill(len: usize) -> Result<Box<[u8]>, BufferAllocError> {
            // Original:
            // let ptr = if T::IS_ZST || len == 0 {
            //     NonNull::dangling()
            // } else {
            //     let layout = match Layout::array::<mem::MaybeUninit<T>>(len) {
            //         Ok(l) => l,
            //         Err(_) => return Err(AllocError),
            //     };
            //     Global.allocate_zeroed(layout)?.cast()
            // };
            // unsafe { Ok(RawVec::from_raw_parts_in(ptr.as_ptr(), len, Global).into_box(len)) }
            let ptr = if len == 0 {
                ptr::dangling_mut()
            } else {
                let layout = Layout::array::<u8>(len)
                    .map_err(|LayoutError {..}| BufferAllocError)?;

                let ptr = unsafe { alloc::alloc_zeroed(layout) };

                if ptr.is_null() {
                    return Err(BufferAllocError);
                }
                ptr
            };

            let allocated_slice = ptr::slice_from_raw_parts_mut(ptr, len);

            Ok(unsafe {
                Box::<[u8]>::from_raw(allocated_slice)
            })
        }

        // Look through the first four buffers
        for _ in 0..4_u8 {
            let Ok(Some(buf)) = self.receiver.try_recv() else {
                break;
            };
            if buf.len() >= min_capacity {
                return Ok(BadPoolBuf(ManuallyDrop::new(buf), 0, self.sender.clone()));
            }
        }

        let buf = try_new_zeroed_slice_polyfill(min_capacity)?;
        Ok(BadPoolBuf(ManuallyDrop::new(buf), 0, self.sender.clone()))
    }
}

#[derive(Debug, Clone)]
pub struct BadPoolBuf(ManuallyDrop<Box<[u8]>>, usize, Sender<Box<[u8]>>);

impl ByteBuffer for BadPoolBuf {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        unsafe { self.0.get_unchecked(..self.1) }
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.0.get_unchecked_mut(..self.1) }
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
        unsafe { self.0.get_unchecked(self.1..) }
    }

    #[inline]
    fn as_remaining_capacity_slice_mut(&mut self) -> &mut [u8] {
        unsafe { self.0.get_unchecked_mut(self.1..) }
    }

    #[inline]
    fn remaining_capacity(&self) -> usize {
        self.0.len() - self.1
    }
}

impl Drop for BadPoolBuf {
    fn drop(&mut self) {
        let buf = unsafe { ManuallyDrop::take(&mut self.0) };
        let _ignore = self.2.send(buf);
    }
}

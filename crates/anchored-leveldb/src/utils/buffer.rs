use crate::pub_traits::pool::{BufferAllocError, BufferPool, ByteBuffer as _};


/// If a buffer is successfully returned, it has length exactly `desired_len`.
///
/// However, the data might be random initialized `u8` values.
pub(crate) fn get_buffer<Pool: BufferPool>(
    pool: &Pool,
    existing_buf: &mut Option<Pool::PooledBuffer>,
    desired_len: usize,
) -> Result<Pool::PooledBuffer, BufferAllocError> {
    let existing_buf = existing_buf.take_if(|buf| buf.capacity() >= desired_len);

    let mut buf = if let Some(buf) = existing_buf {
        buf
    } else {
        pool.try_get_buffer(desired_len)?
    };

    // Note: We ensured that `desired_len <= buf.capacity()`, so this does not panic.
    buf.set_len(desired_len);

    Ok(buf)
}

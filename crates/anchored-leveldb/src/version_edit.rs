use crate::public_format::EntryType;
use crate::format::{InternalKey, MAX_SEEKS_BETWEEN_COMPACTIONS, SequenceNumber};


#[derive(Debug)]
pub struct FileMetadata {
    allowed_seeks:         u32,
    file_number:           u64,
    file_size:             u64,
    // The bet is that user keys are usually so short compared to 4096-byte blocks common in the
    // main buffer pool that using the blocks' buffer pool would not be worth it, and using
    // a second buffer pool just for small and relatively-rarely allocated buffers is not worth it.
    user_key_buffer:       Vec<u8>,
    smallest_user_key_len: usize,
    smallest_seq:          SequenceNumber,
    smallest_entry_type:   EntryType,
    largest_seq:           SequenceNumber,
    largest_entry_type:    EntryType,
}

impl FileMetadata {
    /// `buffer` must be an empty buffer.
    #[inline]
    #[must_use]
    pub fn new(
        file_number:  u64,
        file_size:    u64,
        smallest_key: InternalKey<'_>,
        largest_key:  InternalKey<'_>,
    ) -> Self {
        let smallest_user_key_len = smallest_key.user_key.0.len();

        let mut user_key_buffer = Vec::with_capacity(
            smallest_user_key_len + largest_key.user_key.0.len(),
        );
        user_key_buffer.extend(smallest_key.user_key.0);
        user_key_buffer.extend(largest_key.user_key.0);

        Self {
            allowed_seeks:         MAX_SEEKS_BETWEEN_COMPACTIONS,
            file_number,
            file_size,
            user_key_buffer,
            smallest_user_key_len,
            smallest_seq:          smallest_key.sequence_number,
            smallest_entry_type:   smallest_key.entry_type,
            largest_seq:           largest_key.sequence_number,
            largest_entry_type:    largest_key.entry_type,
        }
    }
}

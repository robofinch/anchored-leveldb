use std::sync::atomic::{AtomicU32, Ordering};

use crate::{containers::RefcountedFamily, public_format::EntryType};
use crate::format::{FileNumber, InternalKey, SequenceNumber, UserKey};


pub type RefcountedFileMetadata<Refcounted>
    = <Refcounted as RefcountedFamily>::Container<FileMetadata>;


pub const MAX_SEEKS_BETWEEN_COMPACTIONS: u32 = (1 << 31) - 1;

#[derive(Debug, Clone, Copy)]
pub struct SeeksBetweenCompactionOptions {
    pub min:           u32,
    pub per_file_size: u32,
}

impl Default for SeeksBetweenCompactionOptions {
    #[inline]
    fn default() -> Self {
        Self {
            min:           100,
            per_file_size: 16384, // 1 << 14
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SeeksRemaining {
    Some,
    None,
}

#[derive(Debug)]
pub(crate) struct FileMetadata {
    // TODO(micro-opt): could be Cell<u32> when single-threaded. However, it seems unlikely
    // that the atomic operation would add that much overhead compared to the rest of the code.
    remaining_seeks:       AtomicU32,
    file_number:           FileNumber,
    file_size:             u64,
    // The bet is that user keys are usually so short compared to 4096-byte blocks common in the
    // main buffer pool that using the blocks' buffer pool would not be worth it, and using
    // a second buffer pool just for small and relatively-rarely allocated buffers is not worth it.
    /// Consists of the smallest user key concatenated with the largest user key.
    ///
    /// As an invariant, its length is always at least `self.smallest_user_key_len`.
    user_key_buffer:       Vec<u8>,
    smallest_user_key_len: usize,
    smallest_seq:          SequenceNumber,
    smallest_entry_type:   EntryType,
    largest_seq:           SequenceNumber,
    largest_entry_type:    EntryType,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl FileMetadata {
    /// `buffer` must be an empty buffer.
    #[must_use]
    pub fn new(
        file_number:  FileNumber,
        file_size:    u64,
        smallest_key: InternalKey<'_>,
        largest_key:  InternalKey<'_>,
        opts:         SeeksBetweenCompactionOptions,
    ) -> Self {
        let smallest_user_key_len = smallest_key.user_key.0.len();

        let mut user_key_buffer = Vec::with_capacity(
            smallest_user_key_len + largest_key.user_key.0.len(),
        );
        // Invariant satisfied here: length is at least `smallest_user_key_len`.
        user_key_buffer.extend(smallest_key.user_key.0);
        user_key_buffer.extend(largest_key.user_key.0);

        #[expect(clippy::integer_division, reason = "intentional; exact value does not matter")]
        let allowed_seeks = file_size / u64::from(opts.per_file_size);
        let allowed_seeks = u32::try_from(allowed_seeks)
            .unwrap_or(u32::MAX)
            .max(opts.min); // If `opts.min` is larger, `max(_)` will output that minimum.

        // This maximum value of `(1 << 31) - 1` (which is `u32::MAX/2`) takes priority over
        // `opts.min` and whatnot. This ensures that `self.remaining_seeks.fetch_sub(1)` cannot
        // wrap around to a sensible value unless `self.record_seek()` is called billions of times
        // concurrently, which is essentially impossible.
        let allowed_seeks = allowed_seeks.min(MAX_SEEKS_BETWEEN_COMPACTIONS);

        Self {
            remaining_seeks:       AtomicU32::new(allowed_seeks),
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

    #[must_use]
    pub fn record_seek(&self) -> SeeksRemaining {
        // `Ordering::Relaxed` is used because the exact value doesn't particularly matter.
        // This function is used as a heuristic for when to perform compactions. It doesn't matter
        // exactly when a compaction is triggered.
        let remaining_seeks = self.remaining_seeks.fetch_sub(1, Ordering::Relaxed);

        if remaining_seeks > MAX_SEEKS_BETWEEN_COMPACTIONS {
            self.remaining_seeks.store(0, Ordering::Relaxed);
            SeeksRemaining::None
        } else if remaining_seeks == 0 {
            // If `self.record_seek()` is called again, it will observe a value which has wrapped
            // around to near `u32::MAX`, and the above case will be taken. It's not like it'll
            // wrap around below `MAX_SEEKS_BETWEEN_COMPACTIONS` if we skip the store when it
            // wrapped to `u32::MAX` just now; and the file associated with this `FileMetadata`
            // might be compacted and deleted before the next time `self.record_seek()` would've
            // been called.
            // TLDR this is a micro-opt.
            SeeksRemaining::None
        } else {
            SeeksRemaining::Some
        }
    }

    #[must_use]
    pub const fn file_number(&self) -> FileNumber {
        self.file_number
    }

    #[must_use]
    pub const fn file_size(&self) -> u64 {
        self.file_size
    }

    #[must_use]
    pub fn smallest_user_key(&self) -> UserKey<'_> {
        #[expect(
            clippy::indexing_slicing,
            reason = "invariant: len of `user_key_buffer` is at least `self.smallest_user_key_len`",
        )]
        let smallest_user_key = &self.user_key_buffer[..self.smallest_user_key_len];
        UserKey(smallest_user_key)
    }

    #[must_use]
    pub fn largest_user_key(&self) -> UserKey<'_> {
        #[expect(
            clippy::indexing_slicing,
            reason = "invariant: len of `user_key_buffer` is at least `self.smallest_user_key_len`",
        )]
        let largest_user_key = &self.user_key_buffer[self.smallest_user_key_len..];
        UserKey(largest_user_key)
    }

    #[must_use]
    pub fn smallest_key(&self) -> InternalKey<'_> {
        InternalKey {
            user_key:        self.smallest_user_key(),
            sequence_number: self.smallest_seq,
            entry_type:      self.smallest_entry_type,
        }
    }

    #[must_use]
    pub fn largest_key(&self) -> InternalKey<'_> {
        InternalKey {
            user_key:        self.largest_user_key(),
            sequence_number: self.largest_seq,
            entry_type:      self.largest_entry_type,
        }
    }
}

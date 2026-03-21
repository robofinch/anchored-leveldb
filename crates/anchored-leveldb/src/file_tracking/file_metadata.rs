use std::num::NonZeroU64;
use std::sync::{Arc, atomic::{AtomicU32, Ordering}};

use crate::options::SeekCompactionOptions;
use crate::{
    pub_typed_bytes::{EntryType, FileNumber, FileSize, Level, MinU32Usize, SequenceNumber},
    typed_bytes::{InternalKey, InternalKeyTag, UserKey},
};


/// Metadata for a table file.
#[derive(Debug)]
pub(crate) struct FileMetadata {
    remaining_seeks:       AtomicU32,
    file_number:           FileNumber,
    file_size:             FileSize,
    // The bet is that user keys are usually so short compared to table blocks that using the
    // blocks' buffer pool would not be worth it, and using a second buffer pool just for small and
    // relatively-rarely allocated buffers is not worth it.
    //
    /// Consists of the smallest user key concatenated with the largest user key.
    ///
    /// # Invariants
    /// Its length is always at least `self.smallest_user_key_len`.
    ///
    /// `&self.user_key_buffer[..self.smallest_user_key_len]` and
    /// `&self.user_key_buffer[self.smallest_user_key_len..]` should both be validated `UserKey`s.
    user_key_buffer:       Vec<u8>,
    smallest_user_key_len: MinU32Usize,
    smallest_seq:          SequenceNumber,
    smallest_entry_type:   EntryType,
    largest_seq:           SequenceNumber,
    largest_entry_type:    EntryType,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl FileMetadata {
    #[must_use]
    pub fn new(
        file_number:  FileNumber,
        file_size:    FileSize,
        smallest_key: InternalKey<'_>,
        largest_key:  InternalKey<'_>,
        opts:         SeekCompactionOptions,
    ) -> Self {
        let smallest_user_key_len = smallest_key.0.len();

        let mut user_key_buffer = Vec::with_capacity(
            usize::from(smallest_user_key_len).saturating_add(usize::from(largest_key.0.len())),
        );
        // Invariant satisfied here: length is at least `smallest_user_key_len`,
        // and both keys are valid user keys.
        user_key_buffer.extend(smallest_key.0.inner());
        user_key_buffer.extend(largest_key.0.inner());

        Self {
            remaining_seeks:       AtomicU32::new(Self::allowed_seeks(opts, file_size)),
            file_number,
            file_size,
            user_key_buffer,
            smallest_user_key_len,
            smallest_seq:          smallest_key.1.sequence_number(),
            smallest_entry_type:   smallest_key.1.entry_type(),
            largest_seq:           largest_key.1.sequence_number(),
            largest_entry_type:    largest_key.1.entry_type(),
        }
    }

    fn allowed_seeks(opts: SeekCompactionOptions, file_size: FileSize) -> u32 {
        #[expect(clippy::integer_division, reason = "intentional; exact value does not matter")]
        let allowed_seeks = file_size.0 / NonZeroU64::from(opts.file_bytes_per_seek);

        let allowed_seeks = u32::try_from(allowed_seeks)
            .unwrap_or(u32::MAX)
            .max(opts.min_allowed_seeks);

        // This maximum value of `u32::MAX/2` takes priority over `opts.min_allowed_seeks` and
        // whatnot. This ensures that `self.remaining_seeks.fetch_sub(1)` cannot wrap around to a
        // sensible value unless `self.record_seek()` has an absurd number of concurrent calls,
        // which is essentially impossible.
        allowed_seeks.min(u32::MAX >> 1)
    }

    #[must_use]
    pub fn record_seek(&self, weight: u16) -> SeeksRemaining {
        // `Ordering::Relaxed` is used because the exact value doesn't particularly matter.
        // This function is used as a heuristic for when to perform compactions. It doesn't matter
        // exactly when a compaction is triggered.
        // Note that over 32000 calls to this function would need to be made concurrently
        // in order for this to unexpectedly wrap around to below `u32::MAX >> 1`,
        // since `(u32::MAX >> 1) / u32::from(u16::MAX) == 32,768`.
        // That will not happen. And even if it did on some insane computer with thousands of
        // logical cores, no `unsafe` code depends on exactly when a file runs out of seeks; the
        // exact value of remaining seeks is not visible outside this module, and this module has
        // no `unsafe`.
        let remaining_seeks = self.remaining_seeks.fetch_sub(u32::from(weight), Ordering::Relaxed);

        // If `fetch_sub` returns `0`, that means the value we wrote wrapped around to above
        // `u32::MAX >> 1`. Likewise, if we see something above `u32::MAX >> 1`, we assume that
        // subtraction had wrapped around, and we're actually out of seeks.
        if remaining_seeks > (u32::MAX >> 1) || remaining_seeks == 0 {
            self.remaining_seeks.store(0, Ordering::Relaxed);
            SeeksRemaining::None
        } else {
            SeeksRemaining::Some
        }
    }

    pub fn reset_remaining_seeks(&self, opts: SeekCompactionOptions) {
        self.remaining_seeks.store(
            Self::allowed_seeks(opts, self.file_size),
            Ordering::Relaxed,
        );
    }

    #[must_use]
    pub const fn file_number(&self) -> FileNumber {
        self.file_number
    }

    #[must_use]
    pub const fn file_size(&self) -> FileSize {
        self.file_size
    }

    #[must_use]
    pub fn smallest_user_key(&self) -> UserKey<'_> {
        #![expect(
            clippy::expect_used,
            clippy::indexing_slicing,
            reason = "necessarily succeeds, as ensured by constructor",
        )]
        let smallest_user_key = &self.user_key_buffer[..usize::from(self.smallest_user_key_len)];
        // Correctness: `self.user_key_buffer` and `self.smallest_user_key_len` are only written
        // in `FileMetadata::new`, where it is guaranteed that this first half of `user_key_buffer`
        // came from a valid `UserKey`.
        UserKey::new(smallest_user_key).expect("`FileMetadata` stores a valid `smallest_user_key`")
    }

    #[must_use]
    pub fn largest_user_key(&self) -> UserKey<'_> {
        #![expect(
            clippy::expect_used,
            clippy::indexing_slicing,
            reason = "necessarily succeeds, as ensured by constructor",
        )]
        let largest_user_key = &self.user_key_buffer[usize::from(self.smallest_user_key_len)..];
        // Correctness: `self.user_key_buffer` and `self.smallest_user_key_len` are only written
        // in `FileMetadata::new`, where it is guaranteed that this second half of `user_key_buffer`
        // came from a valid `UserKey`.
        UserKey::new(largest_user_key).expect("`FileMetadata` stores a valid `largest_user_key")
    }

    #[must_use]
    pub fn smallest_key(&self) -> InternalKey<'_> {
        InternalKey(
            self.smallest_user_key(),
            InternalKeyTag::new(self.smallest_seq, self.smallest_entry_type),
        )
    }

    #[must_use]
    pub fn largest_key(&self) -> InternalKey<'_> {
        InternalKey(
            self.largest_user_key(),
            InternalKeyTag::new(self.largest_seq, self.largest_entry_type),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SeeksRemaining {
    Some,
    None,
}

/// Indicates that a seek compaction should occur to reduce file overlaps.
///
/// If a multiple files overlap a certain key, a "seek" may be recorded on one of the files,
/// indicating that an additional file needed to be read and seeked through (implying that
/// performing a compaction on that file would improve read performance). Once too many seeks
/// occur for a given file, an instance of this type may be produced to indicate which file
/// ran out of allowed seeks.
#[derive(Debug)]
pub(crate) struct StartSeekCompaction {
    pub level: Level,
    pub file:  Arc<FileMetadata>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl StartSeekCompaction {
    /// Determine whether the indicated file has fewer than `weight` remaining seeks allowed; if the
    /// file runs out of allowed seeks, a `Some` value is returned.
    ///
    /// If a multiple files overlap a certain key, a "seek" may be recorded on one of the files,
    /// indicating that an additional file needed to be read and seeked through (implying that
    /// performing a compaction on that file would improve read performance). Once too many seeks
    /// occur for a given file, `Some` is returned, indicating which file ran out of allowed seeks.
    #[must_use]
    pub fn record_seek(
        maybe_seek: Option<(Level, &Arc<FileMetadata>)>,
        mut weight: u32,
    ) -> Option<Self> {
        let (level, file) = maybe_seek?;

        while let Some(decremented) = weight.checked_sub(u32::from(u16::MAX)) {
            weight = decremented;
            // Recording additional seeks beyond the limit continues to return
            // `SeeksRemaining::None`. This is an absurd edge case anyway; with default
            // settings, the average file reaching this loop would be around 2^36 bytes in size
            // (68 gigabytes). There's no need to complicate the control flow with an early
            // return.
            let _checked_below = file.record_seek(u16::MAX);
        }

        // If the above loop is reached, `weight` may be zero. That's fine, `record_seek`
        // works on any `u16` input.
        #[expect(
            clippy::as_conversions,
            clippy::cast_possible_truncation,
            reason = "we get here iff `weight < u32::from(u16::MAX)`, so there's no truncation",
        )]
        match file.record_seek(weight as u16) {
            // The file can still have some more seeks before it needs to be compacted
            SeeksRemaining::Some => None,
            // The file should be compacted since it ran out of allowed seeks.
            SeeksRemaining::None => Some(Self {
                level,
                file: Arc::clone(file),
            }),
        }
    }
}

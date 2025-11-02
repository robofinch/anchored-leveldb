use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::atomic::{AtomicU32, Ordering},
};

use clone_behavior::{AnySpeed, IndependentClone, MirroredClone as _};

use crate::{containers::RefcountedFamily, public_format::EntryType};
use crate::format::{FileNumber, InternalKey, SequenceNumber, UserKey};
use super::level::Level;


pub type RefcountedFileMetadata<Refcounted>
    = <Refcounted as RefcountedFamily>::Container<FileMetadata>;


const MAX_SEEKS_BETWEEN_COMPACTIONS: u32 = (1 << 31) - 1;


/// Metadata for a table file.
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

        Self {
            remaining_seeks:       AtomicU32::new(opts.allowed_seeks(file_size)),
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

    pub fn reset_remaining_seeks(&self, opts: SeeksBetweenCompactionOptions) {
        self.remaining_seeks.store(opts.allowed_seeks(self.file_size), Ordering::Relaxed);
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

impl IndependentClone<AnySpeed> for FileMetadata {
    #[inline]
    fn independent_clone(&self) -> Self {
        Self {
            remaining_seeks:       AtomicU32::new(self.remaining_seeks.load(Ordering::Relaxed)),
            file_number:           self.file_number,
            file_size:             self.file_size,
            user_key_buffer:       self.user_key_buffer.clone(),
            smallest_user_key_len: self.smallest_user_key_len,
            smallest_seq:          self.smallest_seq,
            smallest_entry_type:   self.smallest_entry_type,
            largest_seq:           self.largest_seq,
            largest_entry_type:    self.largest_entry_type,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SeeksBetweenCompactionOptions {
    // TODO: bikeshed option names
    /// Ignored if greater than `(1 << 31) - 1`, which is `u32::MAX/2`.
    pub min_allowed_seeks: u32,
    pub per_file_size:     u32,
}

impl SeeksBetweenCompactionOptions {
    fn allowed_seeks(self, file_size: u64) -> u32 {
        #[expect(clippy::integer_division, reason = "intentional; exact value does not matter")]
        let allowed_seeks = file_size / u64::from(self.per_file_size);
        let allowed_seeks = u32::try_from(allowed_seeks)
            .unwrap_or(u32::MAX)
            .max(self.min_allowed_seeks);

        // This maximum value of `(1 << 31) - 1` (which is `u32::MAX/2`) takes priority over
        // `opts.min_allowed_seeks` and whatnot. This ensures that
        // `self.remaining_seeks.fetch_sub(1)` cannot wrap around to a sensible value
        // unless `self.record_seek()` is called billions of times concurrently,
        // which is essentially impossible.
        allowed_seeks.min(MAX_SEEKS_BETWEEN_COMPACTIONS)
    }
}

impl Default for SeeksBetweenCompactionOptions {
    #[inline]
    fn default() -> Self {
        Self {
            min_allowed_seeks: 100,
            per_file_size:     16384, // 1 << 14
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SeeksRemaining {
    Some,
    None,
}

/// Indicates whether a "seek compaction" should occur to reduce file overlaps.
///
/// If a multiple files overlap a certain key, a "seek" may be recorded on one of the files,
/// indicating that an additional file needed to be read and seeked through (implying that
/// performing a compaction on that file would improve read performance). Once too many seeks
/// occur for a given file, the `Some` variant of this enum may be returned, indicating
/// which file ran out of allowed seeks.
// Does _not_ implement Debug or MirroredClone, since this is an internal type and
// `CurrentVersion` does not derive Debug or implement MirroredClone.
pub(crate) enum MaybeSeekCompaction<Refcounted: RefcountedFamily> {
    Some(Level, RefcountedFileMetadata<Refcounted>),
    None,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> MaybeSeekCompaction<Refcounted> {
    #[must_use]
    pub fn record_seek(maybe_seek: Option<(Level, &RefcountedFileMetadata<Refcounted>)>) -> Self {
        if let Some((level, file)) = maybe_seek {
            match file.record_seek() {
                // The file can still have some more seeks before it needs to be compacted
                SeeksRemaining::Some => Self::None,
                // The file should be compacted since it ran out of allowed seeks.
                SeeksRemaining::None => Self::Some(level, file.mirrored_clone()),
            }
        } else {
            Self::None
        }
    }
}

impl<Refcounted: RefcountedFamily> Debug for MaybeSeekCompaction<Refcounted> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Some(level, metadata) => {
                f.debug_tuple("Some")
                    .field(&level)
                    .field(Refcounted::debug(metadata))
                    .finish()
            }
            Self::None => f.write_str("None"),
        }
    }
}

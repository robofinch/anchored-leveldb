use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    num::{NonZeroU32, NonZeroU64},
    sync::atomic::{AtomicU32, Ordering},
};

use clone_behavior::{DeepClone, MaybeSlow, MirroredClone as _};

use crate::{containers::RefcountedFamily, public_format::EntryType};
use crate::format::{FileNumber, InternalKey, SequenceNumber, UserKey};
use super::level::Level;


pub(crate) type RefcountedFileMetadata<Refcounted>
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
    pub fn record_seek(&self, weight: u16) -> SeeksRemaining {
        // `Ordering::Relaxed` is used because the exact value doesn't particularly matter.
        // This function is used as a heuristic for when to perform compactions. It doesn't matter
        // exactly when a compaction is triggered.
        // Note that over 32000 calls to this function would need to be made concurrently
        // in order for this to unexpectedly wrap around to below `MAX_SEEKS_BETWEEN_COMPACTIONS`,
        // since `MAX_SEEKS_BETWEEN_COMPACTIONS / u32::from(u16::MAX) == 32,768`.
        // That will not happen. And even if it did on some insane computer with thousands of
        // logical cores, no `unsafe` code depends on exactly when a file runs out of seeks; the
        // exact value of remaining seeks is not visible outside this module, and this module has
        // no `unsafe`.
        let remaining_seeks = self.remaining_seeks.fetch_sub(u32::from(weight), Ordering::Relaxed);

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

impl DeepClone<MaybeSlow> for FileMetadata {
    #[inline]
    fn deep_clone(&self) -> Self {
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

/// Settings for how many times an unnecessary read to a file must occur before a seek compaction
/// is triggered on that file.
///
/// The limit on unnecessary reads to a file is calculated based on the file's size, with
/// larger files permitting a greater number of reads before a compaction (as compaction is
/// more expensive for larger files). The limit is clamped to the inclusive range
/// `[self.min_allowed_seeks, u32::MAX/2]`, with the `u32::MAX/2` maximum taking priority over
/// the provided `self.min_allowed_seeks` minimum option.
#[derive(Debug, Clone, Copy)]
pub struct SeeksBetweenCompactionOptions {
    /// Ignored if greater than `u32::MAX/2`.
    ///
    /// Defaults to 100.
    pub min_allowed_seeks:   u32,
    /// Defaults to 16 KiB.
    pub file_bytes_per_seek: NonZeroU32,
}

impl SeeksBetweenCompactionOptions {
    fn allowed_seeks(self, file_size: u64) -> u32 {
        #[expect(clippy::integer_division, reason = "intentional; exact value does not matter")]
        let allowed_seeks = file_size / NonZeroU64::from(self.file_bytes_per_seek);
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
            min_allowed_seeks:   100,
            #[allow(
                clippy::unwrap_used,
                reason = "value is nonzero. Plus, it's checked at compile time",
            )]
            file_bytes_per_seek: const { NonZeroU32::new(16_384).unwrap() }, // 1 << 14
        }
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
// Does _not_ implement MirroredClone
pub(crate) struct StartSeekCompaction<Refcounted: RefcountedFamily> {
    pub level: Level,
    pub file:  RefcountedFileMetadata<Refcounted>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Refcounted: RefcountedFamily> StartSeekCompaction<Refcounted> {
    /// Determine whether the indicated file has fewer than `weight` remaining seeks allowed; if the
    /// file runs out of allowed seeks, a `Some` value is returned.
    ///
    /// If a multiple files overlap a certain key, a "seek" may be recorded on one of the files,
    /// indicating that an additional file needed to be read and seeked through (implying that
    /// performing a compaction on that file would improve read performance). Once too many seeks
    /// occur for a given file, `Some` is returned, indicating which file ran out of allowed seeks.
    #[must_use]
    pub fn record_seek(
        maybe_seek: Option<(Level, &RefcountedFileMetadata<Refcounted>)>,
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
                file: file.mirrored_clone(),
            }),
        }
    }
}

impl<Refcounted: RefcountedFamily> Debug for StartSeekCompaction<Refcounted> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("StartSeekCompaction")
            .field("level", &self.level)
            .field("file",  Refcounted::debug(&self.file))
            .finish()
    }
}

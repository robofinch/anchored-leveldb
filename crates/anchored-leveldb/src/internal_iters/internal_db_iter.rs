#![expect(
    unsafe_code,
    reason = "needed to impl an `into_inner`-ish method for a type that impls Drop",
)]

use std::mem;
use std::{mem::ManuallyDrop, sync::Arc};

use clone_behavior::FastMirroredClone;

use anchored_vfs::LevelDBFilesystem;

use crate::{
    internal_leveldb::InternalDBState,
    pub_leveldb::DB,
    read_sampling::IterReadSampler,
    version::Version,
};
use crate::{
    all_errors::{
        aliases::{RwErrorAlias, RwErrorKindAlias, RwResult},
        types::RwError,
    },
    options::{InternalReadOptions, pub_options::ReadOptions},
    pub_traits::{
        cmp_and_policy::{CoarserThan, FilterPolicy, LevelDBComparator},
        compression::CompressionCodecs,
        pool::BufferPool,
    },
    pub_typed_bytes::{EntryType, SequenceNumber},
    typed_bytes::{
        ContinueSampling, InternalEntry, InternalKey, InternalKeyTag, OwnedUserKey, OwnedUserValue,
        UserKey, UserValue,
    },
};
use super::iter_to_merge::IterToMerge;
use super::linear_merging_iter::{MergingIter, MergingIterWithOpts};


/// Part of the state of an [`ActiveInternalDBIter`] which cannot be stored with the rest of the
/// struct due to borrowck issues.
#[derive(Debug)]
pub(crate) struct ExtraState<'a, Decoders> {
    current:  &'a mut MaybeSavedEntry,
    decoders: &'a mut Decoders,
}

#[derive(Debug)]
enum MaybeSavedEntry {
    BackwardsSome(OwnedUserKey, OwnedUserValue),
    Buffers(Vec<u8>, Vec<u8>),
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl MaybeSavedEntry {
    #[inline]
    #[must_use]
    pub const fn take(&mut self) -> Self {
        mem::replace(self, Self::Buffers(Vec::new(), Vec::new()))
    }

    #[must_use]
    pub fn take_into_key_buf(&mut self) -> Vec<u8> {
        match self.take() {
            Self::BackwardsSome(key, _) => key.into_inner(),
            Self::Buffers(key_buf, _)   => key_buf,
        }
    }

    #[must_use = "if you don't need the return value, just set `*self`"]
    pub fn set_to_entry(
        &mut self,
        owned_key:   OwnedUserKey,
        owned_value: OwnedUserValue,
    ) -> (UserKey<'_>, UserValue<'_>) {
        *self = Self::BackwardsSome(owned_key, owned_value);

        #[expect(clippy::unreachable, reason = "we just set `self` to the `BackwardsSome` variant")]
        match self {
            Self::BackwardsSome(key, value) => (key.borrow(), value.borrow()),
            Self::Buffers(_, _)             => unreachable!(),
        }
    }

    #[must_use = "if you don't need the return value, just set `*self`"]
    pub fn set_to_buffers(
        &mut self,
        key_buf:   Vec<u8>,
        value_buf: Vec<u8>,
    ) -> (&mut Vec<u8>, &mut Vec<u8>) {
        *self = Self::Buffers(key_buf, value_buf);

        #[expect(clippy::unreachable, reason = "we just set `self` to the `Buffers` variant")]
        match self {
            Self::BackwardsSome(_, _) => unreachable!(),
            Self::Buffers(key, value) => (key, value),
        }
    }
}

#[derive(Debug)]
enum MaybeSavedValue {
    Value(OwnedUserValue),
    Buffer(Vec<u8>),
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl MaybeSavedValue {
    #[inline]
    #[must_use]
    pub fn new(entry: InternalEntry<'_>, value_buf: Vec<u8>) -> Self {
        match entry.0.1.entry_type() {
            EntryType::Deletion => Self::Buffer(value_buf),
            EntryType::Value    => {
                Self::Value(entry.not_deleted_user_value().to_owned_with_buf(value_buf))
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn into_buf(self) -> Vec<u8> {
        match self {
            Self::Value(value)      => value.into_inner(),
            Self::Buffer(value_buf) => value_buf,
        }
    }
}

/// An `InternalDBIter` provides access to the user entries of a LevelDB database.
///
/// However, if database corruption occurs, all bets are off in regards to exactly what is returned;
/// it is only guaranteed that no panics or memory unsafety will occur in such a case.
// TODO: Debug impl
pub(crate) struct InternalDBIter<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// If `valid()`, its `current()` must be at a `Value` entry whose sequence number is
    /// the greatest sequence number less than `self`'s sequence number, among the sequence numbers
    /// of entries for the user key of `current()`.
    iter:            MergingIter<FS::RandomAccessFile, Cmp, Policy, Pool>,
    db:              DB<FS, Cmp, Policy, Codecs, Pool>,
    read_opts:       InternalReadOptions,
    sampler:         Option<IterReadSampler>,
    /// The iterator will show what the database's state is/was as of this sequence number.
    sequence_number: SequenceNumber,
    /// The current version, at the time the iterator was created.
    version:         Arc<Version>,
    /// Usually in the `Buffers` state, in which case `self.iter.current()` is currently at the
    /// semantically current key (or returned an error).
    ///
    /// If in the `BackwardsSome` variant, then the semantically current entry (that is,
    /// `self.activate().current()`) is the indicated entry, while `self.iter.current()` is
    /// one internal entry *before* the indicated entry.
    current:         MaybeSavedEntry,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBIter<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator + FastMirroredClone,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    pub fn new(
        mut db:    DB<FS, Cmp, Policy, Codecs, Pool>,
        read_opts: &ReadOptions,
    ) -> RwResult<Self, FS, Cmp, Codecs> {
        let (internal_state, per_handle) = db.inner();

        let verify_data_checksums = read_opts.verify_data_checksums
            .unwrap_or(internal_state.opts.verify_data_checksums);
        let verify_index_checksums = read_opts.verify_index_checksums
            .unwrap_or(internal_state.opts.verify_index_checksums);

        let internal_read_opts = InternalReadOptions {
            verify_data_checksums,
            verify_index_checksums,
            block_cache_usage: read_opts.block_cache_usage,
            table_cache_usage: read_opts.table_cache_usage,
        };

        let mut mut_state = internal_state.lock_mutable_state();

        let seek_opts = internal_state.opts.compaction.seek_compactions;

        let sampler = if read_opts.record_seeks && seek_opts.seek_autocompactions {
            Some(IterReadSampler::new(seek_opts, &mut mut_state.iter_read_sample_seed))
        } else {
            None
        };

        let sequence_number = if let Some(snapshot) = &read_opts.snapshot {
            snapshot.sequence_number()
        } else {
            mut_state.version_set.last_sequence()
        };

        let version = mut_state.version_set.cloned_current_version();

        let mut iters = Vec::new();

        iters.push(IterToMerge::Memtable(
            mut_state.current_memtable.fast_mirrored_clone().lending_iter(),
        ));

        if let Some(imm) = &mut_state.compaction_state.memtable_under_compaction {
            iters.push(IterToMerge::Memtable(imm.fast_mirrored_clone().lending_iter()));
        }

        let manifest_number = mut_state.version_set.manifest_file_number();

        version
            .add_iterators(
                &internal_state.opts,
                &internal_state.mut_opts,
                internal_read_opts,
                &mut per_handle.decoders,
                manifest_number,
                &mut iters,
            )
            .map_err(|kind| RwError {
                db_directory: internal_state.opts.db_directory.clone(),
                kind
            })?;

        let iter = MergingIter::new(iters);

        let current = MaybeSavedEntry::Buffers(
            mem::take(&mut per_handle.iter_key_buf),
            Vec::new(),
        );

        mut_state.lockfile_refcount += 1;

        drop(mut_state);

        Ok(Self {
            iter,
            db,
            read_opts: internal_read_opts,
            sampler,
            sequence_number,
            version,
            current,
        })
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> InternalDBIter<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    #[expect(clippy::type_complexity, reason = "tuple of two types; only complicated by generics")]
    pub fn activate(
        &mut self,
    ) -> (
        ActiveInternalDBIter<'_, FS, Cmp, Policy, Codecs, Pool>,
        ExtraState<'_, Codecs::Decoders>,
    ) {
        let (db_state, per_handle) = self.db.inner();
        let iter = self.iter.with_opts(
            &self.version,
            db_state,
            self.read_opts,
        );
        let activated = ActiveInternalDBIter {
            iter,
            version:      &self.version,
            db_state,
            sampler:      &mut self.sampler,
            sequence_tag: InternalKeyTag::new(self.sequence_number, EntryType::MAX_TYPE),
        };
        let extra_state = ExtraState {
            current:  &mut self.current,
            decoders: &mut per_handle.decoders,
        };
        (activated, extra_state)
    }

    /// Must be called just before dropping or discarding `self`; iterator methods might not work
    /// properly after this is called.
    fn release_resources(&mut self) {
        let (db_state, per_handle) = self.db.inner();

        let key_buf = self.current.take_into_key_buf();
        if key_buf.capacity() <= db_state.opts.iter_buffer_capacity_limit {
            per_handle.iter_key_buf = key_buf;
        }

        let mut mut_state = db_state.lock_mutable_state();

        mut_state.lockfile_refcount -= 1;
    }

    #[inline]
    #[must_use]
    pub const fn valid(&self) -> bool {
        matches!(self.current, MaybeSavedEntry::BackwardsSome(_, _)) || self.iter.valid()
    }

    #[inline]
    #[must_use]
    pub fn current(&self) -> Option<(UserKey<'_>, UserValue<'_>)> {
        match &self.current {
            MaybeSavedEntry::BackwardsSome(key, value) => {
                Some((key.borrow(), value.borrow()))
            }
            MaybeSavedEntry::Buffers(_, _) => {
                let entry = self.iter.current()?;
                // It's an invariant of `self` that the current entry is a `Value` entry.
                Some((entry.user_key(), entry.not_deleted_user_value()))
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn rw_error(
        &self,
        kind: RwErrorKindAlias<FS, Cmp, Codecs>,
    ) -> RwErrorAlias<FS, Cmp, Codecs> {
        RwError {
            db_directory: self.db.db_state().opts.db_directory.clone(),
            kind,
        }
    }

    pub fn into_db(mut self) -> DB<FS, Cmp, Policy, Codecs, Pool> {
        self.release_resources();
        // Note that this is a common way to implement `into_inner` and similar.
        let this = ManuallyDrop::new(self);

        let this_iter            = &raw const this.iter;
        let this_db              = &raw const this.db;
        let this_read_opts       = &raw const this.read_opts;
        let this_sampler         = &raw const this.sampler;
        // Yes, this is `Copy`. Still, makes it easier to reason about this code to drop
        // *every* field other than `this.db`.
        let this_sequence_number = &raw const this.sequence_number;
        let this_version         = &raw const this.version;
        let this_current         = &raw const this.current;

        // SAFETY:
        // For each field `X`, `this.X` is valid for reads because:
        // - it's not a null pointer (since it's inbounds of a Rust allocation)
        // - it's dereferenceable for the type of `this.X`, since it points to a Rust
        //   allocation large enough to store the `this.X` value.
        // - this does not race with any write, since we have exclusive ownership over `self`
        // - we do not interleave accesses with pointers and references
        // It's also properly aligned for the type of `this.X`,
        // since `Self` is not `repr(packed)`.
        // Lastly, it trivially points to a valid value of the type of `this.X`.
        // Additionally, we avoid a double drop by disarming the destructor of `self` in advance.
        // Note that we have to be careful to drop each field once... no typos allowed.
        let _this_iter            = unsafe { this_iter.read() };
        // SAFETY: Same as above.
        let this_db               = unsafe { this_db.read() };
        // SAFETY: Same as above.
        let _this_read_opts       = unsafe { this_read_opts.read() };
        // SAFETY: Same as above.
        let _this_sampler         = unsafe { this_sampler.read() };
        // SAFETY: Same as above.
        let _this_sequence_number = unsafe { this_sequence_number.read() };
        // SAFETY: Same as above.
        let _this_version         = unsafe { this_version.read() };
        // SAFETY: Same as above.
        let _this_current         = unsafe { this_current.read() };

        this_db
    }
}

impl<FS, Cmp, Policy, Codecs, Pool> Drop for InternalDBIter<FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    fn drop(&mut self) {
        self.release_resources();
    }
}

// TODO: Debug impl
pub(crate) struct ActiveInternalDBIter<'a, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// If `valid()`, its `current()` must be at a `Value` entry whose sequence number is
    /// the greatest sequence number less than `self`'s sequence number, among the sequence numbers
    /// of entries for the user key of `current()`.
    iter:         MergingIterWithOpts<'a, FS, Cmp, Policy, Codecs, Pool>,
    version:      &'a Arc<Version>,
    db_state:     &'a InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
    sampler:      &'a mut Option<IterReadSampler>,
    /// The iterator will show what the database's state is/was as of this sequence number.
    ///
    /// Must have [`EntryType::MAX_TYPE`].
    sequence_tag: InternalKeyTag,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> ActiveInternalDBIter<'_, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>>,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    /// Reset the iterator to its initial position.
    pub fn reset(&mut self) {
        self.iter.reset();
    }

    #[inline]
    #[must_use]
    fn rw_error(
        &self,
        kind: RwErrorKindAlias<FS, Cmp, Codecs>,
    ) -> RwErrorAlias<FS, Cmp, Codecs> {
        RwError {
            db_directory: self.db_state.opts.db_directory.clone(),
            kind,
        }
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<FS, Cmp, Policy, Codecs, Pool> ActiveInternalDBIter<'_, FS, Cmp, Policy, Codecs, Pool>
where
    FS:     LevelDBFilesystem,
    Cmp:    LevelDBComparator,
    Policy: FilterPolicy<Eq: CoarserThan<Cmp::Eq>> + FastMirroredClone,
    Codecs: CompressionCodecs,
    Pool:   BufferPool,
{
    fn sample(
        sampler:        &mut Option<IterReadSampler>,
        db_state:       &InternalDBState<FS, Cmp, Policy, Codecs, Pool>,
        decoders:       &mut Codecs::Decoders,
        version:        &Arc<Version>,
        internal_entry: InternalEntry<'_>,
    ) {
        if let Some(some_sampler) = sampler {
            // Guaranteed to not overflow, by invariant of `UserKey`.
            let internal_key_len = usize::from(internal_entry.0.0.len()) + 8;
            let value_len = usize::from(internal_entry.1.0.len());
            let bytes_read = internal_key_len.saturating_add(value_len);
            let continue_sampling = some_sampler.sample(
                db_state,
                decoders,
                version,
                internal_entry.0,
                bytes_read,
            );

            if matches!(continue_sampling, ContinueSampling::False) {
                // The version is no longer the current version, so there's no point in
                // continuing to check if a seek compaction should be triggered on it;
                // no compaction will ever again refer to this old version.
                *sampler = None;
            }
        }
    }

    /// Returns the `key_buf`.
    fn clear_current_entry<'a>(&self, current: &'a mut MaybeSavedEntry) -> &'a mut Vec<u8> {
        let (key_buf, mut value_buf) = match current.take() {
            MaybeSavedEntry::BackwardsSome(key, value)   => (key.into_inner(), value.into_inner()),
            MaybeSavedEntry::Buffers(key_buf, value_buf) => (key_buf, value_buf),
        };

        if value_buf.capacity() > self.db_state.opts.iter_buffer_capacity_limit {
            value_buf = Vec::new();
        }

        current.set_to_buffers(key_buf, value_buf).0
    }

    fn take_cleared_current_entry(&self, current: &mut MaybeSavedEntry) -> (Vec<u8>, Vec<u8>) {
        let (key_buf, mut value_buf) = match current.take() {
            MaybeSavedEntry::BackwardsSome(key, value)   => (key.into_inner(), value.into_inner()),
            MaybeSavedEntry::Buffers(key_buf, value_buf) => (key_buf, value_buf),
        };

        if value_buf.capacity() > self.db_state.opts.iter_buffer_capacity_limit {
            value_buf = Vec::new();
        }

        (key_buf, value_buf)
    }

    /// Scan in the indicated direction until either the end of the iterator or an entry with a
    /// user key different to `current_key` is reached.
    fn scan_to_different_user_key<const NEXT: bool>(
        &mut self,
        decoders:    &mut Codecs::Decoders,
        current_key: OwnedUserKey,
    ) -> (Vec<u8>, RwResult<(), FS, Cmp, Codecs>) {
        let cmp = &self.db_state.opts.cmp;
        loop {
            let next_or_prev = if NEXT {
                self.iter.next(decoders)
            } else {
                self.iter.prev(decoders)
            };

            let next_or_prev = match next_or_prev {
                Ok(Some(next_or_prev)) => next_or_prev.as_internal_entry(),
                Ok(None)  => break,
                Err(kind) => return (current_key.into_inner(), Err(self.rw_error(kind))),
            };

            Self::sample(self.sampler, self.db_state, decoders, self.version, next_or_prev);

            if cmp.cmp_user(current_key.borrow(), next_or_prev.user_key()).is_ne() {
                break;
            }
        }

        (current_key.into_inner(), Ok(()))
    }

    /// Advance to the next non-deleted value with a LE sequence number, starting at wherever
    /// `self.iter.current()` is.
    fn inner_next(
        &mut self,
        decoders: &mut Codecs::Decoders,
        key_buf:  &mut Vec<u8>,
    ) -> RwResult<(), FS, Cmp, Codecs> {
        loop {
            // Scan to the next entry with a LE sequence number.
            let Some(next) = self.iter.current() else {
                return Ok(());
            };
            let next = next.as_internal_entry();
            Self::sample(self.sampler, self.db_state, decoders, self.version, next);

            // Since `self.sequence_tag` has the greatest possible entry type, and since sequence
            // numbers are the more significant bits, the only way for this inequality to hold
            // is for `next` to have a higher-than-desired sequence number.
            // Therefore, keep looking.
            if next.0.1.raw_inner() > self.sequence_tag.raw_inner() {
                match self.iter.next(decoders) {
                    // Return to scanning to the next entry with a LE sequence number.
                    Ok(Some(_)) => continue,
                    Ok(None)    => return Ok(()),
                    Err(kind)   => return Err(self.rw_error(kind)),
                }
            }

            match next.0.1.entry_type() {
                EntryType::Deletion => {
                    let current_key = next.user_key().to_owned_with_buf(mem::take(key_buf));
                    // This key is deleted. Scan to the next user key.
                    let (buf, result) = self.scan_to_different_user_key::<true>(
                        decoders,
                        current_key,
                    );
                    *key_buf = buf;
                    result?;
                }
                EntryType::Value => {
                    // `next` contains a Value entry with a LE sequence number, of a user key
                    // following that of the previous `self.current()` entry, and even if the
                    // value of the user key has since been updated or deleted, this is the
                    // current value as of the sequence number of `self.sequence_tag`.
                    return Ok(());
                }
            }
        }
    }

    /// Return the previous non-deleted value with the greatest LE sequence number for the current
    /// user key, starting at wherever `self.iter.current()` is (that is, `self.iter.current()`
    /// is the first candidate for the previous entry).
    fn inner_prev<'a>(
        &mut self,
        mut key_buf:   Vec<u8>,
        mut value_buf: Vec<u8>,
        current:       &'a mut MaybeSavedEntry,
        decoders:      &mut Codecs::Decoders,
    ) -> RwResult<Option<(UserKey<'a>, UserValue<'a>)>, FS, Cmp, Codecs> {
        let cmp = &self.db_state.opts.cmp;

        loop {
            let Some(prev_entry) = self.iter.current() else {
                *current = MaybeSavedEntry::Buffers(key_buf, value_buf);
                return Ok(None);
            };
            let prev_entry = prev_entry.as_internal_entry();

            Self::sample(self.sampler, self.db_state, decoders, self.version, prev_entry);

            let current_key = prev_entry.user_key().to_owned_with_buf(key_buf);

            // Since `self.sequence_tag` has the greatest possible entry type, and since sequence
            // numbers are the more significant bits, the only way for this inequality to hold
            // is for `prev_key` to have a higher-than-desired sequence number.
            if prev_entry.0.1.raw_inner() > self.sequence_tag.raw_inner() {
                // Every preceding entry with this user key will have sequence numbers that are
                // too high. Go to the preceding user key.
                match self.scan_to_different_user_key::<false>(decoders, current_key) {
                    (buf, Ok(()))   => key_buf = buf,
                    (buf, Err(err)) => {
                        *current = MaybeSavedEntry::Buffers(buf, value_buf);
                        return Err(err);
                    }
                }
                continue;
            }

            // This is a candidate for the previous entry; we will end up returning something
            // for this user key, unless the entry is deleted or an error occurs.
            // Scan `self.iter` to the preceding entry  which either has a different (lower) user
            // key, has a greater sequence number, or is `None`, setting `key` and `value` at the
            // semantically current entry.
            let mut current_value = MaybeSavedValue::new(prev_entry, value_buf);

            loop {
                let maybe_prev = match self.iter.prev(decoders) {
                    Ok(Some(maybe_prev)) => maybe_prev.as_internal_entry(),
                    Ok(None) => break,
                    Err(kind) => {
                        key_buf = current_key.into_inner();
                        value_buf = current_value.into_buf();

                        *current = MaybeSavedEntry::Buffers(key_buf, value_buf);
                        return Err(self.rw_error(kind));
                    }
                };

                Self::sample(self.sampler, self.db_state, decoders, self.version, maybe_prev);

                // As elsewhere, this condition implies that the sequence number of
                // `maybe_prev` exceeds the snapshot sequence number.
                if maybe_prev.0.1.raw_inner() > self.sequence_tag.raw_inner()
                    || cmp.cmp_user(maybe_prev.user_key(), current_key.borrow()).is_ne()
                {
                    break;
                }

                value_buf = current_value.into_buf();

                // Else, continue.
                current_value = MaybeSavedValue::new(maybe_prev, value_buf);
            }

            match current_value {
                MaybeSavedValue::Value(current_value) => {
                    // `current_key`, `current_value` contains a `Value` entry with a LE sequence
                    // number, of a user key preceding that of the former `self.current()` entry,
                    // and even if the value of the user key has since been updated or deleted,
                    // this is the current value as of the sequence number of `self.sequence_tag`.
                    //
                    // Basically `Ok(Some(_))`.
                    return Ok(Some(current.set_to_entry(current_key, current_value)));
                }
                MaybeSavedValue::Buffer(v_buf) => {
                    value_buf = v_buf;
                    // This key is deleted. Scan to the preceding user key.
                    match self.scan_to_different_user_key::<false>(decoders, current_key) {
                        (buf, Ok(()))   => key_buf = buf,
                        (buf, Err(err)) => {
                            *current = MaybeSavedEntry::Buffers(buf, value_buf);
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    /// Advance to the next key. Due to `borrowck` issues, you should then call `current()`
    /// separately.
    #[expect(clippy::needless_pass_by_value, reason = "extra_state is 2 references")]
    pub fn next(
        &mut self,
        extra_state: ExtraState<'_, Codecs::Decoders>,
    ) -> RwResult<(), FS, Cmp, Codecs> {
        let key_buf = match extra_state.current.take() {
            MaybeSavedEntry::BackwardsSome(current_key, current_value) => {
                // `self.iter` should be one position before `current_key`, though since entries
                // could be added anywhere into the memtable at any time, intervening entries may
                // have been added. Scan forward until something matching `current_key` is seen,
                // then scan forwards to a different user key.
                let mut value_buf = current_value.into_inner();
                if value_buf.capacity() > self.db_state.opts.iter_buffer_capacity_limit {
                    value_buf = Vec::new();
                }

                let mut result;
                let cmp = &self.db_state.opts.cmp;

                loop {
                    result = self.iter.next(extra_state.decoders);

                    if let Ok(Some(entry)) = &result {
                        if cmp.cmp_user(entry.user_key(), current_key.borrow()).is_lt() {
                            continue;
                        }
                    }

                    break;
                };

                let key_buf = extra_state.current
                    .set_to_buffers(current_key.into_inner(), value_buf)
                    .0;

                match result {
                    Ok(Some(_)) => {}
                    Ok(None)    => return Ok(()),
                    Err(kind)   => return Err(self.rw_error(kind)),
                }

                key_buf
            }
            MaybeSavedEntry::Buffers(key_buf, value_buf) => {
                extra_state.current.set_to_buffers(key_buf, value_buf).0
            }
        };

        if let Some(current_entry) = self.iter.current() {
            // Seek forwards to a different user key.
            let current_key = current_entry.user_key().to_owned_with_buf(mem::take(key_buf));
            let (buf, result) = self.scan_to_different_user_key::<true>(
                extra_state.decoders,
                current_key,
            );
            *key_buf = buf;
            result?;
        } else {
            // `next` is enough to get to a different user key (unless the iter is empty).
            self.iter
                .next(extra_state.decoders)
                .map_err(|kind| RwError {
                    db_directory: self.db_state.opts.db_directory.clone(),
                    kind,
                })?;
        }

        // Once we get here, we need to get the next non-deleted value with a LE sequence number.
        self.inner_next(extra_state.decoders, key_buf)
    }

    /// # Speed Warning
    /// Backwards iteration is much slower than forwards iteration.
    #[expect(clippy::needless_pass_by_value, reason = "extra_state is 2 references")]
    pub fn prev<'a>(
        &mut self,
        extra_state: ExtraState<'a, Codecs::Decoders>,
    ) -> RwResult<Option<(UserKey<'a>, UserValue<'a>)>, FS, Cmp, Codecs> {
        let (key_buf, value_buf) = match extra_state.current.take() {
            MaybeSavedEntry::BackwardsSome(key, value) => {
                (key.into_inner(), value.into_inner())
            }
            MaybeSavedEntry::Buffers(mut key_buf, value_buf) => {
                // `self.iter` is pointing at the semantically current entry (if any);
                // we need it to point at the previous internal entry.
                let error = if let Some(current_entry) = self.iter.current() {
                    let current_key = current_entry.user_key().to_owned_with_buf(key_buf);
                    let (buf, result) = self.scan_to_different_user_key::<false>(
                        extra_state.decoders,
                        current_key,
                    );
                    key_buf = buf;
                    result.err()
                } else {
                    self.iter
                        .prev(extra_state.decoders)
                        .err()
                        .map(|kind| self.rw_error(kind))
                };

                if let Some(error) = error {
                    *extra_state.current = MaybeSavedEntry::Buffers(key_buf, value_buf);
                    return Err(error);
                }
                (key_buf, value_buf)
            }
        };

        // Get the previous non-deleted value with a LE sequence number.
        self.inner_prev(key_buf, value_buf, extra_state.current, extra_state.decoders)
    }

    #[expect(clippy::needless_pass_by_value, reason = "extra_state is 2 references")]
    pub fn seek(
        &mut self,
        extra_state: ExtraState<'_, Codecs::Decoders>,
        lower_bound: UserKey<'_>,
    ) -> RwResult<(), FS, Cmp, Codecs> {
        let key_buf = self.clear_current_entry(extra_state.current);

        self.iter
            .seek(extra_state.decoders, InternalKey(lower_bound, self.sequence_tag))
            .map_err(|kind| self.rw_error(kind))?;

        // Get the next non-deleted value with a LE sequence number.
        self.inner_next(extra_state.decoders, key_buf)?;
        Ok(())
    }

    /// # Speed Warning
    /// Backwards iteration is much slower than forwards iteration.
    #[expect(clippy::needless_pass_by_value, reason = "extra_state is 2 references")]
    pub fn seek_before(
        &mut self,
        extra_state:        ExtraState<'_, Codecs::Decoders>,
        strict_upper_bound: UserKey<'_>,
    ) -> RwResult<(), FS, Cmp, Codecs> {
        let (key_buf, value_buf) = self.take_cleared_current_entry(extra_state.current);

        self.iter
            .seek_before(extra_state.decoders, InternalKey(strict_upper_bound, self.sequence_tag))
            .map_err(|kind| self.rw_error(kind))?;

        // Get the previous non-deleted value with a LE sequence number.
        self.inner_prev(key_buf, value_buf, extra_state.current, extra_state.decoders)?;
        Ok(())
    }

    #[expect(clippy::needless_pass_by_value, reason = "extra_state is 2 references")]
    pub fn seek_to_first(
        &mut self,
        extra_state: ExtraState<'_, Codecs::Decoders>,
    ) -> RwResult<(), FS, Cmp, Codecs> {
        let key_buf = self.clear_current_entry(extra_state.current);

        self.iter
            .seek_to_first(extra_state.decoders)
            .map_err(|kind| self.rw_error(kind))?;

        // Get the next non-deleted value with a LE sequence number.
        self.inner_next(extra_state.decoders, key_buf)?;
        Ok(())
    }

    /// # Speed Warning
    /// Backwards iteration is much slower than forwards iteration.
    #[expect(clippy::needless_pass_by_value, reason = "extra_state is 2 references")]
    pub fn seek_to_last(
        &mut self,
        extra_state: ExtraState<'_, Codecs::Decoders>,
    ) -> RwResult<(), FS, Cmp, Codecs> {
        let (key_buf, value_buf) = self.take_cleared_current_entry(extra_state.current);

        self.iter
            .seek_to_last(extra_state.decoders)
            .map_err(|kind| self.rw_error(kind))?;

        // Get the previous non-deleted value with a LE sequence number.
        self.inner_prev(key_buf, value_buf, extra_state.current, extra_state.decoders)?;
        Ok(())
    }
}

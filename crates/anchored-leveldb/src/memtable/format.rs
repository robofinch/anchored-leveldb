use std::{ptr, slice};
use std::{marker::PhantomData, mem::MaybeUninit, num::NonZeroUsize};

use variance_family::{CovariantFamily, MaxUpperBound, Varying, WithLifetime, covariant};

use anchored_skiplist::{
    EncodeWith, Skiplist, SkiplistFormat, SkiplistIter, SkiplistLendingIter, SkiplistReader,
    UniqueSkiplist,
};

use crate::{
    pub_traits::cmp_and_policy::LevelDBComparator,
    pub_typed_bytes::ShortSlice,
    table_format::InternalComparator,
};
use crate::typed_bytes::{
    EncodedInternalEntry, EncodedInternalKey, InternalEntry, InternalKey, MaybeUserValue,
};


pub(super) type MemtableUniqueSkiplist<Cmp> = UniqueSkiplist<
    MemtableFormat<Cmp>,
    MaxUpperBound,
    InternalComparator<Cmp>,
>;

pub(super) type MemtableSkiplist<Cmp> = Skiplist<
    MemtableFormat<Cmp>,
    MaxUpperBound,
    InternalComparator<Cmp>,
>;

pub(super) type MemtableSkiplistReader<Cmp> = SkiplistReader<
    MemtableFormat<Cmp>,
    MaxUpperBound,
    InternalComparator<Cmp>,
>;

pub(super) type MemtableSkiplistIter<'a, Cmp> = SkiplistIter<
    'a,
    MemtableFormat<Cmp>,
    MaxUpperBound,
    InternalComparator<Cmp>,
>;

pub(super) type MemtableSkiplistLendingIter<Cmp> = SkiplistLendingIter<
    MemtableFormat<Cmp>,
    MaxUpperBound,
    InternalComparator<Cmp>,
>;

#[derive(Debug, Clone, Copy)]
pub(super) struct VaryingEncodedInternalEntry;

covariant! {
    impl<'varying> CovariantFamily<'_, _>
    // SAFETY: `VaryingEncodedInternalEntry` is defined in this crate.
    for #[unsafe(not_a_foreign_fundamental_type)] VaryingEncodedInternalEntry
    as EncodedInternalEntry<'varying>
}

#[derive(Debug, Clone, Copy)]
pub(super) struct VaryingInternalKey;

covariant! {
    impl<'varying> CovariantFamily<'_, _>
    // SAFETY: `VaryingInternalKey` is defined in this crate.
    for #[unsafe(not_a_foreign_fundamental_type)] VaryingInternalKey
    as InternalKey<'varying>
}

/// The format of a memtable entry is as follows:
///
/// - 1 byte indicating the length of key and value lengths.
///   - The lower nibble is the key length (`key_len_len`),
///     and its value must be between 0 and 4.
///   - The upper nibble is the value length (`value_len_len`),
///     and its value must be between 0 and 4.
/// - `key_len_len`-many bytes indicating the user key length.
///   - It consists of the `key_len_len`-many least significant bytes of the `u32` user key length
///     (`key_len`) stored in little-endian order. Its value must be at most `u32::MAX - 8`
///     or `usize::MAX`, whichever is smaller. (Note: the lengths of user keys in table files
///     could exceed `usize::MAX`. However, anything inserted into the memtable necessarily
///     fits in a slice, and therefore has length at most `usize::MAX`.)
/// - `key_len`-many bytes comprising the user key.
///   - The user key value (`user_key`) should be comparable by the user comparator.
///     (This assumption is *not* a safety guarantee, it just causes a panic if the user inserted
///     a bad key into the database.)
/// - 8 bytes comprising an internal key tag.
///   - The tag (`key_tag`) is stored as a `u64` in little-endian order. It *must* be valid; that
///     is, its least-significant byte must be either 0 or 1 (such that the least-significant byte
///     is a valid `EntryType`, and such that the `u64` is a valid `InternalKeyTag`).
/// - `value_len_len`-many bytes indicating the value length.
///  - It consists of the `value_len_len`-many least significant bytes of the `u32` value length
///    (`value_len`) stored in little-endian order. Its value must be at most `u32::MAX`
///    or `usize::MAX`, whichever is smaller. (Note: the lengths of values in table files
///    could exceed `usize::MAX`. However, anything inserted into the memtable necessarily
///    fits in a slice, and therefore has length at most `usize::MAX`.)
/// - `value_len`-many bytes comprising the value (`maybe_user_value`).
pub(super) struct MemtableFormat<Cmp>(PhantomData<fn() -> Cmp>);

#[expect(
    unsafe_code,
    clippy::undocumented_unsafe_blocks,
    reason = "TODO: justify all this `unsafe`",
)]
#[inline]
#[must_use]
unsafe fn decode_key<'a>(data: *const u8) -> (EncodedInternalKey<'a>, usize, *const u8) {
    let len_lens = unsafe { ptr::read(data) };
    let key_len_len = usize::from(len_lens & 0b1111_u8);
    let value_len_len = usize::from(len_lens >> 4_u8);

    let mut key_len = [0_u8; 4];
    let key_len_data = unsafe { data.add(1) };
    let key_len_dst = key_len.as_mut_ptr();
    unsafe {
        ptr::copy_nonoverlapping(key_len_data, key_len_dst, key_len_len);
    };

    let key_len = usize::try_from(u32::from_le_bytes(key_len));
    let key_len = unsafe { key_len.unwrap_unchecked() };
    let key_and_tag_len = unsafe { key_len.unchecked_add(8) };

    let key_data = unsafe { key_len_data.add(key_len_len) };
    let internal_key = unsafe { slice::from_raw_parts(key_data, key_and_tag_len) };
    let internal_key = EncodedInternalKey::new_unchecked(internal_key);

    let key_tag_data = unsafe { key_data.add(key_len) };

    (internal_key, value_len_len, key_tag_data)
}

#[expect(
    unsafe_code,
    clippy::undocumented_unsafe_blocks,
    reason = "TODO: justify all this `unsafe`",
)]
#[inline]
#[must_use]
unsafe fn decode_entry<'a>(data: *const u8) -> EncodedInternalEntry<'a> {
    let (internal_key, value_len_len, key_tag_data) = unsafe { decode_key(data) };

    let mut value_len = [0_u8; 4];
    let value_len_data = unsafe { key_tag_data.add(size_of::<u64>()) };
    let value_len_dst = value_len.as_mut_ptr();
    unsafe {
        ptr::copy_nonoverlapping(value_len_data, value_len_dst, value_len_len);
    };

    let value_len = usize::try_from(u32::from_le_bytes(value_len));
    let value_len = unsafe { value_len.unwrap_unchecked() };

    let value_data = unsafe { value_len_data.add(value_len_len) };
    let maybe_user_value = unsafe { slice::from_raw_parts(value_data, value_len) };
    let maybe_user_value = unsafe { ShortSlice::new(maybe_user_value).unwrap_unchecked() };
    let maybe_user_value = MaybeUserValue(maybe_user_value);

    EncodedInternalEntry(internal_key, maybe_user_value)
}

#[expect(
    unsafe_code,
    clippy::undocumented_unsafe_blocks,
    reason = "TODO: justify all this `unsafe`",
)]
impl<Cmp: LevelDBComparator> SkiplistFormat for MemtableFormat<Cmp> {
    type Entry = VaryingEncodedInternalEntry;
    type Key = VaryingInternalKey;
    type Cmp = InternalComparator<Cmp>;

    #[allow(clippy::unwrap_used, reason = "checked at comptime")]
    const ENTRY_ALIGN: NonZeroUsize = const { NonZeroUsize::new(1).unwrap() };

    #[inline]
    unsafe fn decode_entry<'a>(data: *const u8) -> EncodedInternalEntry<'a> {
        unsafe { decode_entry(data) }
    }

    #[inline]
    unsafe fn decode_key<'a>(data: *const u8) -> InternalKey<'a> {
        unsafe { decode_key(data).0.as_internal_key() }
    }
}

pub(super) struct MemtableEntryEncoder<'a> {
    /// # Safety invariant
    /// This `total_len` value must be equal to the total encoded length of this entry. That is,
    /// it must be equal to the following (without overflow):
    /// ```ignore
    /// 1 + self.user_key_len_len + self.internal_key.0.inner().len() + 8
    ///     + self.value_len_len + self.value.inner().len()
    /// ```
    total_len:        usize,
    user_key_len_len: u8,
    value_len_len:    u8,
    /// # Safety invariant
    /// The `UserKey<'_>` contained in `internal_key` must have length at most `u32::MAX - 8`.
    internal_key:     InternalKey<'a>,
    /// # Safety invariant
    /// The `value` must have length at most `u32::MAX`.
    value:            MaybeUserValue<'a>,
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<'a> MemtableEntryEncoder<'a> {
    /// # Panics
    /// May panic if the user key and user value's lengths sum to `usize::MAX - 17` or more.
    ///
    /// There is an *exceedingly* narrow window for that to occur, given that each slice
    /// has length at most `isize::MAX`.
    ///
    /// This panic warning is *not* propagated all the way up to the end user, since at that point
    /// it'd be more sane to declare every possible OOM error in this library.
    pub fn new(entry: InternalEntry<'a>) -> Self {
        /// Use checked operations everywhere here to clearly *require* that no underflow
        /// or overflow occurs, lest the `unsafe` code in this module be buggy.
        fn inner_new(entry: InternalEntry<'_>) -> Option<(usize, u8, u8)> {
            let four = u8::try_from(size_of::<u32>()).ok()?;

            let key_len = entry.0.0.inner().len();
            let value_len = entry.1.0.inner().len();

            // The "leading" bytes are the most-significant bytes.
            let key_len_leading_zero_bytes = key_len.leading_zeros().checked_div(8)?;
            let key_len_leading_zero_bytes = u8::try_from(key_len_leading_zero_bytes).ok()?;
            let key_len_len = four.checked_sub(key_len_leading_zero_bytes)?;

            let value_len_leading_zero_bytes = value_len.leading_zeros().checked_div(8)?;
            let value_len_leading_zero_bytes = u8::try_from(value_len_leading_zero_bytes).ok()?;
            let value_len_len = four.checked_sub(value_len_leading_zero_bytes)?;

            #[expect(
                clippy::expect_used,
                reason = "see function docs; this panic is absurdly hard to trigger",
            )]
            let total_len = 1_usize
                .checked_add(usize::from(key_len_len))?
                .checked_add(key_len)?
                .checked_add(8)?
                .checked_add(usize::from(value_len_len))?
                // This is the one call where overflow could happen normally
                .checked_add(value_len).expect("length of internal entry exceeds `usize::MAX`");

            Some((total_len, key_len_len, value_len_len))
        }

        #[expect(
            clippy::expect_used,
            reason = "other than the one place where overflow can happen, none of the math should \
                      overflow or underflow. `unsafe` code depends on this, so we might \
                      as well make it impossible for a bug there to trigger UB",
        )]
        let (total_len, user_key_len_len, value_len_len) = inner_new(entry)
            .expect("bug: incorrect math in MemtableEntryEncoder");

        Self {
            total_len,
            user_key_len_len,
            value_len_len,
            internal_key: entry.0,
            value:        entry.1,
        }
    }

    #[expect(
        unsafe_code,
        clippy::undocumented_unsafe_blocks,
        reason = "TODO: justify all this `unsafe`",
    )]
    unsafe fn encode_entry(self, data: *mut u8) {
        let len_lens = self.user_key_len_len | (self.value_len_len << 4_u8);
        unsafe {
            ptr::write(data, len_lens);
        };

        let key_len_len = usize::from(self.user_key_len_len);
        let value_len_len = usize::from(self.value_len_len);

        let key_len_data = unsafe { data.add(1) };
        let key_len_usize = self.internal_key.0.inner().len();
        let key_len = unsafe { u32::try_from(key_len_usize).unwrap_unchecked() };
        let key_len = key_len.to_le_bytes();
        let key_len_src = key_len.as_ptr();
        unsafe {
            ptr::copy_nonoverlapping(key_len_src, key_len_data, key_len_len);
        };

        let key_data = unsafe { key_len_data.add(key_len_len) };
        let key_src = self.internal_key.0.inner().as_ptr();
        unsafe {
            ptr::copy_nonoverlapping(key_src, key_data, key_len_usize);
        };

        let key_tag_data = unsafe { key_data.add(key_len_usize) };
        let key_tag = self.internal_key.1.raw_inner().to_le_bytes();
        unsafe {
            ptr::write(key_tag_data.cast::<[u8; 8]>(), key_tag);
        };

        let value_len_data = unsafe { key_tag_data.add(size_of::<u64>()) };
        let value_len_usize = self.value.0.inner().len();
        let value_len = unsafe { u32::try_from(value_len_usize).unwrap_unchecked() };
        let value_len = value_len.to_le_bytes();
        let value_len_src = value_len.as_ptr();
        unsafe {
            ptr::copy_nonoverlapping(value_len_src, value_len_data, value_len_len);
        };

        let value_data = unsafe { value_len_data.add(value_len_len) };
        let value_src = self.value.0.inner().as_ptr();
        unsafe {
            ptr::copy_nonoverlapping(value_src, value_data, value_len_usize);
        };
    }
}

#[expect(
    unsafe_code,
    clippy::undocumented_unsafe_blocks,
    reason = "TODO: justify all this `unsafe`",
)]
unsafe impl<Cmp: LevelDBComparator> EncodeWith<MemtableEntryEncoder<'_>> for MemtableFormat<Cmp> {
    fn entry_size(encoder: &MemtableEntryEncoder<'_>) -> usize {
        encoder.total_len
    }

    unsafe fn encode_entry(encoder: MemtableEntryEncoder<'_>, data: &mut [MaybeUninit<u8>]) {
        let data = data.as_mut_ptr().cast();
        unsafe {
            encoder.encode_entry(data);
        };
    }
}

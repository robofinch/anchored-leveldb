#![expect(unsafe_code, reason = "work with type-erased data storage")]

use core::{cmp::Ordering, mem::MaybeUninit, num::NonZeroUsize};

use variance_family::{LifetimeFamily, MaxUpperBound, UpperBound, Varying};


/// The `Entry` type of a [`SkiplistFormat`].
pub type Entry<'a, F, Upper = MaxUpperBound>
    = Varying<'a, 'a, Upper, <F as SkiplistFormat<Upper>>::Entry>;

/// The `Key` type of a [`SkiplistFormat`].
pub type Key<'a, F, Upper = MaxUpperBound>
    = Varying<'a, 'a, Upper, <F as SkiplistFormat<Upper>>::Key>;

/// Define the entry format and sorting order of a skiplist.
///
/// This trait is safe, since the sole requirement placed on implementors is that the implementation
/// is sound.
///
/// # Notes for Implementors
/// ## Overall intent
/// A usual format implementation will put owned data into the skiplist with a trivial destructor,
/// such as plain-old-data types, while [`Self::Entry`] and [`Self::Key`] borrow from that data
/// (and perhaps contain copies of a small amount of the data).
///
/// Note that destructors will never be run on values inserted into a skiplist, thus the
/// recommendation for trivial destructors.
///
/// ## Format details
/// Implementing this trait defines the format of raw entry data, including any layout and validity
/// requirements. In particular, the raw entry data is allowed to contain [uninit] data, pointer
/// [provenance], and come with alignment and length requirements. The backing storage of the data
/// can in general be assumed to be an aligned `[MaybeUninit<u8>]`, capable of storing any possible
/// byte pattern \(unless this implementation of `SkiplistFormat` explicitly guarantees weaker
/// constrains on the backing data\).
///
/// Technically, it is even perfectly sound to implement `SkiplistFormat` with uninhabited
/// [`Self::Entry`] and [`Self::Key`] types, in which case any sound implementations of
/// [`EncodeWith`] for the format must also be uninhabited, implying that a `data` value meeting the
/// safety precondition of [`decode_entry`] or [`decode_key`] can never be obtained.
///
/// Custom data formats with dynamic sizes are possible. Stable Rust only supports DSTs with a
/// single unsized field (as the last non-ZST field), while skiplist formats can use multiple
/// unsized fields. However, a skiplist format must be self-describing given only a pointer to the
/// start of the raw entry data; the length of unsized fields must be encoded into the raw entry
/// data in some way.
///
/// ## `Sync`-ness
/// Do not needlessly put non-[`Sync`] data in the entry or key types; if [`Entry<'a, Self, _>`] or
/// [`Key<'a, Self, _>`] is `!Sync` for some lifetime `'a`, then concurrent reads to the raw data
/// (with `decode_entry` or `decode_key`, for any lifetimes) are not permitted.
///
/// ## Aliasing
/// Shared aliasing rules apply to the raw data passed to [`decode_entry`] and [`decode_key`]. In
/// particular, data should not be mutated except via internal mutability (that is, [`UnsafeCell`]).
///
/// [`decode_entry`]: SkiplistFormat::decode_entry
/// [`decode_key`]: SkiplistFormat::decode_key
/// [uninit]: core::mem::MaybeUninit
/// [provenance]: core::ptr#provenance
/// [`UnsafeCell`]: core::cell::UnsafeCell
pub trait SkiplistFormat<Upper: UpperBound = MaxUpperBound> {
    /// The type of entries that can be read from a skiplist.
    type Entry: for<'lower> LifetimeFamily<'lower, Upper, Is: Sized>;
    /// The type of keys used to sort or search for entries in a skiplist.
    ///
    /// The key type should be cheaply cloneable; keys may be frequently cloned. Additionally,
    /// they will generally be passed by value even when not strictly necessary to do so.
    type Key:   for<'lower> LifetimeFamily<'lower, Upper, Is: Clone>;
    type Cmp:   for<'a, 'b> Comparator<Key<'a, Self, Upper>, Key<'b, Self, Upper>>;

    /// The alignment of raw entry data.
    ///
    /// This must be a power of two. (Otherwise, all attempted insertions into the skiplist will
    /// fail.)
    ///
    /// Used when inserting an entry into a skiplist.
    const ENTRY_ALIGN: NonZeroUsize;

    /// Decode raw data into a [`Self::Entry`] value.
    ///
    /// Used when reading an entry from a skiplist.
    ///
    /// # Safety
    /// - Where `Self` is this implementation of `SkiplistFormat`, `data` must be the pointer of a
    ///   slice written by <code><Self as [EncodeWith]\<E>>::[encode_entry]</code> for some `E`.
    /// - Where `data_len` is the length of the slice passed to `encode_entry` to write `data`,
    ///   the slice with pointer `data` and length `data_len` must not be accessed except by
    ///   [`decode_entry`] and [`decode_key`] during at least lifetime `'a`. Note in particular
    ///   that deallocation is considered an access.
    /// - Concurrent reads to `data` (with `decode_entry` or `decode_key`) are not permitted
    ///   unless [`Entry<'a, Self, _>`] and [`Key<'a, Self, _>`] implement [`Sync`] for all
    ///   lifetimes possible for this format (that is, all `'a` at most as long as the `Upper`
    ///   bound of this format).
    ///
    /// [encode_entry]: EncodeWith::encode_entry
    /// [`decode_entry`]: Self::decode_entry
    /// [`decode_key`]: Self::decode_key
    #[must_use]
    unsafe fn decode_entry<'a>(data: *const u8) -> Entry<'a, Self, Upper>;

    /// Decode raw data into a [`Self::Key`] value.
    ///
    /// Used when sorting or searching for entries in a skiplist; this function should be fast,
    /// since it is called frequently.
    ///
    /// # Safety
    /// - Where `Self` is this implementation of `SkiplistFormat`, `data` must be the pointer of a
    ///   slice written by <code><Self as [EncodeWith]\<E>>::[encode_entry]</code> for some `E`.
    /// - Where `data_len` is the length of the slice passed to `encode_entry` to write `data`,
    ///   the slice with pointer `data` and length `data_len` must not be accessed except by
    ///   [`decode_entry`] and [`decode_key`] during at least lifetime `'a`. Note in particular
    ///   that deallocation is considered an access.
    /// - Concurrent reads to `data` (with `decode_entry` or `decode_key`) are not permitted
    ///   unless [`Entry<'a, Self, _>`] and [`Key<'a, Self, _>`] implement [`Sync`] for all
    ///   lifetimes possible for this format (that is, all `'a` at most as long as the `Upper`
    ///   bound of this format).
    ///
    /// [encode_entry]: EncodeWith::encode_entry
    /// [`decode_entry`]: Self::decode_entry
    /// [`decode_key`]: Self::decode_key
    #[must_use]
    unsafe fn decode_key<'a>(data: *const u8) -> Key<'a, Self, Upper>;
}

/// Encode data into the raw entry format used by a skiplist.
///
/// This trait is used for skiplist insertions.
///
/// # Safety
/// The implementation of [`Self::encode_entry`] must be compatible with the raw entry format used
/// by [`Self::decode_entry`] and [`Self::decode_key`].
///
/// [`Self::decode_entry`]: SkiplistFormat::decode_entry
/// [`Self::decode_key`]: SkiplistFormat::decode_key
pub unsafe trait EncodeWith<Encoder: ?Sized, U: UpperBound>: SkiplistFormat<U> {
    /// The size (in bytes) of the entry which will be written by this encoder.
    ///
    /// The size is *not* required to be a multiple of [`Self::ENTRY_ALIGN`].
    ///
    /// Used when inserting an entry into a skiplist.
    ///
    /// [`Self::ENTRY_ALIGN`]: SkiplistFormat::ENTRY_ALIGN
    #[must_use]
    fn entry_size(encoder: &Encoder) -> usize;

    /// Write an entry to a type-erased target format.
    ///
    /// # Safety
    /// The length of `data` must be equal to `Self::entry_size(&encoder)`.
    ///
    /// (More precisely, `encoder` should not be accessed between that call to
    /// `Self::entry_size(&encoder)` and this call to `Self::encode_entry(encoder)`, except
    /// for moving the `encoder`.)
    unsafe fn encode_entry(encoder: Encoder, data: &mut [MaybeUninit<u8>]);
}

/// A comparator should provide a total order across all values of any types it can compare.
/// Any clones of a comparator should behave identically to the source comparator.
///
/// This is essentially a generalization of [`Ord`].
///
/// Note that none of the axioms that define a total order require that two elements which compare
/// as equal are "truly" equal in some more fundamental sense; that is, keys which are distinct
/// (perhaps according to an [`Eq`] or [`PartialEq`] implementation) may compare as equal in the
/// comparator's total order (and corresponding equivalence relation).
///
/// Unsafe code is not allowed to rely on the correctness of implementations; that is, an incorrect
/// `Comparator` implementation may cause severe logic errors, but must not cause memory unsafety.
pub trait Comparator<Lhs, Rhs> {
    /// This method returns the [`Ordering`] between `lhs` and `rhs` in the total order provided
    /// by the `self` comparator.
    ///
    /// By convention, `self.cmp(lhs, rhs)` returns the ordering matching the expression
    /// `lhs <operator> rhs` if true (under the total order provided by `self`).
    ///
    /// This method is akin to [`Ord::cmp`].
    #[must_use]
    fn cmp(&self, lhs: Lhs, rhs: Rhs) -> Ordering;
}

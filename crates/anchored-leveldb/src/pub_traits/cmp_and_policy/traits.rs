use std::cmp::Ordering;


/// Indicates that this type represents an
/// [equivalence relation](https://en.wikipedia.org/wiki/Equivalence_relation)
/// used by a [`LevelDBComparator`] or [`FilterPolicy`].
///
/// Remember to implement [`CoarserThan<Self>`] when implementing this trait.
///
/// [`CoarserThan<Self>`]: CoarserThan
pub trait EquivalenceRelation {}

/// Indicates that this [`EquivalenceRelation`] is
/// [coarser than](https://en.wikipedia.org/wiki/Equivalence_relation#Comparing_equivalence_relations)
/// the `Other` equivalence relation.
///
/// Essentially, whenever the `Other` equivalence relation says that two things are equal, the
/// `Self` equivalence relation must also say that they are equal.
pub trait CoarserThan<Other: ?Sized + EquivalenceRelation>: EquivalenceRelation {}

/// Trait for comparing two byte slices in a LevelDB database. In addition to the comparison
/// function, several operations needed by LevelDB are included.
///
/// See [`BytewiseComparator`] for a good default implementation which uses byte slices'
/// [`Ord`] implementation.
///
/// Note that unsafe code is *not* allowed to rely on the correctness of an arbitrary
/// implementation of this trait; that is, an incorrect implementation may cause severe logic
/// errors, but must not cause memory unsafety.
///
/// [`BytewiseComparator`]: super::implementors::BytewiseComparator
pub trait LevelDBComparator {
    /// The [equivalence relation](https://en.wikipedia.org/wiki/Equivalence_relation)
    /// corresponding to the total order on byte slices provided by [`Self::cmp`].
    type Eq: EquivalenceRelation;
    /// The error returned if a key read from a LevelDB database is corrupt to the extent that
    /// it cannot be sensibly compared by [`Self::cmp`].
    type InvalidKeyError;

    /// The name identifying the comparator's behavior.
    ///
    /// The name should usually be a valid `&'static str`, but is not strictly required to be UTF-8.
    ///
    /// When opening a LevelDB database, it is checked that the database's comparator name matches
    /// the name of the comparator used to open the database. Try to make the name somewhat
    /// unique, to help catch mistakes.
    ///
    /// # Downstream Panics
    /// If the length of the name exceeds [`u32::MAX`], panics may occur.
    #[must_use]
    fn name(&self) -> &'static [u8];

    /// Compare two byte slices in a total order.
    ///
    /// This method is analogous to [`Ord::cmp`]; in fact, [`BytewiseComparator`] uses `Ord`.
    ///
    /// Note that none of the axioms that define a total order require that two elements which
    /// compare as equal are "*truly*" equal in some more fundamental sense; that is, keys which
    /// are distinct (perhaps according to the [`Eq`] implementation of `[u8]`) may compare as
    /// equal in the provided total order and corresponding equivalence relation.
    ///
    /// # Panics
    /// If either of the two keys is corrupted to the extent that it cannot be sensibly compared
    /// with the other, a panic may occur. Call [`Self::validate_comparable`] on each key to check
    /// whether this method would panic.
    ///
    /// [`BytewiseComparator`]: super::implementors::BytewiseComparator
    #[must_use]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;

    /// Called on keys read from a LevelDB database (as it is assumed that the persistent data
    /// might be corrupt), though not on keys newly inserted to the database, which are assumed
    /// to not be corrupt.
    ///
    /// This method should only perform the minimum necessary validation to ensure that
    /// [`Self::cmp`] can perform sensible comparisons on the given `key`; it should generally
    /// not be necessary to fully validate that the `key` could actually have been inserted
    /// into the database.
    fn validate_comparable(&self, key: &[u8]) -> Result<(), Self::InvalidKeyError>;

    /// Find a short byte slice which compares greater than or equal to `from`
    /// and strictly less than `to`.
    ///
    /// The output slice should be written to `separator`.
    ///
    /// Implementors may assume that `from` compares strictly less than `to` and that the passed
    /// `separator` is an empty `Vec`, and callers must uphold these assumptions.
    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>);

   /// Find a short byte slice which compares greater than or equal to `key`.
    ///
    /// The output slice should be written to `successor`.
    ///
    /// Implementors may assume that the passed `successor` is an empty `Vec`, and callers must
    /// uphold this assumption.
    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>);
}

/// A trait for generating "filters" which check whether a list of input keys *probably* contains or
/// *definitely does not* contain a given key.
///
/// Filters are used to optimize the performance of random-access read performance; that is,
/// the performance of getting the value (if any) corresponding to an arbitrary key in a LevelDB
/// database. If a filter associated with part of the database does not match a key, then that part
/// of the database does not need to be read and searched for an entry corresponding to that key.
///
/// The canonical example is a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter),
/// though other filters can be used.
pub trait FilterPolicy {
    /// The [equivalence relation](https://en.wikipedia.org/wiki/Equivalence_relation)
    /// on byte slices used by [`Self::key_may_match`].
    type Eq: EquivalenceRelation;
    /// The error returned if a filter could not be generated, which should usually only occur
    /// in extreme conditions.
    type FilterError;

    /// The name identifying the filter policy's behavior.
    ///
    /// Should usually be a valid `&'static str`, but is not strictly required to be UTF-8.
    ///
    /// This name is used to find existing filters in a LevelDB database corresponding to this
    /// policy.
    ///
    /// # Downstream Panics
    /// If the length of the name exceeds one GiB in length (`1 << 30`), panics may occur.
    #[must_use]
    fn name(&self) -> &'static [u8];

    /// Extends the `filter` buffer with a filter corresponding to the provided flattened keys.
    ///
    /// `flattened_keys` is a slice of all the keys concatenated together.
    /// Each element of `key_offsets` is the index of the start of a key in `flattened_keys`.
    ///
    /// The `filter` buffer **must only be extended** with the newly-generated filter; any existing
    /// contents of the buffer must not be modified, or else substantial logical errors may occur.
    /// Implementors **must not** assume that the provided `filter` is an empty `Vec`.
    ///
    /// `self.key_may_match()` must return `true` when it is passed the filter generated here and
    /// a key which compares equal (with respect to the [`EquivalenceRelation`] indicated by
    /// [`Self::Eq`]) to one of the keys in `flattened_keys`.
    ///
    /// An empty filter must not match any keys.
    ///
    /// # Errors
    /// If a filter cannot be correctly generated for the input data, an error should be returned
    /// (rather than panicking or similar).
    ///
    /// Errors should be reserved for extreme (and unlikely) conditions, such as the length of
    /// `flattened_keys` and/or `key_offsets` exceeding [`u32::MAX`] or some other high numeric
    /// limit that prevents a filter from being generated.
    ///
    /// If a filter fails to be generated, an entire file of the LevelDB database (which, with
    /// default settings, contains slightly over 2 megabytes of data) will not have filters,
    /// which will reduce the performance random-access reads involving that file.
    /// (Any filters already generated for that file before the erroring call to this function
    /// will be discarded, and additional filters will not be generated for that file.) No harm
    /// is done to other files of the database.
    fn create_filter(
        &self,
        flattened_keys: &[u8],
        key_offsets:    &[usize],
        filter:         &mut Vec<u8>,
    ) -> Result<(), Self::FilterError>;

    /// Return `true` if something comparing equal (with respect to the [`EquivalenceRelation`]
    /// indicated by [`Self::Eq`]) to the `key` may have been among the keys for which the `filter`
    /// was generated.
    ///
    /// False positives are permissible, while false negatives are a logical error.
    /// Additionally, if the provided filter has length `0`, the key must not match.
    #[must_use]
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

// NOTE: the above `FilterPolicy` trait does not provide good enough support for custom filters
// of Google's C++ filter interface, which expect a slice of key slices instead of a single
// flattened key slice. Better interoperability could be achieved with a little effort,
// if someone shows interest.

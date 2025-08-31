use std::cmp::Ordering;


/// The maximum length that the `flattened_keys` and `key_offsets` slices passed to
/// [`FilterPolicy::create_filter`] may have.
///
/// Equal to `1 << 20`.
pub const FILTER_KEYS_LENGTH_LIMIT: u32 = 1 << 20;


/// Trait for comparing two byte slices in a LevelDB database. In addition to the comparison
/// function, several operations needed by LevelDB are included.
///
/// See [`BytewiseComparator`] for a good default implementation which uses byte slices'
/// [`Ord`] implementation.
///
/// [`BytewiseComparator`]: super::implementors::BytewiseComparator
pub trait LevelDBComparator {
    /// The name identifying the comparator's behavior.
    ///
    /// The name should usually be a valid `&'static str`, but is not strictly required to be UTF-8.
    ///
    /// When opening a LevelDB database, it is checked that the database's comparator name matches
    /// the name of the comparator used to open the database. Try to make the name somewhat
    /// unique, to help catch mistakes.
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
    /// However, note that the [`FilterPolicy`] of a LevelDB database must be compatible with the
    /// equivalence relation of the `LevelDBComparator` of the database; see
    /// [`FilterPolicy::key_may_match`].
    ///
    /// Unsafe code is *not* allowed to rely on the correctness of implementations; that is, an
    /// incorrect implementation may cause severe logic errors, but must not cause
    /// memory unsafety.
    ///
    /// [`BytewiseComparator`]: super::implementors::BytewiseComparator
    #[must_use]
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering;

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

pub trait FilterPolicy {
    /// The name identifying the filter policy's behavior.
    ///
    /// The name should usually be a valid `&'static str`, but is not strictly required to be UTF-8.
    ///
    /// When opening a LevelDB database using a certain [`FilterPolicy`], this name is used to find
    /// the existing filters related to this policy. Try to make the name unique to avoid conflicts
    /// and help catch mistakes.
    #[must_use]
    fn name(&self) -> &'static [u8];

    /// Extends the `filter` buffer with a filter corresponding to the provided flattened keys.
    ///
    /// `flattened_keys` is a slice of all the keys concatenated together.
    /// Each element of `key_offsets` is the index of the start of a key in `flattened_keys`.
    /// Implementors may assume that `flattened_keys.len() <= 1 << 20`
    /// and `key_offsets.len() <= 1 << 20`, and callers must uphold this length constraint.
    /// This limit is available as [`FILTER_KEYS_LENGTH_LIMIT`].
    ///
    /// The `filter` buffer must _only_ be extended; any existing contents of the buffer must not
    /// be modified, or else severe logical errors may occur. Implementors **must not** assume
    /// that the provided `filter` is an empty `Vec`.
    ///
    /// When the generated filter is passed to `self.key_may_match()` along with a key which
    /// compares equal to one of the flattened keys, `self.key_may_match()` must return true.
    ///
    /// The `FilterPolicy` and [`LevelDBComparator`] of a LevelDB database must be compatible; in
    /// particular, if the equivalence relation of the [`LevelDBComparator`] is looser than strict
    /// equality, the `FilterPolicy` must ensure that generated filters match not only the exact
    /// keys for which the filter was generated, but also any key which compares equal to a key
    /// the filter was generated for.
    fn create_filter(
        &self,
        flattened_keys: &[u8],
        key_offsets:    &[usize],
        filter:         &mut Vec<u8>,
    );

    /// Return `true` if something comparing equal to the `key` may have been among
    /// the keys for which the `filter` was generated.
    ///
    /// False positives are permissible, while false negatives are a logical error.
    ///
    /// The `FilterPolicy` and [`LevelDBComparator`] of a LevelDB database must be compatible; in
    /// particular, if the equivalence relation of the [`LevelDBComparator`] is looser than strict
    /// equality, the `FilterPolicy` must ensure that generated filters match not only the exact
    /// keys for which the filter was generated, but also any key which compares equal to a key
    /// the filter was generated for.
    #[must_use]
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

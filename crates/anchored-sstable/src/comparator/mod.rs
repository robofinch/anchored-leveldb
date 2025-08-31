mod implementors;


use std::cmp::Ordering;


pub use self::implementors::{ComparatorAdapter, LexicographicComparator, MetaindexComparator};


/// Trait for comparing two byte slices in a [`Table`]. In addition to the comparison function,
/// several operations needed by the [`Table`] are included.
///
/// See [`LexicographicComparator`] for a good default implementation which uses byte slices'
/// [`Ord`] implementation.
///
/// [`Table`]: crate::table::Table
pub trait TableComparator {
    /// Compare two byte slices in a total order.
    ///
    /// This method is analogous to [`Ord::cmp`]; in fact, [`LexicographicComparator`] uses `Ord`.
    ///
    /// Note that none of the axioms that define a total order require that two elements which
    /// compare as equal are "*truly*" equal in some more fundamental sense; that is, keys which
    /// are distinct (perhaps according to the [`Eq`] implementation of `[u8]`) may compare as
    /// equal in the provided total order and corresponding equivalence relation.
    ///
    /// However, note that the [`TableFilterPolicy`] of a [`Table`] must be compatible with the
    /// equivalence relation of the `TableComparator` of the [`Table`]; see
    /// [`TableFilterPolicy::key_may_match`].
    ///
    /// Unsafe code is *not* allowed to rely on the correctness of implementations; that is, an
    /// incorrect implementation may cause severe logic errors, but must not cause
    /// memory unsafety.
    ///
    /// [`TableFilterPolicy`]: crate::filters::TableFilterPolicy
    /// [`TableFilterPolicy::key_may_match`]: crate::filters::TableFilterPolicy::key_may_match
    /// [`Table`]: crate::table::Table
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

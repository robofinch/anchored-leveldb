mod implementors;


use std::cmp::Ordering;


pub use self::implementors::{
    ComparatorAdapter, DefaultComparator, DefaultComparatorID, MetaindexComparator,
};


/// Trait for comparing two byte slices in a [`Table`]. In addition to the comparison function,
/// several operations needed by the [`Table`] are included.
///
/// If [`TableComparator::cmp`] is overridden, then [`find_short_separator`] and
/// [`find_short_successor`] should be overridden as well.
///
/// [`find_short_separator`]: TableComparator::find_short_separator
/// [`find_short_successor`]: TableComparator::find_short_successor
pub trait TableComparator {
    /// A unique identifier for the comparator's behavior.
    ///
    /// Should usually be a valid `&'static str`, but is not strictly required to be UTF-8.
    ///
    /// When opening a [`Table`], it is checked that the comparator id matches the id on disk.
    #[must_use]
    fn id(&self) -> &'static [u8];

    /// Compare two byte slices in a total order.
    ///
    /// This method is analogous to [`Ord::cmp`]; in fact, [`DefaultComparator`] uses `Ord`.
    ///
    /// Note that none of the axioms that define a total order require that two elements which
    /// compare as equal are "*truly*" equal in some more fundamental sense; that is, keys which
    /// are distinct (perhaps according to the [`Eq`] implementation of `[u8]`) may compare as
    /// equal in the provided total order and corresponding equivalence relation.
    ///
    /// Unsafe code is *not* allowed to rely on the correctness of implementations; that is, an
    /// incorrect implementation may cause severe logic errors, but must not cause
    /// memory unsafety.
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

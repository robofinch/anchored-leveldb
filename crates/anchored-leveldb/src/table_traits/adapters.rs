use std::cmp::Ordering;

use clone_behavior::{IndependentClone, MirroredClone, Speed};
use seekable_iterator::Comparator;

use anchored_sstable::options::{TableComparator, TableFilterPolicy};

use crate::public_format::EntryType;
use crate::format::{
    sequence_and_type_tag, EncodedInternalKey, EncodedMemtableEntry, InternalKey,
    MemtableEntry, SequenceNumber, UserKey,
};
use super::trait_equivalents::{FilterPolicy, LevelDBComparator};


// TODO: if feasible, ensure that keys are always validated *before* reaching a comparator.
// If that could be ensured, then a bunch of the error handling goes poof.

/// Sort first by user key, then in decreasing order by sequence number, and then by
/// decreasing order by entry type.
#[inline]
#[must_use]
fn cmp_internal_keys<Cmp: LevelDBComparator>(
    user_cmp: &Cmp,
    lhs:      InternalKey<'_>,
    rhs:      InternalKey<'_>,
) -> Ordering {
    match user_cmp.cmp(lhs.user_key.0, rhs.user_key.0) {
        Ordering::Equal => {},
        non_equal @ (Ordering::Less | Ordering::Greater) => return non_equal,
    }

    // Swapped lhs and rhs to sort decreasing
    match rhs.sequence_number.cmp(&lhs.sequence_number) {
        Ordering::Equal => {},
        non_equal @ (Ordering::Less | Ordering::Greater) => return non_equal,
    }

    // Swapped lhs and rhs to sort decreasing
    u8::from(rhs.entry_type).cmp(&u8::from(lhs.entry_type))
}

/// This type fulfills the semantic constraints of [`TableComparator`], in addition to satisfying
/// additional properties.
///
/// # Additional Property
///
/// We ensure that where `min_bound` is an internal key with user key `user_key`, sequence number
/// `seq_num` which is strictly less than the maximum sequence number, and entry type
/// [`EntryType::MAX_TYPE`] that calling [`Table::get`] on `min_bound` will always return
/// `Some(None(_))` if there is an internal key in the `Table` whose user key is `user_key` and
/// whose sequence number is at most `seq_num`.
///
/// # Sufficient Requirements
///
/// To ensure the above property, it suffices that:
/// - internal keys are first sorted by user key, and then in decreasing order by sequence
///   number, and then in decreasing order by entry type;
/// - if a filter of the internal filter policy did not match `min_bound`, no user key
///   comparing equal to `user_key` was used to create that filter; and
/// - for any two internal keys `from` and `to` which are adjacent in the `Table`, if
///   `from < min_bound < to` and `min_bound` is less than or equal to
///   `internal_cmp.find_short_separator(from, to)` where `internal_cmp` is the `Table`'s internal
///   comparator, then there is no internal key in the `Table` with user key `user_key` and
///   sequence number at most `seq_num`.
///
/// # Justification of Requirements
///
/// To show that these four requirements (in addition to the existing semantic obligations of
/// [`TableComparator`], [`TableFilterPolicy`], and so on) suffice, we can consider the four
/// conditions in which [`Table::get`] may return `Ok(None)`, assuming that no corruption is
/// is encountered.
///
/// If [`Table::get`] returns `Ok(None)` with the above assumptions, then either:
///
/// #### Case 1
/// There is no internal key in the `Table` greater than or equal to `min_bound`.
///
/// Since internal keys are sorted first by user key and then by sequence number in decreasing
/// order, there is no internal key in the `Table` with user key `user_key` and sequence number
/// less than or equal to `min_bound`, since such an internal key would sort greater than or equal
/// to `min_bound`.
///
/// #### Case 2
/// A filter was generated on all keys in the `Table` greater than or equal to `min_bound`, and
/// that filter did not match `min_bound`.
///
/// Since the filter would have matched `min_bound` if any internal key with a user key comparing
/// equal to `user_key` were used to create the filter, none of the keys in the `Table` greater
/// than or equal to `min_bound` have user key `user_key`. For the same reason as above, this
/// implies that there is no internal key in the `Table` with user key `user_key` and sequence
/// number less than or equal to `min_bound`.
///
/// #### Case 3
/// There exist internal keys `from` and `to` in the `Table` such that `from < to` and a `filter`
/// did not match `min_bound`, where:
/// - `min_bound <= separator`,
/// - `separator` is the output of `internal_cmp.find_short_separator(from, to)`, and
/// - `filter` is a filter generated from all keys in the `Table` loosely between `min_bound`
///   and `separator`.
///
/// In this case:
/// - No internal keys in the `Table` loosely between `min_bound` and that `separator` have user
///   key `user_key`, because otherwise the filter would have matched.
/// - If `min_bound <= from`, then:
///   - `from` is an internal key loosely between `min_bound` and `separator`, so `from` does
///     not have user key `user_key`.
///   - By the sorting of internal keys, the user key of `from` must compare greater than or equal
///     to `user_key`, and, combined with the above bullet point, it must be strictly greater than
///     `user_key`.
///   - Any internal keys in the table with user key `user_key` and sequence number at most
///     `seq_num` compare greater than or equal to `min_bound` and strictly less than `from`.
///   - Therefore, any such internal key would be loosely between `min_bound` and `from`
///     and thus loosely between `min_bound` and `separator`; therefore, no such key exists by
///     the first bullet of this case (otherwise the filter would have matched).
/// - Otherwise, `min_bound > from`, in which case:
///   - If `min_bound` compared equal to `to`, then since `from <= separator < to`, we would have
///     `separator < min_bound`; since `min_bound <= separator` in all of Case 3, we thus know that
///     `min_bound < to`.
///   - Because `from` and `to` are internal keys in the `Table` such that `from < min_bound < to`
///     and `min_bound <= separator` where `separator` is
///     `internal_cmp.find_short_separator(from, to)`, it follows that there is no internal
///     key in the `Table` with user key `user_key` and sequence number at most `seq_num`.
///
/// #### Case 4
/// There exist internal keys `from` and `to` in the `Table` such that `from < min_bound < to`
/// and `min_bound <= separator` where `separator` is `internal_cmp.find_short_separator(from, to)`.
///
/// By assumption of how `InternalComparator::find_short_separator` behaves, there is no internal
/// key in the `Table` with user key `user_key` and sequence number at most `seq_num`.
///
///
/// ## Summary
/// In all four cases, if [`Table::get`] returns `Ok(None)`, then there is no internal
/// key in the `Table` with user key `user_key` and sequence number at most `seq_num`.
///
/// Therefore, if there _is_ such an internal key, then [`Table::get`] returns `Ok(Some(_))`
/// (unless corruption or a similar error was found), and it would be the least internal key
/// in the `Table` with `user_key` and whose sequence number is at most `seq_num`.
///
/// Note that if there is not such an internal key, it is still possible for [`Table::get`]
/// to return `Ok(Some(_))`. In particular, the `user_key` must be checked; if [`Table::get`]
/// returns `Ok(Some(_))` and the entry's user key is `user_key`, then it is the least internal key
/// in the `Table` with `user_key` and whose sequence number is at most `seq_num`. The fact about
/// the sequence number follows from the ordering of internal keys.
///
/// # Fulfilling the relevant requirements
/// The first requirement places an additional constraint on [`InternalComparator::cmp`].
///
/// The second requirement places an additional constraint on [`FilterPolicy`]; essentially, it must
/// simply ignore the sequence number and entry type, and only consider the user key.
///
/// The third requirement places an additional constraint on
/// [`InternalComparator::find_short_separator`].
/// Note that it may be assumed that any internal keys in the `Table` are valid; therefore, the
/// third requirement can be reduced to showing for any three valid internal keys
/// `from < min_bound < to` that if:
/// - `min_bound <= Self::find_short_separator(_, from, to)`,
/// - the sequence number of `min_bound` is strictly less than the maximum sequence number, and
/// - the entry type of `min_bound` is the greatest possible entry type,
///
/// then any valid internal key whose user key compares equal to the user key of `min_bound`
/// and whose sequence number is less than or equal to that of `min_bound` is strictly between
/// `from` and the separator. This reduction implies that such an internal key is strictly
/// between `from` and `to`. In the third requirement, the keys `from` and `to` are adjacent,
/// so there is no internal key in the `Table` strictly between them, so the third requirement
/// is met.
///
/// [`Table::get`]: anchored_sstable::Table::get
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct InternalComparator<Cmp>(pub Cmp);

impl<Cmp: LevelDBComparator> InternalComparator<Cmp> {
    /// Compare two valid internal keys in a total order.
    ///
    /// Internal keys are sorted first by user key (with respect to `Cmp`), then by sequence
    /// number in decreasing order, and lastly by entry type in decreasing order
    /// ([`EntryType::Value`] first and [`EntryType::Deletion`] second).
    ///
    /// In particular, [`EntryType::MAX_TYPE`] compares less than or equal to the other
    /// entry types.
    ///
    /// See the type-level documentation of [`InternalComparator`] for reasoning reliant on this
    /// ordering.
    #[must_use]
    pub fn cmp_internal(&self, lhs: InternalKey<'_>, rhs: InternalKey<'_>) -> Ordering {
        cmp_internal_keys(&self.0, lhs, rhs)
    }

    /// Compare two user keys with respect to the user comparator.
    #[must_use]
    pub fn cmp_user(&self, lhs: UserKey<'_>, rhs: UserKey<'_>) -> Ordering {
        self.0.cmp(lhs.0, rhs.0)
    }
}

// TODO: I see a `leveldb.InternalKeyComparator` string.
// Is that name part of the on-disk format, or are only the user comparator names used?
// I would hope it's the latter, otherwise the ID is quite useless.

impl<Cmp: LevelDBComparator> TableComparator for InternalComparator<Cmp> {
    /// Compare two internal keys in a total order.
    ///
    /// Internal keys are sorted first by user key (with respect to `Cmp`), then by sequence
    /// number in decreasing order, and lastly by entry type in decreasing order
    /// ([`EntryType::Value`] first and [`EntryType::Deletion`] second).
    ///
    /// In particular, [`EntryType::MAX_TYPE`] compares less than or equal to the other
    /// entry types.
    ///
    /// See the type-level documentation of [`InternalComparator`] for reasoning reliant on this
    /// ordering.
    ///
    /// # Invalid keys
    /// Any byte slice which is not a valid internal key are sorted equal to each other and sorted
    /// sorted strictly greater than any valid internal key.
    ///
    /// Provided that `Cmp` is compatible with `Policy`, the equivalence relation of
    /// `InternalComparator<Cmp>` is compatible with `InternalFilterPolicy<Policy>` because they
    /// handle invalid internal keys in a consistent way.
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        /// Fallback for when one or both of the internal keys are corrupted/invalid.
        ///
        /// Corrupted internal keys sort as the last key (greater than anything else, and equal
        /// to each other).
        #[inline(never)]
        fn error_fallback(
            lhs: Result<InternalKey<'_>, ()>,
            rhs: Result<InternalKey<'_>, ()>,
        ) -> Ordering {
            // TODO: log errors

            #[expect(clippy::unreachable, reason = "this is a fallback for the non-Ok cases")]
            match (lhs, rhs) {
                (Ok(_), Ok(_))                 => unreachable!(),
                (Ok(_), Err(_rhs_err))         => Ordering::Less,
                (Err(_lhs_err), Ok(_))         => Ordering::Greater,
                (Err(_lhs_err), Err(_rhs_err)) => Ordering::Equal
            }
        }

        let lhs = InternalKey::decode(EncodedInternalKey(lhs));
        let rhs = InternalKey::decode(EncodedInternalKey(rhs));

        let (Ok(lhs), Ok(rhs)) = (lhs, rhs) else {
            return error_fallback(lhs, rhs)
        };

        cmp_internal_keys(&self.0, lhs, rhs)
    }

    /// Find a short byte slice which compares greater than or equal to `from`
    /// and strictly less than `to`.
    ///
    /// The separator is written to `separator`. It is assumed that `from` compares strictly less
    /// than `to` and that the passed `separator` is an empty `Vec`; callers must uphold these
    /// assumptions.
    ///
    /// Additionally, this function ensures that for any three valid internal keys `from`,
    /// `min_bound`, and `to` such that `from < min_bound < to`, if:
    /// - `min_bound <= Self::find_short_separator(_, from, to)`,
    /// - the sequence number of `min_bound` is strictly less than the maximum sequence number, and
    /// - the entry type of `min_bound` is the greatest possible entry type,
    ///
    /// then any valid internal key with a user key equal to that of `min_bound` and a sequence
    /// number less than or equal to the sequence number of `min_bound` is strictly greater than
    /// `from` and strictly less than `separator`.
    ///
    /// See the type-level documentation of [`InternalComparator`] for reasoning reliant on this
    /// additional behavior.
    fn find_short_separator(&self, from: &[u8], to: &[u8], separator: &mut Vec<u8>) {
        // Summary: if there's an invalid internal key, give up and output `from` as the separator;
        // in non-corrupted situations, it's always a valid response, and it's hard to do any
        // better in a corrupted situation.
        //
        // Otherwise, if `from` and `to` have equal user key, output `from`.
        // Otherwise, generate a user separator, `user_separator`, for the user keys of `from` and
        // `to`, and if `user_separator` compares equal to the user key of `from`, add the
        // sequence number and entry type of `from` to the `user_separator` to get a `separator`
        // which compares equal to `from`.
        // Otherwise, output an internal key with user key `user_separator`, the highest possible
        // sequence number, and the greatest valid internal entry type.
        //
        // This ensures that if `from < min_bound < to` and `min_bound <= separator`, we cannot
        // be in the first or second case (as otherwise `from < min_bound <= separator == from`).
        // Because the having the highest possible sequence number and valid entry type makes
        // `separator` the least internal key with its user key, the only way for `min_bound`
        // to be less than or equal to `separator` without having the maximum sequence number
        // is for `min_bound`'s user key to be strictly less than that of `separator`.
        // Therefore, any internal key with the user key of `min_bound` and sequence number
        // at most that of `min_bound` compares greater than or equal to `min_bound` and strictly
        // less than `separator`, by comparison on user keys. Therefore, any such internal key
        // is strictly between `from` and `separator`.

        let from_decoded = InternalKey::decode(EncodedInternalKey(from));
        let to_decoded = InternalKey::decode(EncodedInternalKey(to));

        #[expect(clippy::shadow_unrelated, reason = "prevent accidental use of the user keys")]
        let (Ok(from), Ok(to)) = (from_decoded, to_decoded) else {
            // TODO: log errors

            // Note that `from` is in the interval `[from, to)`, so this is, generally, valid.
            separator.extend(from);
            return;
        };

        if self.cmp_user(from.user_key, to.user_key) == Ordering::Equal {
            from.append_encoded(separator);
            return;
        }

        // Note that in this code path, the `separator` buffer is empty before this call.
        self.0.find_short_separator(from.user_key.0, to.user_key.0, separator);

        if self.cmp_user(from.user_key, UserKey(separator)) == Ordering::Equal {
            separator.extend(from.tag().to_le_bytes());
        } else {
            separator.extend(sequence_and_type_tag(
                SequenceNumber::MAX_SEQUENCE_NUMBER,
                EntryType::MAX_TYPE,
            ).to_le_bytes());
        }
    }

    fn find_short_successor(&self, key: &[u8], successor: &mut Vec<u8>) {
        let decoded_key = InternalKey::decode(EncodedInternalKey(key));

        #[expect(clippy::shadow_unrelated, reason = "prevent accidental use of the user key")]
        let Ok(key) = decoded_key else {
            // TODO: log error

            // Note that `key` is in the range `key..`, so this is, generally, valid.
            successor.extend(key);
            return;
        };

        self.0.find_short_successor(key.user_key.0, successor);

        // Note that regardless of whether the above call produced a strictly larger successor,
        // the greatest possible internal key for a given user key is the internal key with the
        // minimum sequence number and entry type.
        successor.extend(sequence_and_type_tag(
            SequenceNumber::ZERO,
            EntryType::MIN_TYPE,
        ).to_le_bytes());
    }
}

impl<Cmp, S> MirroredClone<S> for InternalComparator<Cmp>
where
    Cmp: MirroredClone<S>,
    S:   Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

impl<Cmp, S> IndependentClone<S> for InternalComparator<Cmp>
where
    Cmp: IndependentClone<S>,
    S:   Speed,
{
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

/// This type fulfills the semantic constraints of [`TableFilterPolicy`], in addition to
/// satisfying an additional property.
///
/// # Additional Property
///
/// We consider only the user key for creating filters and checking filters for matches. Therefore,
/// a filter will match any internal key whose user key compares equal to the user key of any of
/// the internal keys provided when creating the filter.
///
/// See the type-level documentation of [`InternalComparator`] for reasoning reliant on this
/// property.
///
/// # Comparator-Policy Compatibility
///
/// Provided that `Cmp` is compatible with `Policy`, the equivalence relation of
/// `InternalComparator<Cmp>` is compatible with `InternalFilterPolicy<Policy>` because they
/// handle invalid internal keys in a consistent way.
///
/// See [`TableFilterPolicy::key_may_match`].
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct InternalFilterPolicy<Policy>(pub Policy);

impl<Policy: FilterPolicy> TableFilterPolicy for InternalFilterPolicy<Policy> {
    #[inline]
    fn name(&self) -> &'static [u8] {
        self.0.name()
    }

    fn append_key_data(&self, key: &[u8], flattened_key_data: &mut Vec<u8>) {
        match EncodedInternalKey(key).user_key() {
            Ok(user_key) => flattened_key_data.extend(user_key.0),
            // TODO: log error
            Err(_)       => {},
        }
    }

    fn create_filter(
        &self,
        flattened_key_data: &[u8],
        key_offsets:        &[usize],
        filter:             &mut Vec<u8>,
    ) {
        // We know that the `flattened_key_data` slice consists of concatenated user keys,
        // plus possibly some additional empty slices from invalid internal keys.
        // Provided that the empty slice is a valid user key, that shouldn't do any harm.
        // TODO: make sure that no key is inserted into a TableBuilder unless it's confirmed
        // to be a valid internal key. (If I do that, I can remove some of the error logs
        // from filter generation.)
        self.0.create_filter(flattened_key_data, key_offsets, filter);
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        let Ok(user_key) = EncodedInternalKey(key).user_key() else {
            // TODO: log error
            // If there's corruption, default to saying "maybe the key is in the filter"
            // since we can't know for sure that we should return false.
            return true;
        };

        // The filter was generated by the user filter policy for the concatenated user keys.
        self.0.key_may_match(user_key.0, filter)
    }
}

impl<Policy, S> MirroredClone<S> for InternalFilterPolicy<Policy>
where
    Policy: MirroredClone<S>,
    S:      Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

impl<Policy, S> IndependentClone<S> for InternalFilterPolicy<Policy>
where
    Policy: IndependentClone<S>,
    S:      Speed,
{
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

/// Sort two [`EncodedMemtableEntry`]s by their internal keys. (Two entries with the same
/// `internal_key`s and different `value`s still compare equal).
///
/// This comparator should only be used for skiplists which exclusively contain
/// slices of [`EncodedMemtableEntry`]s. In particular, byte slices which are not valid
/// [`EncodedMemtableEntry`]s may cause a panic in this comparator.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct MemtableComparator<Cmp>(pub Cmp);

impl<Cmp: LevelDBComparator> Comparator<[u8]> for MemtableComparator<Cmp> {
    /// Sort two valid [`EncodedMemtableEntry`]s by their internal keys. (Two entries with the same
    /// `internal_key`s and different `value`s still compare equal).
    ///
    /// # Panics
    /// Panics if either slice is not a valid [`EncodedMemtableEntry`].
    fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Ordering {
        // TODO(opt): if benchmarks ever indicate that this is a bottleneck, then
        // consider manually inlining parts of `MemtableEntry::decode` and `cmp_internal_keys`
        // for maximal performance.

        // We declare the panic, so it's fine to call `new_unchecked`.
        let lhs = MemtableEntry::decode(EncodedMemtableEntry::new_unchecked(lhs));
        let rhs = MemtableEntry::decode(EncodedMemtableEntry::new_unchecked(rhs));

        cmp_internal_keys(&self.0, lhs.internal_key(), rhs.internal_key())
    }
}

impl<Cmp, S> MirroredClone<S> for MemtableComparator<Cmp>
where
    Cmp: MirroredClone<S>,
    S:   Speed,
{
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

impl<Cmp, S> IndependentClone<S> for MemtableComparator<Cmp>
where
    Cmp: IndependentClone<S>,
    S:   Speed,
{
    #[inline]
    fn independent_clone(&self) -> Self {
        Self(self.0.independent_clone())
    }
}

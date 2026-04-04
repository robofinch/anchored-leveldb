use std::cmp::Ordering;

use clone_behavior::{MirroredClone, Speed};

use anchored_skiplist::Comparator;

use crate::{
    pub_traits::cmp_and_policy::{FilterPolicy, LevelDBComparator},
    pub_typed_bytes::{EntryType, SequenceNumber},
    typed_bytes::{EncodedInternalKey, InternalKey, InternalKeyTag, UserKey},
};


/// In addition to providing a total order whose equivalence relation is strictly finer than
/// [`InternalFilterPolicy<Policy>`]'s equivalence relation (provided that the equivalence relation
/// of `Cmp` is finer than that of `Policy`), this comparator satisfies an additional property.
///
/// # Additional Property
///
/// For any lookup key `lookup_key` (which corresponds to some internal key `min_bound` with user
/// key `user_key`, sequence number `seq_num` which is strictly less than the maximum sequence
/// number, and entry type [`EntryType::MAX_TYPE`]), we ensure that calling [`TableReader::get`]
/// on `lookup_key` will return `Ok(Some(_))` if and only if there is an internal key in the SSTable
/// whose user key is `user_key` and whose sequence number is at most `seq_num`.
///
/// # Sufficient Requirements
///
/// To ensure the above property, it suffices that:
/// - internal keys are first sorted by user key, and then in decreasing order by sequence
///   number, and then in decreasing order by entry type;
/// - if a filter of the internal filter policy did not match `min_bound`, no user key
///   comparing equal to `user_key` was used to create that filter; and
/// - for any two internal keys `from` and `to` which are adjacent in the SSTable, if
///   `from < min_bound < to` and `min_bound` is less than or equal to the result of
///   `self.find_short_separator(from, to, _)` (where the SSTable's entries are in `self`'s sorted
///   order), then there is no internal key in the SSTable with user key `user_key` and
///   sequence number at most `seq_num`.
///
/// # Justification of Requirements
///
/// To show that these three requirements suffice, we can consider the four conditions in which
/// [`TableReader::get`] may return `Ok(None)`, assuming that no corruption is encountered.
///
/// If [`TableReader::get`] returns `Ok(None)` with the above assumptions, then either:
///
/// #### Case 1
/// There is no internal key in the SSTable greater than or equal to `min_bound` with a user key
/// that compares equal to `user_key`.
///
/// Since internal keys are sorted first by user key and then by sequence number in decreasing
/// order, there is no internal key in the SSTable with user key `user_key` and sequence number
/// less than or equal to `min_bound`, since such an internal key would sort greater than or equal
/// to `min_bound`.
///
/// #### Case 2
/// A filter was generated on all keys in the SSTable greater than or equal to `min_bound`, and
/// that filter did not match `min_bound`.
///
/// Since the filter would have matched `min_bound` if any internal key with a user key comparing
/// equal to `user_key` were used to create the filter, none of the keys in the SSTable greater
/// than or equal to `min_bound` have user key `user_key`. For the same reason as above, this
/// implies that there is no internal key in the SSTable with user key `user_key` and sequence
/// number less than or equal to `min_bound`.
///
/// #### Case 3
/// There exist internal keys `from` and `to` which are adjacent in the SSTable such that
/// `from < to` and a `filter` did not match `min_bound`, where:
/// - `min_bound <= separator`,
/// - `separator` is the output of `self.find_short_separator(from, to, _)`, and
/// - `filter` is a filter generated on (at least) all keys in the SSTable loosely between
///   `min_bound` and `separator`.
///
/// In this case:
/// - No internal keys in the SSTable loosely between `min_bound` and that `separator` have user
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
///   - Since `from <= separator < to`, and `min_bound <= separator` in all of Case 3, we know
///     that `min_bound < to`.
///   - Because `from` and `to` are internal keys in the SSTable such that `from < min_bound < to`
///     and `min_bound <= separator` where `separator` is the output of
///     `self.find_short_separator(from, to, _)`, it follows that there is no internal
///     key in the SSTable with user key `user_key` and sequence number at most `seq_num`.
///
/// #### Case 4
/// There exist adjacent internal keys `from` and `to` in the SSTable such that
/// `from < min_bound < to` and `min_bound <= separator`, where `separator` is the output of
/// `self.find_short_separator(from, to, _)`.
///
/// By assumption of how `InternalComparator::find_short_separator` behaves, there is no internal
/// key in the SSTable with user key `user_key` and sequence number at most `seq_num`.
///
///
/// ## Summary
/// In all four cases, if [`TableReader::get`] returns `Ok(None)`, then there is no internal
/// key in the SSTable with user key `user_key` and sequence number at most `seq_num`.
///
/// Therefore, if there _is_ such an internal key, then [`TableReader::get`] returns `Ok(Some(_))`
/// (unless corruption or a similar error was found), and it would be the least internal key
/// in the SSTable with `user_key` and whose sequence number is at most `seq_num`.
///
/// # Fulfilling the relevant requirements
/// The first requirement places an additional constraint on `InternalComparator::cmp`.
///
/// The second requirement places an additional constraint on [`InternalFilterPolicy`];
/// essentially, it must simply ignore the sequence number and entry type, and only consider the
/// user key.
///
/// The third requirement places an additional constraint on
/// [`InternalComparator::find_short_separator`], which reduces to showing for any three valid
/// internal keys `from < min_bound < to` that if:
/// - `min_bound` is less than or equal to the output of `self.find_short_separator(from, to, _)`,
/// - the sequence number of `min_bound` is strictly less than the maximum sequence number, and
/// - the entry type of `min_bound` is the greatest possible entry type,
///
/// then any valid internal key whose user key compares equal to the user key of `min_bound`
/// and whose sequence number is less than or equal to that of `min_bound` is strictly between
/// `from` and the separator. This reduction implies that such an internal key is strictly
/// between `from` and `to`. In the third requirement, the keys `from` and `to` are adjacent,
/// so there is no internal key in the SSTable strictly between them, so the third requirement
/// is met by this reduction.
///
/// [`TableReader::get`]: crate::sstable::TableReader::get
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct InternalComparator<Cmp>(pub Cmp);

impl<Cmp: LevelDBComparator> Comparator<EncodedInternalKey<'_>, EncodedInternalKey<'_>>
for InternalComparator<Cmp>
{
    #[inline]
    fn cmp(&self, lhs: EncodedInternalKey<'_>, rhs: EncodedInternalKey<'_>) -> Ordering {
        self.cmp(lhs.as_internal_key(), rhs.as_internal_key())
    }
}

impl<Cmp: LevelDBComparator> Comparator<InternalKey<'_>, InternalKey<'_>>
for InternalComparator<Cmp>
{
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
    #[inline]
    fn cmp(&self, lhs: InternalKey<'_>, rhs: InternalKey<'_>) -> Ordering {
        match self.cmp_user(lhs.0, rhs.0) {
            Ordering::Equal => {},
            non_equal @ (Ordering::Less | Ordering::Greater) => return non_equal,
        }

        // Swapped lhs and rhs to sort decreasing. Note that the sequence number comparison
        // takes precedence over the entry type comparison, since the entry type is stored
        // in the least significant byte.
        rhs.1.raw_inner().cmp(&lhs.1.raw_inner())
    }
}

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Cmp: LevelDBComparator> InternalComparator<Cmp> {
    /// Called on keys read from a LevelDB database (as it is assumed that the persistent data
    /// might be corrupt), though not on keys newly inserted to the database, which are assumed to
    /// not be corrupt.
    pub fn validate_user(&self) -> impl Fn(UserKey<'_>) -> Result<(), Cmp::InvalidKeyError> {
        move |user_key| self.0.validate_comparable(user_key.inner())
    }

    /// Compare two user keys with respect to the user comparator.
    #[inline]
    #[must_use]
    pub fn cmp_user(&self, lhs: UserKey<'_>, rhs: UserKey<'_>) -> Ordering {
        self.0.cmp(lhs.inner(), rhs.inner())
    }

    /// Find a short [`EncodedInternalKey`] which compares greater than or equal to `from`
    /// and strictly less than `to`.
    ///
    /// The encoded separator is written to `separator`. It is assumed that `from` compares
    /// strictly less than `to` and that the passed `separator` is an empty `Vec`; callers must
    /// uphold these assumptions.
    ///
    /// Additionally, this function ensures that for any three internal keys `from`,
    /// `min_bound`, and `to` such that `from < min_bound < to`, if:
    /// - `min_bound <= Self::find_short_separator(_, from, to)`,
    /// - the sequence number of `min_bound` is strictly less than the maximum sequence number, and
    /// - the entry type of `min_bound` is the greatest possible entry type,
    ///
    /// then any internal key with a user key equal to that of `min_bound` and a sequence
    /// number less than or equal to the sequence number of `min_bound` is strictly greater than
    /// `from` and strictly less than `separator`.
    ///
    /// See the type-level documentation of [`InternalComparator`] for reasoning reliant on this
    /// additional behavior. Note that any [`LookupKey`] meets the constraints on the
    /// sequence number and entry type of `min_bound`.
    ///
    /// # Panics
    /// Panics if `Cmp` incorrectly implements [`LevelDBComparator`] and outputs a
    /// `separator` which is too long. In other words, if this function successfully returns,
    /// `separator` is guaranteed to contain a valid [`EncodedInternalKey`].
    pub fn find_short_separator(
        &self,
        from:      InternalKey<'_>,
        to:        InternalKey<'_>,
        separator: &mut Vec<u8>,
    ) {
        // Summary:
        // 1. If `from` and `to` have equal user key, output `from`.
        // 2. Otherwise, generate a user separator, `user_separator`, for the user keys of `from`
        //    and `to`, and if `user_separator` compares equal to the user key of `from`, add the
        //    sequence number and entry type of `from` to the `user_separator` to get a `separator`
        //    which compares equal to `from`.
        // 3. Otherwise, output an internal key with user key `user_separator`, the highest possible
        //    sequence number, and the greatest valid internal entry type.
        //
        // This ensures that if `from < min_bound < to` and `min_bound <= separator`, we cannot
        // be in the first case or second case (because if we were in one of those cases,
        // `from` would equal `separator`, implying that `from < min_bound <= separator == from`).
        // Because the having the highest possible sequence number and valid entry type makes
        // `separator` the least internal key with its user key, the only way for `min_bound`
        // to be less than or equal to `separator` without having the maximum sequence number
        // is for `min_bound`'s user key to be strictly less than that of `separator`.
        // Therefore, any internal key with the user key of `min_bound` and sequence number
        // at most that of `min_bound` compares greater than or equal to `min_bound` and strictly
        // less than `separator`, by comparison on user keys. Therefore, any such internal key
        // is strictly between `from` and `separator`.

        if self.cmp_user(from.0, to.0).is_eq() {
            from.append_encoded(separator);
            return;
        }

        // Note that in this code path, the `separator` buffer is empty before this call.
        self.0.find_short_separator(from.0.inner(), to.0.inner(), separator);
        #[expect(clippy::expect_used, reason = "this panic is documented by LevelDBComparator")]
        let user_separator = UserKey::new(separator)
            .expect("incorrect LevelDBComparator impl; separator too long");

        let tag = if self.cmp_user(from.0, user_separator).is_eq() {
            from.1
        } else {
            InternalKeyTag::MAX_KEY_TAG
        };

        separator.extend(tag.raw_inner().to_le_bytes().as_slice());
    }

    /// Find a short [`EncodedInternalKey`] which compares greater than or equal to `key`.
    ///
    /// The encoded separator is written to `successor`. It is assumed that the passed `successor`
    /// is an empty `Vec`; callers must uphold this assumption.
    ///
    /// # Panics
    /// Panics if `Cmp` incorrectly implements [`LevelDBComparator`] and outputs a
    /// `successor` which is too long. In other words, if this function successfully returns,
    /// `successor` is guaranteed to contain a valid [`EncodedInternalKey`].
    pub fn find_short_successor(&self, key: InternalKey<'_>, successor: &mut Vec<u8>) {
        self.0.find_short_successor(key.0.inner(), successor);
        // Check that the implementation is correct, instead of letting the error spread to
        // some downstream place.
        #[expect(clippy::expect_used, reason = "this panic is documented by LevelDBComparator")]
        UserKey::new(successor).expect("incorrect LevelDBComparator impl; successor too long");

        // Note that regardless of whether the above call produced a strictly larger successor,
        // the greatest possible internal key for a given user key is the internal key with the
        // minimum sequence number and entry type.
        let tag = InternalKeyTag::new(SequenceNumber::ZERO, EntryType::MIN_TYPE);
        successor.extend(tag.raw_inner().to_le_bytes().as_slice());
    }
}

impl<Cmp: MirroredClone<S>, S: Speed> MirroredClone<S> for InternalComparator<Cmp> {
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

/// In addition to acting as a filter whose equivalence relation is coarser than that of
/// [`InternalComparator<Cmp>`] (provided that the equivalence relation of `Policy` is coarser
/// than that of `Cmp`), this type satisfies an additional property.
///
/// # Additional Property
///
/// Only user keys are considered when creating filters and checking filters for matches. Therefore,
/// a filter will match any internal key whose user key compares equal to the user key of any of
/// the internal keys provided when creating the filter.
///
/// See the type-level documentation of [`InternalComparator`] for reasoning reliant on this
/// property.
#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct InternalFilterPolicy<Policy>(pub Policy);

#[expect(unreachable_pub, reason = "control visibility at type definition")]
impl<Policy: FilterPolicy> InternalFilterPolicy<Policy> {
    /// See [`FilterPolicy::create_filter`].
    ///
    /// The input data must be concatenated [`UserKey`] slices, rather than (for instance)
    /// [`EncodedInternalKey`] slices, and they must be sorted in nondecreasing / ascending order
    /// with respect to the database's comparator.
    ///
    /// [`EncodedInternalKey`]: crate::typed_bytes::EncodedInternalKey
    pub fn create_filter(
        &self,
        flattened_user_key_data: &[u8],
        user_key_offsets:        &[usize],
        filter:                  &mut Vec<u8>,
    ) -> Result<(), Policy::FilterError> {
        // We know that the `flattened_user_key_data` slice consists of concatenated user keys.
        self.0.create_filter(flattened_user_key_data, user_key_offsets, filter)
    }

    /// See [`FilterPolicy::key_may_match`].
    pub fn key_may_match(&self, key: UserKey<'_>, filter: &[u8]) -> bool {
        // The filter was generated by the user filter policy for the concatenated user keys.
        self.0.key_may_match(key.inner(), filter)
    }
}

impl<Policy: MirroredClone<S>, S: Speed> MirroredClone<S> for InternalFilterPolicy<Policy> {
    fn mirrored_clone(&self) -> Self {
        Self(self.0.mirrored_clone())
    }
}

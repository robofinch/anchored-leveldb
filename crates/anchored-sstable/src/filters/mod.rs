mod implementors;


pub use self::implementors::{BloomPolicy, BloomPolicyName, NoFilterPolicy};


/// The maximum length that the `flattened_keys` and `key_offsets` slices passed to
/// [`FilterPolicy::create_filter`] may have.
///
/// Equal to `1 << 20`.
pub const FILTER_KEYS_LENGTH_LIMIT: u32 = 1 << 20;


pub trait FilterPolicy {
    /// The name identifying the filter policy's behavior.
    ///
    /// Should usually be a valid `&'static str`, but is not strictly required to be UTF-8.
    ///
    /// When opening a [`Table`] using a certain [`FilterPolicy`], this name is used to find
    /// the existing filters related to this policy.
    ///
    /// [`Table`]: crate::table::Table
    #[must_use]
    fn name(&self) -> &'static [u8];

    /// Extends the `filter` buffer with a filter corresponding to the provided flattened keys.
    ///
    /// Each element of `key_offsets` is the index of the start of a key in `flattened_keys`.
    /// Implementors may assume that `flattened_keys.len() <= 1 << 20`
    /// and `key_offsets.len() <= 1 << 20`, and callers must uphold this length constraint.
    /// This limit is available as [`FILTER_KEYS_LENGTH_LIMIT`].
    ///
    /// The `filter` buffer must _only_ be extended; any existing contents of the buffer must not
    /// be modified, or else severe logical errors may occur. Implementors **must not** assume
    /// that the provided `filter` is an empty `Vec`.
    ///
    /// When the generated filter is passed to `self.key_may_match()` along with one of the keys
    /// that are among the provided flattened keys, `self.key_may_match()` must return true.
    fn create_filter(&self, flattened_keys: &[u8], key_offsets: &[usize], filter: &mut Vec<u8>);

    /// Return `true` if the `key` may have been among the keys for which the `filter` was
    /// generated.
    ///
    /// False positives are permissible, while false negatives are a logical error.
    #[must_use]
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

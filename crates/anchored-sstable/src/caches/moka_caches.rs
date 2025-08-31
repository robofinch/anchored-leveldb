use std::{cell::RefCell, hash::Hash, rc::Rc};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{ConstantTime, MirroredClone, Speed};
use mini_moka::unsync::Cache as UnsyncCache;
use moka::sync::Cache as SyncCache;

use super::KVCache;


pub struct UnsyncMokaCache<Key, Value>(
    pub Rc<RefCell<UnsyncCache<Key, Value>>>,
);

impl<Key, Value> KVCache<Key, Value> for UnsyncMokaCache<Key, Value>
where
    Key:   Eq + Hash,
    Value: MirroredClone<ConstantTime>,
{
    #[inline]
    fn insert(&self, cache_key: Key, value: &Value) {
        self.0.borrow_mut()
            .insert(cache_key, value.mirrored_clone());
    }

    #[inline]
    fn get(&self, cache_key: &Key) -> Option<Value> {
        self.0.borrow_mut()
            .get(cache_key)
            .map(Value::mirrored_clone)
    }

    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        Key:   Debug,
        Value: Debug,
    {
        Debug::fmt(&self, f)
    }
}

impl<Key, Value> Clone for UnsyncMokaCache<Key, Value> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<Key, Value, S: Speed> MirroredClone<S> for UnsyncMokaCache<Key, Value> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<Key, Value> Debug for UnsyncMokaCache<Key, Value>
where
    Key:   Debug + Eq + Hash,
    Value: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("UnsyncMokaCache").field(&self.0).finish()
    }
}

/// The `Value` generic should use an [`Arc`] or similar; the `Clone` implementation
/// of `Value` should be a constant-time operation.
///
/// [`Arc`]: std::sync::Arc
pub struct SyncMokaCache<Key, Value>(pub SyncCache<Key, Value>);

impl<Key, Value> KVCache<Key, Value> for SyncMokaCache<Key, Value>
where
    Key:   Eq + Hash + Send + Sync + 'static,
    Value: MirroredClone<ConstantTime> + Clone + Send + Sync + 'static,
{
    #[inline]
    fn insert(&self, cache_key: Key, value: &Value) {
        self.0.insert(cache_key, value.mirrored_clone());
    }

    #[inline]
    fn get(&self, cache_key: &Key) -> Option<Value> {
        self.0.get(cache_key)
    }

    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        Key:   Debug,
        Value: Debug,
    {
        Debug::fmt(&self, f)
    }
}

impl<Key, Value> Clone for SyncMokaCache<Key, Value> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Key, Value, S: Speed> MirroredClone<S> for SyncMokaCache<Key, Value> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        // `self.clone()` performs 5 atomic operations. Also, the struct is less than 64 bytes.
        self.clone()
    }
}

impl<Key, Value> Debug for SyncMokaCache<Key, Value>
where
    Key:   Debug + Eq + Hash + Send + Sync + 'static,
    Value: Debug + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("SyncMokaCache").field(&self.0).finish()
    }
}

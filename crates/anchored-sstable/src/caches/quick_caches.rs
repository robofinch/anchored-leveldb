use std::{cell::RefCell, hash::Hash, rc::Rc, sync::Arc};
use std::fmt::{Debug, Formatter, Result as FmtResult};

use clone_behavior::{Fast, MirroredClone, Speed};
use quick_cache::{sync::Cache as SyncCache, unsync::Cache as UnsyncCache};

use super::KVCache;


#[derive(Debug)]
pub struct UnsyncQuickCache<Key, Value>(
    pub Rc<RefCell<UnsyncCache<Key, Value>>>,
);

impl<Key, Value> KVCache<Key, Value> for UnsyncQuickCache<Key, Value>
where
    Key:   Eq + Hash,
    Value: MirroredClone<Fast>,
{
    #[inline]
    fn insert(&self, cache_key: Key, value: &Value) {
        self.0.borrow_mut()
            .insert(cache_key, value.fast_mirrored_clone());
    }

    #[inline]
    fn get(&self, cache_key: &Key) -> Option<Value> {
        self.0.borrow_mut()
            .get(cache_key)
            .map(Value::fast_mirrored_clone)
    }

    fn debug(&self, f: &mut Formatter<'_>) -> FmtResult
    where
        Key:   Debug,
        Value: Debug,
    {
        Debug::fmt(&self, f)
    }
}

impl<Key, Value> Clone for UnsyncQuickCache<Key, Value> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<Key, Value, S: Speed> MirroredClone<S> for UnsyncQuickCache<Key, Value> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

/// The `Value` generic should use an [`Arc`] or similar; the `Clone` implementations
/// of both `Value` and `TableCmp` should be constant-time operations.
///
/// [`Arc`]: std::sync::Arc
pub struct SyncQuickCache<Key, Value>(pub Arc<SyncCache<Key, Value>>);

impl<Key, Value> KVCache<Key, Value> for SyncQuickCache<Key, Value>
where
    Key:   Eq + Hash,
    Value: MirroredClone<Fast> + Clone,
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

impl<Key, Value> Clone for SyncQuickCache<Key, Value> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<Key, Value, S: Speed> MirroredClone<S> for SyncQuickCache<Key, Value> {
    #[inline]
    fn mirrored_clone(&self) -> Self {
        // cloning an `Arc` is cheap
        self.clone()
    }
}

impl<Key, Value> Debug for SyncQuickCache<Key, Value> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("SyncMokaCache").field(&self.0).finish()
    }
}

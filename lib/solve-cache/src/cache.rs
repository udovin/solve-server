use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{Cache, Object};

pub struct LruCache<K, V> {
    lru: Arc<Mutex<lru::LruCache<K, Object<V>>>>,
}

impl<K, V> LruCache<K, V>
where
    K: Send + Sync + Clone + std::hash::Hash + std::cmp::Eq + PartialEq + 'static,
    V: Send + Sync + 'static,
{
    pub fn new(cap: NonZeroUsize) -> Self {
        Self {
            lru: Arc::new(Mutex::new(lru::LruCache::new(cap))),
        }
    }
}

impl<K, V> Clone for LruCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            lru: self.lru.clone(),
        }
    }
}

#[async_trait::async_trait]
impl<K, V> Cache for LruCache<K, V>
where
    K: Send + Sync + Clone + std::hash::Hash + std::cmp::Eq + PartialEq + 'static,
    V: Send + Sync + 'static,
{
    type Key = K;
    type Value = V;

    async fn get(&self, key: &Self::Key) -> Option<Object<Self::Value>> {
        let mut lru = self.lru.lock().await;
        lru.get(key).cloned()
    }

    async fn set(&self, key: Self::Key, value: Object<Self::Value>) {
        let mut lru = self.lru.lock().await;
        lru.put(key, value);
    }

    async fn remove(&self, key: &Self::Key) -> Option<Object<Self::Value>> {
        let mut lru: tokio::sync::MutexGuard<lru::LruCache<K, Object<V>>> = self.lru.lock().await;
        lru.pop(key)
    }
}

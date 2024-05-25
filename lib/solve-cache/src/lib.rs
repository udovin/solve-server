mod cache;

pub use cache::*;

use futures::future::Shared;
use futures::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type SharedError = Arc<dyn std::error::Error + Send + Sync + 'static>;

#[async_trait::async_trait]
pub trait Store: Send + Sync + Clone {
    type Key;

    type Value;

    async fn load(&self, key: &Self::Key) -> Result<Self::Value, Error>;

    async fn free(&self, key: &Self::Key, value: Self::Value);
}

#[async_trait::async_trait]
pub trait Cache: Send + Sync + Clone {
    type Key;

    type Value;

    async fn get(&self, key: &Self::Key) -> Option<Object<Self::Value>>;

    async fn set(&self, key: Self::Key, value: Object<Self::Value>);

    async fn remove(&self, key: &Self::Key) -> Option<Object<Self::Value>>;
}

type ObjectFuture<V> = Pin<Box<dyn Future<Output = Result<Object<V>, SharedError>> + Send>>;

pub struct Manager<S, C, K, V>
where
    S: Store<Key = K, Value = V>,
    C: Cache<Key = K, Value = V>,
{
    store: S,
    cache: C,
    futures: Arc<RwLock<HashMap<K, Shared<ObjectFuture<V>>>>>,
}

impl<S, C, K, V> Clone for Manager<S, C, K, V>
where
    S: Store<Key = K, Value = V>,
    C: Cache<Key = K, Value = V>,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            cache: self.cache.clone(),
            futures: self.futures.clone(),
        }
    }
}

struct ObjectInner<V> {
    value: Option<V>,
    free: Option<Box<dyn FnOnce(V) + Send + Sync>>,
}

impl<V> Drop for ObjectInner<V> {
    fn drop(&mut self) {
        self.free.take().unwrap()(self.value.take().unwrap());
    }
}

pub struct Object<V> {
    inner: Arc<ObjectInner<V>>,
}

impl<V> Clone for Object<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<V> std::ops::Deref for Object<V> {
    type Target = V;

    fn deref(&self) -> &V {
        self.inner.value.as_ref().unwrap()
    }
}

impl<S, C, K, V> Manager<S, C, K, V>
where
    S: Store<Key = K, Value = V> + 'static,
    C: Cache<Key = K, Value = V> + 'static,
    K: std::hash::Hash + std::cmp::Eq + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new(store: S, cache: C) -> Self {
        Self {
            futures: Default::default(),
            store,
            cache,
        }
    }

    pub async fn load(&self, key: &K) -> Result<Object<V>, SharedError> {
        if let Some(v) = self.cache.get(key).await {
            return Ok(v);
        }
        {
            let futures = self.futures.read().await;
            if let Some(v) = self.cache.get(key).await {
                return Ok(v);
            }
            if let Some(v) = futures.get(key) {
                let future = v.clone();
                drop(futures);
                return future.await;
            }
        }
        self.reload(key).await
    }

    pub async fn reload(&self, key: &K) -> Result<Object<V>, SharedError> {
        {
            let mut futures = self.futures.write().await;
            match futures.get(key) {
                Some(v) => v.clone(),
                None => {
                    let future = self.load_future(key);
                    futures.insert(key.clone(), future.clone());
                    future
                }
            }
        }
        .await
    }

    pub async fn delete(&self, key: &K) -> Option<Object<V>> {
        self.cache.remove(key).await
    }

    fn load_future(&self, key: &K) -> Shared<ObjectFuture<V>> {
        let free = Box::new({
            let key = key.clone();
            let store = self.store.clone();
            move |value: V| {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(store.free(&key, value))
                })
            }
        });
        let future = {
            let key = key.clone();
            let store = self.store.clone();
            let cache = self.cache.clone();
            let futures = self.futures.clone();
            async move {
                // TODO: Refactor this.
                tokio::task::block_in_place(|| {
                    let handle = tokio::runtime::Handle::current();
                    let result = handle.block_on(store.load(&key));
                    let mut futures = futures.blocking_write();
                    futures.remove(&key);
                    match result {
                        Ok(v) => {
                            let object = Object {
                                inner: Arc::new(ObjectInner {
                                    value: Some(v),
                                    free: Some(Box::new(free)),
                                }),
                            };
                            handle.block_on(cache.set(key, object.clone()));
                            Ok(object)
                        }
                        Err(err) => Err(Arc::from(err)),
                    }
                })
            }
        };
        future.boxed().shared()
    }
}

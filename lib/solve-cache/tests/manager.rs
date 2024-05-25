use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use solve_cache::{Error, LruCache, Manager, Store};
use tokio::sync::{mpsc, Mutex};

struct TestValue(usize);

#[derive(Clone)]
struct TestStore {
    map: Arc<Mutex<HashMap<String, TestValue>>>,
    rx: Arc<Mutex<mpsc::UnboundedReceiver<usize>>>,
    tx: mpsc::UnboundedSender<usize>,
}

impl TestStore {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            map: Default::default(),
            rx: Arc::new(Mutex::new(rx)),
            tx,
        }
    }

    pub fn new_value(&self, value: usize) {
        self.tx.send(value).unwrap();
    }

    pub async fn len(&self) -> usize {
        let map = self.map.lock().await;
        map.len()
    }
}

#[async_trait::async_trait]
impl Store for TestStore {
    type Key = String;

    type Value = TestValue;

    async fn load(&self, key: &Self::Key) -> Result<TestValue, Error> {
        let value = {
            let mut rx = self.rx.lock().await;
            TestValue(rx.recv().await.ok_or("cannot recv value")?)
        };
        let mut map = self.map.lock().await;
        map.insert(key.clone(), TestValue(value.0));
        Ok(value)
    }

    async fn free(&self, key: &Self::Key, _value: TestValue) {
        let mut map = self.map.lock().await;
        map.remove(key).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_manager() {
    let store = TestStore::new();
    let cache = LruCache::new(NonZeroUsize::new(10).unwrap());
    let manager = Manager::new(store.clone(), cache);
    {
        let key = "key1".to_owned();
        store.new_value(111);
        let value1 = manager.load(&key).await.unwrap();
        let value2 = manager.load(&key).await.unwrap();
        assert_eq!(value1.0, 111);
        assert_eq!(value2.0, 111);
        assert_eq!(store.len().await, 1);
        manager.delete(&key).await;
        assert_eq!(store.len().await, 1);
        drop(value1);
        assert_eq!(store.len().await, 1);
        drop(value2);
        assert_eq!(store.len().await, 0);
    }
    {
        let key = "key2".to_owned();
        let value1_fut = tokio::spawn({
            let manager = manager.clone();
            let key = key.clone();
            async move { manager.load(&key).await }
        });
        let value2_fut = tokio::spawn({
            let manager = manager.clone();
            let key = key.clone();
            async move { manager.load(&key).await }
        });
        store.new_value(222);
        let value1 = value1_fut.await.unwrap().unwrap();
        let value2 = value2_fut.await.unwrap().unwrap();
        assert_eq!(value1.0, 222);
        assert_eq!(value2.0, 222);
        assert_eq!(store.len().await, 1);
        manager.delete(&key).await;
        assert_eq!(store.len().await, 1);
        drop(value1);
        assert_eq!(store.len().await, 1);
        drop(value2);
        assert_eq!(store.len().await, 0);
    }
}

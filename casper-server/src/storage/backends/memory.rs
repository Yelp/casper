use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use actix_http::{Response, StatusCode};
use async_trait::async_trait;
use bytes::Bytes;
use linked_hash_map::LinkedHashMap;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::storage::{decode_headers, encode_headers, Body, Item, ItemKey, Key, Storage};

// Memory backend configuration
#[derive(Deserialize)]
pub struct Config {
    /// Store up to `max_size` bytes (soft limit)
    pub max_size: usize,
}

struct Value {
    status: StatusCode,
    headers: Vec<u8>,
    body: Bytes,
    expires: SystemTime,
    surrogate_keys: Vec<Key>,
}

impl Value {
    /// Calculates size (in bytes) of this Value
    fn size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        size += self.headers.len() + self.body.len();
        for sk in &self.surrogate_keys {
            size += sk.len();
        }
        size
    }
}

#[derive(Clone)]
pub struct MemoryBackend {
    name: String,
    inner: Arc<Mutex<MemoryBackendImpl>>,
}

impl MemoryBackend {
    pub fn new(config: &Config, name: impl Into<Option<String>>) -> Self {
        let name = name.into().unwrap_or_else(|| "memory".to_string());
        let inner = Arc::new(Mutex::new(MemoryBackendImpl::new(config.max_size)));
        MemoryBackend { name, inner }
    }
}

struct MemoryBackendImpl {
    max_size: usize,
    size: usize,
    cache: LinkedHashMap<Key, Value>,
    index: HashMap<Key, HashSet<Key>>,
}

impl MemoryBackendImpl {
    /// Creates a new instance that can hold up to `max_size` bytes (soft limit)
    pub fn new(max_size: usize) -> Self {
        MemoryBackendImpl {
            max_size,
            size: 0,
            cache: LinkedHashMap::new(),
            index: HashMap::new(),
        }
    }

    /// Inserts key/value to the cache while maintaining `max_size`
    pub fn insert(&mut self, key: Key, val: Value) {
        // Ensure that we have free space to store the value
        while !self.cache.is_empty() && self.size + val.size() > self.max_size {
            self.pop_lru();
        }

        // Update index first
        for sk in &val.surrogate_keys {
            self.index
                .entry(sk.clone())
                .or_default()
                .insert(key.clone());
        }

        // Then insert the value
        self.size += val.size();
        self.cache.insert(key, val);
    }

    /// Removes least recently used value from the cache
    fn pop_lru(&mut self) -> Option<(Key, Value)> {
        if let Some((key, value)) = self.cache.pop_front() {
            for sk in &value.surrogate_keys {
                if let Some(sv) = self.index.get_mut(sk) {
                    sv.remove(&key);
                }
            }
            self.size -= value.size();
            return Some((key, value));
        }
        None
    }

    /// Returns unexpired value from the cache
    fn get_unexpired(&mut self, key: &Key) -> Option<&Value> {
        match self.cache.get_refresh(key) {
            Some(value) if value.expires > SystemTime::now() => self.cache.get(key),
            Some(value) if value.expires <= SystemTime::now() => {
                self.remove(key);
                None
            }
            _ => None,
        }
    }

    /// Removes value from the cache by `key`
    fn remove(&mut self, key: &Key) -> Option<Value> {
        if let Some(value) = self.cache.remove(key) {
            for sk in &value.surrogate_keys {
                if let Some(sv) = self.index.get_mut(sk) {
                    sv.remove(key);
                }
            }
            self.size -= value.size();
            return Some(value);
        }
        None
    }

    /// Removes all values from the cache that have the same surrogate key
    fn remove_by_skey(&mut self, sk: &Key) {
        if let Some(set) = self.index.remove(sk) {
            for key in set {
                if let Some(val) = self.cache.remove(&key) {
                    self.size -= val.size();
                }
            }
        }
    }
}

#[async_trait(?Send)]
impl Storage for MemoryBackend {
    type Body = Body;
    type Error = anyhow::Error;

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error> {
        self.get_responses([key]).await.remove(0)
    }

    async fn get_responses<KI>(
        &self,
        keys: KI,
    ) -> Vec<Result<Option<Response<Self::Body>>, Self::Error>>
    where
        KI: IntoIterator<Item = Key>,
    {
        let mut memory = self.inner.lock().await;
        let mut responses = Vec::new();
        for key in keys {
            let resp = memory
                .get_unexpired(&key)
                .map(|value| {
                    let headers = decode_headers(&value.headers)?;
                    let body = Body::left(value.body.clone());

                    let mut resp = Response::with_body(value.status, body);
                    *resp.headers_mut() = headers;

                    Ok::<_, Self::Error>(resp)
                })
                .transpose();
            responses.push(resp);
        }
        responses
    }

    async fn delete_responses(&self, key: ItemKey) -> Result<(), Self::Error> {
        self.delete_responses_multi([key]).await.remove(0)
    }

    async fn delete_responses_multi<KI>(&self, keys: KI) -> Vec<Result<(), Self::Error>>
    where
        KI: IntoIterator<Item = ItemKey>,
    {
        let mut memory = self.inner.lock().await;
        let mut results = Vec::new();
        for key in keys {
            match key {
                ItemKey::Primary(key) => {
                    memory.remove(&key);
                }
                ItemKey::Surrogate(sk) => {
                    memory.remove_by_skey(&sk);
                }
            }
            results.push(Ok(()));
        }
        results
    }

    async fn store_response<'a>(&self, item: Item<'a>) -> Result<(), Self::Error> {
        self.store_responses([item]).await.remove(0)
    }

    async fn store_responses<'a, I>(&self, items: I) -> Vec<Result<(), Self::Error>>
    where
        I: IntoIterator<Item = Item<'a>>,
    {
        let mut memory = self.inner.lock().await;
        let mut results = Vec::new();
        for item in items {
            let result = (|| {
                let value = Value {
                    status: item.status,
                    headers: encode_headers(&item.headers)?,
                    body: item.body,
                    expires: SystemTime::now() + item.ttl,
                    surrogate_keys: item.surrogate_keys,
                };
                memory.insert(item.key, value);
                Ok(())
            })();
            results.push(result);
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actix_http::body::to_bytes;
    use actix_http::header::{HeaderName, HeaderValue};
    use actix_http::Response;
    use bytes::Bytes;

    use super::{Config, MemoryBackend};
    use crate::storage::{Item, ItemKey, Storage};

    fn make_response(body: impl Into<Bytes>) -> Response<Bytes> {
        Response::ok().set_body(body.into())
    }

    #[actix_web::test]
    async fn test_backend() {
        let memory = MemoryBackend::new(&Config { max_size: 1024 }, None);
        let mut resp = make_response("hello, world");

        resp.headers_mut().insert(
            HeaderName::from_static("hello"),
            HeaderValue::from_static("World"),
        );

        // Cache response
        let ttl = Duration::from_secs(1);
        memory
            .store_response(Item::new("key1", resp, ttl))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = memory.get_response("key1".into()).await.unwrap().unwrap();
        assert_eq!(
            resp.headers().get("Hello"),
            Some(&HeaderValue::from_static("World"))
        );
        let body = to_bytes(resp.into_body()).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");

        // Delete cached response
        memory
            .delete_responses(ItemKey::Primary("key1".into()))
            .await
            .unwrap();

        // Try to fetch it back
        let resp = memory.get_response("key1".into()).await.unwrap();
        assert!(matches!(resp, None));
    }

    #[actix_web::test]
    async fn test_backend_ttl() {
        let memory = MemoryBackend::new(&Config { max_size: 1024 }, None);
        let mut resp = make_response("hello, world");

        resp.headers_mut().insert(
            HeaderName::from_static("hello"),
            HeaderValue::from_static("World"),
        );

        // Cache response with TTL
        let ttl = Duration::from_millis(10);
        memory
            .store_response(Item::new("key2", resp, ttl))
            .await
            .unwrap();

        // Sleep to expire cached item
        tokio::time::sleep(ttl).await;

        // Try to fetch it back
        let resp = memory.get_response("key2".into()).await.unwrap();
        assert!(matches!(resp, None));
    }

    #[actix_web::test]
    async fn test_surrogate_keys() {
        let memory = MemoryBackend::new(&Config { max_size: 1024 }, None);
        let resp = make_response("hello, world");

        let surrogate_keys = vec!["abc"];
        let ttl = Duration::from_secs(1);
        memory
            .store_response(Item::new_with_skeys("key1", resp, surrogate_keys, ttl))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = memory.get_response("key1".into()).await.unwrap().unwrap();
        let body = to_bytes(resp.into_body()).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");

        // Delete by surrogate key
        memory
            .delete_responses(ItemKey::Surrogate("abc".into()))
            .await
            .unwrap();

        // Try to fetch it back
        let resp = memory.get_response("key1".into()).await.unwrap();
        assert!(matches!(resp, None));
    }
}

use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;

use async_trait::async_trait;
use http::{HeaderMap, Response, StatusCode};
use linked_hash_map::LinkedHashMap;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::storage::{Item, ItemKey, Storage};

// Memory backend configuration
#[derive(Deserialize)]
pub struct Config {
    /// Store up to `max_size` bytes (soft limit)
    max_size: usize,
}

type Key = Vec<u8>;

struct Value {
    status: StatusCode,
    headers: Vec<u8>,
    body: Vec<u8>,
    expires: Option<SystemTime>,
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

pub struct MemoryBackend(Mutex<MemoryBackendImpl>);

impl MemoryBackend {
    pub fn new(config: &Config) -> Self {
        MemoryBackend(Mutex::new(MemoryBackendImpl::new(config.max_size)))
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
    fn new(max_size: usize) -> Self {
        MemoryBackendImpl {
            max_size,
            size: 0,
            cache: LinkedHashMap::new(),
            index: HashMap::new(),
        }
    }

    /// Inserts key/value to the cache while maintaining `max_size`
    fn insert(&mut self, key: Key, val: Value) {
        // Ensure that we have free space to store the value
        while self.cache.len() > 0 && self.size + val.size() > self.max_size {
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
    fn get_unexpired(&mut self, key: &[u8]) -> Option<&Value> {
        match self.cache.get_refresh(key) {
            Some(value) if value.expires.is_none() || value.expires > Some(SystemTime::now()) => {
                self.cache.get(key)
            }
            Some(value) if value.expires <= Some(SystemTime::now()) => {
                self.remove(key);
                None
            }
            _ => None,
        }
    }

    /// Removes value from the cache by `key`
    fn remove(&mut self, key: &[u8]) -> Option<Value> {
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
    fn remove_by_skey(&mut self, sk: &[u8]) {
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
    type Body = hyper::Body;
    type Error = anyhow::Error;

    async fn get_responses<K, KI>(
        &self,
        keys: KI,
    ) -> Result<Vec<Option<Response<Self::Body>>>, Self::Error>
    where
        KI: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let mut result = Vec::new();

        let mut memory = self.0.lock().await;
        for key in keys {
            let resp = memory
                .get_unexpired(key.as_ref())
                .map(|value| {
                    let headers: hyper_serde::De<HeaderMap> =
                        flexbuffers::from_slice(&value.headers)?;
                    let body = hyper::Body::from(value.body.clone());

                    let mut resp = Response::new(body);
                    *resp.status_mut() = value.status;
                    *resp.headers_mut() = headers.into_inner();

                    Ok::<_, Self::Error>(resp)
                })
                .transpose()?;
            result.push(resp);
        }

        Ok(result)
    }

    async fn delete_responses<K, KI>(&self, keys: KI) -> Result<(), Self::Error>
    where
        KI: IntoIterator<Item = ItemKey<K>>,
        K: AsRef<[u8]>,
    {
        let mut memory = self.0.lock().await;
        for key in keys {
            match key {
                ItemKey::Primary(key) => {
                    memory.remove(key.as_ref());
                }
                ItemKey::Surrogate(sk) => {
                    memory.remove_by_skey(sk.as_ref());
                }
            }
        }
        Ok(())
    }

    async fn cache_responses<K, R, SK, I>(&self, items: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = Item<K, R, SK>>,
        K: AsRef<[u8]>,
        R: BorrowMut<Response<Self::Body>>,
        SK: AsRef<[u8]>,
    {
        let mut memory = self.0.lock().await;
        for mut it in items {
            let resp = it.response.borrow_mut();
            let value = Value {
                status: resp.status(),
                headers: flexbuffers::to_vec(&hyper_serde::Ser::new(resp.headers()))?,
                // Likely body already has been read concurrently in Lua and now available as a byte array
                body: hyper::body::to_bytes(resp.body_mut()).await?.to_vec(),
                expires: it.ttl.map(|ttl| SystemTime::now() + ttl),
                surrogate_keys: it
                    .surrogate_keys
                    .into_iter()
                    .map(|x| x.as_ref().to_vec())
                    .collect(),
            };
            memory.insert(it.key.as_ref().to_vec(), value);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::time::Duration;

    use http::HeaderValue;
    use hyper::{Body, Response};

    use crate::backends::memory::Config;
    use crate::backends::MemoryBackend;
    use crate::storage::{Item, ItemKey, Storage};

    fn make_response<B: ToString + ?Sized>(body: &B) -> Response<Body> {
        Response::builder()
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn test_backend() {
        let memory = MemoryBackend::new(&Config { max_size: 1024 });
        let mut resp = make_response("hello, world");

        resp.headers_mut()
            .insert("Hello", "World".try_into().unwrap());

        // Cache response without TTL
        memory
            .cache_response(Item::new("key1", resp, None))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = memory.get_response("key1").await.unwrap().unwrap();
        assert_eq!(
            resp.headers().get("Hello"),
            Some(&HeaderValue::from_static("World"))
        );
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");

        // Delete cached response
        memory.delete_response("key1").await.unwrap();

        // Try to fetch it back
        let resp = memory.get_response("key1").await.unwrap();
        assert!(matches!(resp, None));
    }

    #[tokio::test]
    async fn test_backend_ttl() {
        let memory = MemoryBackend::new(&Config { max_size: 1024 });
        let mut resp = make_response("hello, world");

        resp.headers_mut()
            .insert("Hello", "World".try_into().unwrap());

        // Cache response with TTL
        let ttl = Duration::from_millis(10);
        memory
            .cache_response(Item::new("key2", resp, Some(ttl)))
            .await
            .unwrap();

        // Sleep to expire cached item
        tokio::time::sleep(ttl).await;

        // Try to fetch it back
        let resp = memory.get_response("key2").await.unwrap();
        assert!(matches!(resp, None));
    }

    #[tokio::test]
    async fn test_surrogate_keys() {
        let memory = MemoryBackend::new(&Config { max_size: 1024 });
        let resp = make_response("hello, world");

        let surrogate_keys = vec!["abc"];
        memory
            .cache_response(Item::new_with_skeys("key1", resp, surrogate_keys, None))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = memory.get_response("key1").await.unwrap().unwrap();
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");

        // Delete by surrogate key
        memory
            .delete_responses([ItemKey::Surrogate("abc")])
            .await
            .unwrap();

        // Try to fetch it back
        let resp = memory.get_response("key1").await.unwrap();
        assert!(matches!(resp, None));
    }
}
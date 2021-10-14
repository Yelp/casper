use std::borrow::BorrowMut;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use http::{HeaderMap, Response, StatusCode};
use lru_time_cache::LruCache;
use tokio::sync::Mutex;

use crate::storage::Storage;

struct Value {
    status: StatusCode,
    headers: Vec<u8>,
    body: Vec<u8>,
    expires: Option<SystemTime>,
}

pub struct MemoryBackend {
    cache: Mutex<LruCache<Vec<u8>, Value>>,
}

impl MemoryBackend {
    pub fn new(capacity: usize) -> Self {
        MemoryBackend {
            cache: Mutex::new(LruCache::with_capacity(capacity)),
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
        K: AsRef<[u8]>,
        KI: IntoIterator<Item = K>,
    {
        let mut result = Vec::new();

        let mut cache = self.cache.lock().await;
        for key in keys {
            let key = key.as_ref();
            let resp = match cache.get(key) {
                // We interested in non-expired items
                Some(value)
                    if value.expires.is_none() || value.expires > Some(SystemTime::now()) =>
                {
                    let headers: hyper_serde::De<HeaderMap> =
                        flexbuffers::from_slice(&value.headers).unwrap();
                    let body = hyper::Body::from(value.body.clone());

                    let mut resp = Response::new(body);
                    *resp.status_mut() = value.status;
                    *resp.headers_mut() = headers.into_inner();

                    Some(resp)
                }
                Some(value) if value.expires <= Some(SystemTime::now()) => {
                    cache.remove(key);
                    None
                }
                _ => None,
            };
            result.push(resp);
        }

        Ok(result)
    }

    async fn delete_responses<K, KI>(&self, keys: KI) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]>,
        KI: IntoIterator<Item = K>,
    {
        let mut cache = self.cache.lock().await;
        for key in keys {
            cache.remove(key.as_ref());
        }
        Ok(())
    }

    async fn cache_responses<K, R, I>(&self, items: I) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]>,
        R: BorrowMut<Response<Self::Body>>,
        I: IntoIterator<Item = (K, R, Option<Duration>)>,
    {
        let mut cache = self.cache.lock().await;
        for (key, mut resp, ttl) in items {
            let resp = resp.borrow_mut();
            let value = Value {
                status: resp.status(),
                headers: flexbuffers::to_vec(&hyper_serde::Ser::new(resp.headers()))?,
                body: hyper::body::to_bytes(resp.body_mut()).await?.to_vec(),
                expires: ttl.map(|ttl| SystemTime::now() + ttl),
            };
            cache.insert(key.as_ref().to_vec(), value);
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

    use crate::backends::MemoryBackend;
    use crate::storage::Storage;

    fn make_response<B: ToString + ?Sized>(body: &B) -> Response<Body> {
        Response::builder()
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn test_memory_backend() {
        let memory = MemoryBackend::new(100);
        let mut resp = make_response("hello, world");

        resp.headers_mut()
            .insert("Hello", "World".try_into().unwrap());

        // Cache response without TTL
        memory.cache_response("key1", resp, None).await.unwrap();

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
    async fn test_memory_backend_ttl() {
        let memory = MemoryBackend::new(100);
        let mut resp = make_response("hello, world");

        resp.headers_mut()
            .insert("Hello", "World".try_into().unwrap());

        // Cache response with TTL
        let ttl = Duration::from_millis(10);
        memory
            .cache_response("key2", resp, Some(ttl))
            .await
            .unwrap();

        // Sleep to expire cached item
        tokio::time::sleep(ttl).await;

        // Try to fetch it back
        let resp = memory.get_response("key2").await.unwrap();
        assert!(matches!(resp, None));
    }
}

use std::borrow::BorrowMut;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bstr::ByteVec;
use bytes::{BufMut, Bytes};
use fred::pool::StaticRedisPool;
use fred::prelude::{Expiration, MultipleKeys, RedisKey, RedisValue, SetOptions};
use futures::{
    future::try_join_all,
    stream::{self, StreamExt},
};
use hyper::{HeaderMap, Response, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use crate::storage::{Item, ItemKey, Key, Storage};

pub struct RedisBackend {
    config: Config,
    client: StaticRedisPool,
}

// Redis backend configuration
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub enable_tls: bool,

    pub username: Option<String>,
    pub password: Option<String>,

    // TODO: Support reconnect policy
    #[serde(default)]
    pub timeouts: TimeoutConfig,

    #[serde(default = "Config::default_pool_size")]
    pub pool_size: usize,

    #[serde(default = "Config::default_max_body_chunk_size")]
    pub max_body_chunk_size: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub enum ServerConfig {
    Centralized { host: String, port: u16 },
    Clustered { hosts: Vec<(String, u16)> },
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig::Centralized {
            host: "127.0.0.1".into(),
            port: 6379,
        }
    }
}

impl ServerConfig {
    fn into_redis_server_config(self) -> fred::types::ServerConfig {
        match self {
            ServerConfig::Centralized { host, port } => {
                fred::types::ServerConfig::Centralized { host, port }
            }
            ServerConfig::Clustered { hosts } => fred::types::ServerConfig::Clustered { hosts },
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct TimeoutConfig {
    /// A limit on the amount of time an application can take to make initial connection to Redis
    #[serde(default = "TimeoutConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// A limit on the amount of time an application can take to fetch response or next chunk from Redis
    #[serde(default = "TimeoutConfig::default_fetch_timeout_ms")]
    pub fetch_timeout_ms: u64,

    /// A limit on the amount of time an application can take to store response in Redis
    #[serde(default = "TimeoutConfig::default_store_timeout_ms")]
    pub store_timeout_ms: u64,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        TimeoutConfig {
            connect_timeout_ms: TimeoutConfig::default_connect_timeout_ms(),
            fetch_timeout_ms: TimeoutConfig::default_fetch_timeout_ms(),
            store_timeout_ms: TimeoutConfig::default_store_timeout_ms(),
        }
    }
}

impl TimeoutConfig {
    const fn default_connect_timeout_ms() -> u64 {
        10000
    }

    const fn default_fetch_timeout_ms() -> u64 {
        10000
    }

    const fn default_store_timeout_ms() -> u64 {
        10000
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig::default(),
            enable_tls: false,
            username: None,
            password: None,
            timeouts: TimeoutConfig::default(),
            pool_size: Config::default_pool_size(),
            max_body_chunk_size: Config::default_max_body_chunk_size(),
        }
    }
}

impl Config {
    fn default_pool_size() -> usize {
        num_cpus::get()
    }

    const fn default_max_body_chunk_size() -> usize {
        1024 * 1024 // 1 MB
    }

    fn into_redis_config(self) -> fred::types::RedisConfig {
        fred::types::RedisConfig {
            username: self.username,
            password: self.password,
            server: self.server.into_redis_server_config(),
            tls: if self.enable_tls {
                Some(fred::types::TlsConfig::default())
            } else {
                None
            },
            ..Default::default()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ResponseItem {
    #[serde(with = "hyper_serde")]
    headers: HeaderMap,
    status_code: u16,
    timestamp: u64,
    surrogate_keys: Vec<Key>,
    body: Bytes,
    num_chunks: u32,
    // flags: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SurrogateKeyItem {
    timestamp: u64,
}

impl RedisBackend {
    #[allow(unused)]
    pub async fn new(config: Config) -> Result<Self> {
        let policy = fred::types::ReconnectPolicy::default();
        let pool_size = config.pool_size;

        let pool = StaticRedisPool::new(config.clone().into_redis_config(), pool_size)?;
        let _ = pool.connect(Some(policy));

        let connect_timeout = Duration::from_millis(config.timeouts.connect_timeout_ms);
        timeout(connect_timeout, pool.wait_for_connect())
            .await
            .map_err(anyhow::Error::new)
            .and_then(|r| r.map_err(anyhow::Error::new))
            .context("Failed to connect to Redis")?;

        Ok(RedisBackend {
            config,
            client: pool,
        })
    }

    async fn get_response_inner(&self, key: Key) -> Result<Option<Response<hyper::Body>>> {
        // Fetch response item
        let res: Option<Vec<u8>> = self.client.get(RedisKey::new(key.clone())).await?;
        let response_item: ResponseItem = match res {
            Some(res) => flexbuffers::from_slice(&res)?,
            None => return Ok(None),
        };

        // Fetch surrogate keys
        if !response_item.surrogate_keys.is_empty() {
            let skeys = response_item
                .surrogate_keys
                .iter()
                .map(|sk| RedisKey::new(sk.clone()))
                .collect::<MultipleKeys>();

            let skeys_vals = match self.client.mget(skeys).await? {
                RedisValue::Array(vals) => vals,
                val => vec![val],
            };
            for (_sk, sk_value) in response_item.surrogate_keys.into_iter().zip(skeys_vals) {
                if let Some(sk_data) = sk_value.as_bytes() {
                    let sk_item: SurrogateKeyItem = flexbuffers::from_slice(sk_data)?;
                    // Check that the response item having this key is not expired
                    if response_item.timestamp <= sk_item.timestamp {
                        return Ok(None);
                    }
                } else {
                    // If one of the keys is missing then we cannot proceed
                    // Probably the missing key was evicted
                    return Ok(None);
                }
            }
        }

        // Make body stream to fetch chunks from Redis
        let num_chunks = response_item.num_chunks as usize;
        // First chunk is stored in the response item, skip it
        let chunks_stream = stream::iter(vec![(self.client.clone(), key); num_chunks - 1])
            .enumerate()
            .then(move |(i, (client, key))| async move {
                let chunk_key = make_chunk_key(&key, i as u32 + 1);
                match client.get::<Option<Vec<u8>>, _>(chunk_key).await? {
                    Some(data) => anyhow::Ok(Bytes::from(data)),
                    None => bail!("Cannot find chunk {}/{}", i + 2, num_chunks),
                }
            });
        let body = hyper::Body::wrap_stream(
            stream::iter(vec![anyhow::Ok(response_item.body)]).chain(chunks_stream),
        );

        // Construct a response object
        let mut resp = Response::new(body);
        *resp.status_mut() = StatusCode::from_u16(response_item.status_code)?;
        *resp.headers_mut() = response_item.headers;

        Ok(Some(resp))
    }

    async fn delete_responses_inner(&self, key: ItemKey) -> Result<()> {
        match key {
            ItemKey::Primary(key) => Ok(self.client.del(RedisKey::new(key)).await?),
            ItemKey::Surrogate(skey) => {
                let sk_item = SurrogateKeyItem {
                    timestamp: unix_timestamp(),
                };
                let sk_item_enc = flexbuffers::to_vec(&sk_item)?;

                Ok(self
                    .client
                    .set(
                        RedisKey::new(skey),
                        RedisValue::Bytes(sk_item_enc),
                        Some(Expiration::KEEPTTL), // Retain the TTL associated with the key
                        Some(SetOptions::XX),      // Only set the key if it already exist
                        false,
                    )
                    .await?)
            }
        }
    }

    async fn store_response_inner(
        &self,
        key: Key,
        response: &mut Response<hyper::Body>,
        surrogate_keys: Vec<Key>,
        ttl: Duration,
    ) -> Result<()> {
        // Get the response body
        let mut body = hyper::body::to_bytes(response.body_mut()).await?;

        // Split body to chunks and save chunks first
        let max_chunk_size = self.config.max_body_chunk_size;
        let mut num_chunks = 1;
        if max_chunk_size > 0 && body.len() > max_chunk_size {
            let body_tail = body.split_off(max_chunk_size);
            for (i, chunk) in body_tail.chunks(max_chunk_size).enumerate() {
                num_chunks += 1;
                // Store chunk in Redis
                self.client
                    .set(
                        make_chunk_key(&key, i as u32 + 1),
                        RedisValue::Bytes(chunk.to_vec()),
                        Some(Expiration::EX(ttl.as_secs() as i64)),
                        None,
                        false,
                    )
                    .await?;
            }
        }

        let response_item = ResponseItem {
            headers: response.headers().clone(),
            status_code: response.status().as_u16(),
            timestamp: unix_timestamp(),
            surrogate_keys: surrogate_keys.clone(),
            body,
            num_chunks,
        };
        let response_item_enc = flexbuffers::to_vec(&response_item)?;

        // Store response item
        self.client
            .set(
                RedisKey::new(key),
                RedisValue::Bytes(response_item_enc),
                Some(Expiration::EX(ttl.as_secs() as i64)),
                None,
                false,
            )
            .await?;

        // Update surrogate keys
        try_join_all(surrogate_keys.into_iter().map(|skey| async move {
            let sk_item = SurrogateKeyItem { timestamp: 0 };
            let sk_item_enc = flexbuffers::to_vec(&sk_item)?;

            // Store surrogate key only if it does not exist (NX option)
            let is_exists: RedisValue = self
                .client
                .set(
                    RedisKey::new(skey.clone()),
                    RedisValue::Bytes(sk_item_enc),
                    Some(Expiration::EX(86400)), // 24 hours
                    Some(SetOptions::NX),
                    false,
                )
                .await?;

            // In case the key already exist, reset TTL to 24h
            if is_exists.is_null() {
                self.client.expire(RedisKey::new(skey), 86400).await?;
            }

            anyhow::Ok(())
        }))
        .await?;

        Ok(())
    }

    fn get_fetch_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timeouts.fetch_timeout_ms)
    }

    fn get_store_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timeouts.store_timeout_ms)
    }
}

#[async_trait]
impl Storage for RedisBackend {
    type Body = hyper::Body;
    type Error = anyhow::Error;

    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error> {
        let fetch_timeout = self.get_fetch_timeout();
        timeout(fetch_timeout, self.get_response_inner(key.clone()))
            .await
            .map_err(anyhow::Error::new)
            .and_then(|x| x)
            .with_context(|| format!("Failed to fetch Response for key {}", hex::encode(key)))
    }

    async fn delete_responses(&self, key: ItemKey) -> Result<(), Self::Error> {
        let store_timeout = self.get_store_timeout();
        timeout(store_timeout, self.delete_responses_inner(key.clone()))
            .await
            .map_err(anyhow::Error::new)
            .and_then(|x| x)
            .with_context(|| format!("Failed to delete Response(s) for key {}", key))
    }

    async fn store_response<R>(&self, mut item: Item<R>) -> Result<(), Self::Error>
    where
        R: BorrowMut<Response<Self::Body>> + Send,
    {
        let key = item.key.clone();
        let response = item.response.borrow_mut();
        let store_timeout = self.get_store_timeout();
        timeout(
            store_timeout,
            self.store_response_inner(item.key, response, item.surrogate_keys, item.ttl),
        )
        .await
        .map_err(anyhow::Error::new)
        .and_then(|x| x)
        .with_context(|| format!("Failed to store Response with key {}", key))
    }
}

#[inline]
fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time is before UNIX_EPOCH")
        .as_secs()
}

#[inline]
fn make_chunk_key(key: &Key, n: u32) -> RedisKey {
    let mut key2 = Key::from("{");
    key2.extend_from_slice(key);
    key2.push_char('}');
    key2.push_char('|');
    key2.put_u32(n);
    RedisKey::new(key2)
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::time::Duration;

    use http::HeaderValue;
    use hyper::{Body, Response};

    use crate::backends::redis::{Config, RedisBackend};
    use crate::storage::{Item, ItemKey, Storage};

    fn make_response<B: ToString + ?Sized>(body: &B) -> Response<Body> {
        Response::builder()
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn test_backend() {
        let backend = RedisBackend::new(Config::default()).await.unwrap();

        let mut resp = make_response("hello, world");
        resp.headers_mut()
            .insert("Hello", "World".try_into().unwrap());

        // Cache response
        backend
            .store_response(Item::new("key1", resp, Duration::from_secs(3)))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response("key1".into()).await.unwrap().unwrap();
        assert_eq!(
            resp.headers().get("Hello"),
            Some(&HeaderValue::from_static("World"))
        );
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");

        // Delete cached response
        backend
            .delete_responses(ItemKey::Primary("key1".into()))
            .await
            .unwrap();

        // Try to fetch it back
        let resp = backend.get_response("key1".into()).await.unwrap();
        assert!(matches!(resp, None));
    }

    #[tokio::test]
    async fn test_chunked_body() {
        let mut config = Config::default();
        config.max_body_chunk_size = 2; // Set max chunk size to 2 bytes
        let backend = RedisBackend::new(config).await.unwrap();

        // Cache response
        let resp = make_response("hello, world");
        backend
            .store_response(Item::new("key2", resp, Duration::from_secs(3)))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response("key2".into()).await.unwrap().unwrap();
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");
    }

    #[tokio::test]
    async fn test_surrogate_keys() {
        let backend = RedisBackend::new(Config::default()).await.unwrap();

        // Cache response
        let resp = make_response("hello, world");
        backend
            .store_response(Item::new_with_skeys(
                "key3",
                resp,
                vec!["abc", "def"],
                Duration::from_secs(3),
            ))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response("key3".into()).await.unwrap().unwrap();
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");

        // Delete by surrogate key
        backend
            .delete_responses(ItemKey::Surrogate("def".into()))
            .await
            .unwrap();

        // Try to fetch it back
        let resp = backend.get_response("key3".into()).await.unwrap();
        assert!(matches!(resp, None));
    }
}

use std::borrow::BorrowMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bitflags::bitflags;
use bytes::Bytes;
use fred::pool::StaticRedisPool;
use fred::prelude::{Expiration, RedisError, RedisKey, RedisValue, SetOptions, Stats};
use futures::future::{try_join, try_join_all, TryFutureExt};
use futures::stream::{self, StreamExt};
use hyper::{Response, StatusCode};
use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::error;

use super::common::{compress_with_zstd, decode_headers, encode_headers};
use crate::storage::{Item, ItemKey, Key, Storage};
use crate::utils::zstd::ZstdDecoder;

pub const MAX_CONCURRENCY: usize = 100;

pub struct RedisBackend {
    name: String,
    config: Config,
    client: StaticRedisPool,
    connected: AtomicBool,
    internal_cache: Mutex<InternalCache>,
}

#[derive(Default)]
struct InternalCache {
    // Surrogate keys cache
    map: LinkedHashMap<Key, (SurrogateKeyItem, Instant)>,
    // Map (approx.) size in bytes
    size: usize,
}

// Redis backend configuration
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
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
    pub compression_level: Option<i32>,
    pub wait_for_connect: Option<f32>,
    #[serde(default)]
    pub lazy: bool,

    #[serde(default = "Config::default_internal_cache_size")]
    pub internal_cache_size: usize,
    #[serde(default = "Config::default_internal_cache_ttl")]
    pub internal_cache_ttl: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub enum ServerConfig {
    #[serde(rename = "centralized")]
    Centralized { endpoint: String },
    #[serde(rename = "clustered")]
    Clustered { endpoints: Vec<String> },
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig::Centralized {
            endpoint: "127.0.0.1".into(),
        }
    }
}

impl ServerConfig {
    fn into_redis_server_config(self) -> Result<fred::types::ServerConfig> {
        match self {
            ServerConfig::Centralized { endpoint } => {
                let (host, port) = parse_host_port(&endpoint)
                    .with_context(|| format!("invalid redis endpoint `{endpoint}`"))?;
                Ok(fred::types::ServerConfig::Centralized { host, port })
            }
            ServerConfig::Clustered { endpoints } => {
                let mut hosts = Vec::new();
                for endpoint in endpoints {
                    let (host, port) = parse_host_port(&endpoint)
                        .with_context(|| format!("invalid redis endpoint `{endpoint}`"))?;
                    hosts.push((host, port));
                }
                Ok(fred::types::ServerConfig::Clustered { hosts })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct TimeoutConfig {
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
            fetch_timeout_ms: TimeoutConfig::default_fetch_timeout_ms(),
            store_timeout_ms: TimeoutConfig::default_store_timeout_ms(),
        }
    }
}

impl TimeoutConfig {
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
            compression_level: None,
            wait_for_connect: None,
            lazy: false,
            internal_cache_size: Config::default_internal_cache_size(),
            internal_cache_ttl: Config::default_internal_cache_ttl(),
        }
    }
}

impl Config {
    fn default_pool_size() -> usize {
        8 * num_cpus::get()
    }

    const fn default_max_body_chunk_size() -> usize {
        1024 * 1024 // 1 MB
    }

    const fn default_internal_cache_size() -> usize {
        32 * 1024 * 1024 // 32 MB
    }

    const fn default_internal_cache_ttl() -> f64 {
        1.0
    }

    fn into_redis_config(self) -> Result<fred::types::RedisConfig> {
        Ok(fred::types::RedisConfig {
            username: self.username,
            password: self.password,
            server: self.server.into_redis_server_config()?,
            tls: if self.enable_tls {
                Some(fred::types::TlsConfig::default())
            } else {
                None
            },
            ..Default::default()
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ResponseItem {
    #[serde(with = "serde_bytes")]
    headers: Vec<u8>,
    status_code: u16,
    timestamp: u64,
    surrogate_keys: Vec<Key>,
    body: Bytes,
    num_chunks: u32,
    flags: Flags,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct SurrogateKeyItem {
    timestamp: u64,
}

bitflags! {
    #[derive(Serialize, Deserialize)]
    pub struct Flags: u32 {
        const NONE = 0b0;
        const COMPRESSED = 0b1;
    }
}

impl RedisBackend {
    pub async fn new(config: Config, name: impl Into<Option<String>>) -> Result<Self> {
        let pool_size = config.pool_size;

        let redis_config = config.clone().into_redis_config()?;
        let pool =
            StaticRedisPool::new(redis_config, pool_size).expect("Failed to create Redis pool");

        let backend = RedisBackend {
            name: name.into().unwrap_or_else(|| "redis".to_string()),
            config: config.clone(),
            client: pool,
            connected: AtomicBool::new(false),
            internal_cache: Mutex::default(),
        };

        if !config.lazy {
            backend.ensure_connected();
            if let Err(err) = backend.wait_for_connect().await {
                error!("{:#}", err.context("Failed to connect to Redis"));
            }
        }

        Ok(backend)
    }

    #[inline]
    pub fn ensure_connected(&self) {
        if !self.connected.swap(true, Ordering::Relaxed) {
            let policy = fred::types::ReconnectPolicy::default();
            let _ = self.client.connect(Some(policy));
        }
    }

    pub async fn wait_for_connect(&self) -> Result<()> {
        match self.config.wait_for_connect {
            Some(secs) if secs > 0.0 => {
                let dur = Duration::from_secs_f32(secs);
                timeout(dur, self.client.wait_for_connect())
                    .await
                    .map_err(anyhow::Error::new)
                    .and_then(|r| r.map_err(anyhow::Error::new))
            }
            Some(_) => Ok(self.client.wait_for_connect().await?),
            None => Ok(()),
        }
    }

    async fn get_response_inner(&self, key: Key) -> Result<Option<Response<hyper::Body>>> {
        // Fetch response item
        let res: Option<Vec<u8>> = self.client.get(make_redis_key(&key)).await?;
        let response_item: ResponseItem = match res {
            Some(res) => flexbuffers::from_slice(&res)?,
            None => return Ok(None),
        };

        // Check surrogate keys in the internal cache first
        let mut surrogate_keys = response_item.surrogate_keys;
        if self.config.internal_cache_size > 0 {
            let mut int_cache = self.internal_cache.lock().await;
            let int_cache_ttl = self.config.internal_cache_ttl;

            let mut surrogate_keys_new = Vec::with_capacity(surrogate_keys.len());
            for sk in surrogate_keys {
                match int_cache.map.get_refresh(&sk) {
                    // If we have a cached key that indicates expired record then don't go to Redis
                    Some((sk_item, _)) if response_item.timestamp <= sk_item.timestamp => {
                        return Ok(None);
                    }
                    // Filter surrogate keys that fetched earlier and not expired
                    Some((_, t)) if t.elapsed().as_secs_f64() <= int_cache_ttl => {}
                    _ => {
                        surrogate_keys_new.push(sk);
                    }
                }
            }
            surrogate_keys = surrogate_keys_new;
        }

        // Fetch surrogate keys
        if !surrogate_keys.is_empty() {
            // We cannot use "mget" operation in sharded mode because keys can be in different shards
            let skeys_vals = stream::iter(surrogate_keys.clone())
                .map(|sk| self.client.get(make_redis_key(&sk)))
                .buffered(MAX_CONCURRENCY)
                .collect::<Vec<Result<RedisValue, RedisError>>>()
                .await;

            for (sk, sk_value) in surrogate_keys.into_iter().zip(skeys_vals) {
                let sk_value =
                    sk_value.with_context(|| format!("Failed to fetch surrogate key `{}`", sk))?;
                if let Some(sk_data) = sk_value.as_bytes() {
                    let sk_item: SurrogateKeyItem = flexbuffers::from_slice(sk_data)?;

                    // Cache this surrogate key
                    if self.config.internal_cache_size > 0 {
                        let mut int_cache = self.internal_cache.lock().await;
                        self.insert_to_internal_cache(&mut int_cache, sk, sk_item);
                    }

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
        let body_stream = stream::iter(vec![anyhow::Ok(response_item.body)]).chain(chunks_stream);

        // Decompress the body and headers if required
        let (body, headers);
        if response_item.flags.contains(Flags::COMPRESSED) {
            body = hyper::Body::wrap_stream(ZstdDecoder::new(body_stream));
            headers = zstd::stream::decode_all(response_item.headers.as_slice())?;
        } else {
            body = hyper::Body::wrap_stream(body_stream);
            headers = response_item.headers;
        }

        // Construct a response object
        let mut resp = Response::new(body);
        *resp.status_mut() = StatusCode::from_u16(response_item.status_code)?;
        *resp.headers_mut() = decode_headers(&headers)?;

        Ok(Some(resp))
    }

    async fn delete_responses_inner(&self, key: ItemKey) -> Result<()> {
        match key {
            ItemKey::Primary(key) => Ok(self.client.del(make_redis_key(&key)).await?),
            ItemKey::Surrogate(skey) => {
                let sk_item = SurrogateKeyItem {
                    timestamp: unix_timestamp(),
                };
                let sk_item_enc = flexbuffers::to_vec(&sk_item)?;

                // Update internal cache
                if self.config.internal_cache_size > 0 {
                    let mut int_cache = self.internal_cache.lock().await;
                    self.insert_to_internal_cache(&mut int_cache, skey.clone(), sk_item);
                }

                Ok(self
                    .client
                    .set(
                        make_redis_key(&skey),
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
        // Get the response body and headers
        let mut body = hyper::body::to_bytes(response.body_mut()).await?;
        let mut headers = encode_headers(response.headers())?;

        // If a compression level is set, compress the body and headers with the zstd encoding, if compressed update flags
        let mut flags = Flags::NONE;
        if let Some(level) = self.config.compression_level {
            // TODO: Change this after Rust 1.59 release
            // See https://github.com/rust-lang/rust/issues/71126
            let body_and_headers = try_join(
                compress_with_zstd(body, level).map_ok(Bytes::from),
                compress_with_zstd(Bytes::from(headers), level),
            )
            .await?;
            body = body_and_headers.0;
            headers = body_and_headers.1;
            flags.insert(Flags::COMPRESSED);
        }

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
            status_code: response.status().as_u16(),
            timestamp: unix_timestamp(),
            surrogate_keys: surrogate_keys.clone(),
            headers,
            body,
            num_chunks,
            flags,
        };
        let response_item_enc = flexbuffers::to_vec(&response_item)?;

        // Store response item
        self.client
            .set(
                make_redis_key(&key),
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
                    make_redis_key(&skey),
                    RedisValue::Bytes(sk_item_enc),
                    Some(Expiration::EX(86400)), // 24 hours
                    Some(SetOptions::NX),
                    false,
                )
                .await?;

            // In case the key already exist, reset TTL to 24h
            if is_exists.is_null() {
                self.client.expire(make_redis_key(&skey), 86400).await?;
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

    #[allow(unused)]
    pub fn take_latency_metrics(&self) -> Stats {
        self.client.take_latency_metrics()
    }

    #[allow(unused)]
    pub fn take_network_latency_metrics(&self) -> Stats {
        self.client.take_network_latency_metrics()
    }

    fn insert_to_internal_cache(&self, cache: &mut InternalCache, key: Key, val: SurrogateKeyItem) {
        let max_size = self.config.internal_cache_size;
        while !cache.map.is_empty() && cache.size + key.len() > max_size {
            let (removed_key, _) = cache.map.pop_front().unwrap(); // never fails
            cache.size -= removed_key.len();
        }
        if cache.size + key.len() <= max_size {
            cache.size += key.len();
            cache.map.insert(key, (val, Instant::now()));
        }
    }
}

#[async_trait]
impl Storage for RedisBackend {
    type Body = hyper::Body;
    type Error = anyhow::Error;

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error> {
        self.ensure_connected();
        let fetch_timeout = self.get_fetch_timeout();
        timeout(fetch_timeout, self.get_response_inner(key.clone()))
            .await
            .map_err(anyhow::Error::new)
            .and_then(|x| x)
            .with_context(|| format!("Failed to fetch Response for key `{}`", hex::encode(key)))
    }

    async fn delete_responses(&self, key: ItemKey) -> Result<(), Self::Error> {
        self.ensure_connected();
        let store_timeout = self.get_store_timeout();
        timeout(store_timeout, self.delete_responses_inner(key.clone()))
            .await
            .map_err(anyhow::Error::new)
            .and_then(|x| x)
            .with_context(|| format!("Failed to delete Response(s) for key `{}`", key))
    }

    async fn store_response<R>(&self, mut item: Item<R>) -> Result<(), Self::Error>
    where
        R: BorrowMut<Response<Self::Body>> + Send,
    {
        self.ensure_connected();
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
        .with_context(|| format!("Failed to store Response with key `{}`", hex::encode(key)))
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
fn make_redis_key(key: &impl AsRef<[u8]>) -> RedisKey {
    RedisKey::new(base64::encode_config(key, base64::URL_SAFE_NO_PAD))
}

#[inline]
fn make_chunk_key(key: &impl AsRef<[u8]>, n: u32) -> RedisKey {
    let key = base64::encode_config(key, base64::URL_SAFE_NO_PAD);
    RedisKey::new(format!("{{{}}}|{}", key, n))
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use http::HeaderValue;
    use hyper::{Body, Response};

    use crate::backends::redis::{Config, RedisBackend};
    use crate::storage::{Item, ItemKey, Key, Storage};

    fn make_response<B: ToString + ?Sized>(body: &B) -> Response<Body> {
        Response::builder()
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn make_uniq_key() -> Key {
        static N: AtomicUsize = AtomicUsize::new(0);
        format!("key{}", N.fetch_add(1, Ordering::Relaxed)).into()
    }

    #[tokio::test]
    async fn test_backend() {
        let backend = RedisBackend::new(Config::default(), None).await.unwrap();

        let mut resp = make_response("hello, world");
        resp.headers_mut()
            .insert("Hello", "World".try_into().unwrap());

        let key = make_uniq_key();

        // Cache response
        backend
            .store_response(Item::new(key.clone(), resp, Duration::from_secs(3)))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        assert_eq!(
            resp.headers().get("Hello"),
            Some(&HeaderValue::from_static("World"))
        );
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");

        // Delete cached response
        backend
            .delete_responses(ItemKey::Primary(key.clone()))
            .await
            .unwrap();

        // Try to fetch it back
        let resp = backend.get_response(key.clone()).await.unwrap();
        assert!(matches!(resp, None));
    }

    #[tokio::test]
    async fn test_chunked_body() {
        let mut config = Config::default();
        config.max_body_chunk_size = 2; // Set max chunk size to 2 bytes
        let backend = RedisBackend::new(config, None).await.unwrap();

        let key = make_uniq_key();

        // Cache response
        let resp = make_response("hello, world");
        backend
            .store_response(Item::new(key.clone(), resp, Duration::from_secs(3)))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");
    }

    #[tokio::test]
    async fn test_chunked_compressed_body() {
        // Same as the above test, but with compression enabled
        let mut config = Config::default();
        config.max_body_chunk_size = 2; // Set max chunk size to 2 bytes
        config.compression_level = Some(0);
        let backend = RedisBackend::new(config, None).await.unwrap();

        let key = make_uniq_key();

        // Cache response
        let resp = make_response("hello, world");

        backend
            .store_response(Item::new(key.clone(), resp, Duration::from_secs(3)))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");
    }

    #[tokio::test]
    async fn test_compressed_headers() {
        let mut config = Config::default();
        config.compression_level = Some(22);
        let backend = RedisBackend::new(config, None).await.unwrap();

        let key = make_uniq_key();

        // Cache response
        let mut resp = make_response("hello, world");
        resp.headers_mut().insert(
            "Hello-World-Header",
            "Hello world header data".try_into().unwrap(),
        );

        backend
            .store_response(Item::new(key.clone(), resp, Duration::from_secs(3)))
            .await
            .unwrap();

        // Fetch it back
        let resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        assert_eq!(
            resp.headers().get("Hello-World-Header").unwrap(),
            "Hello world header data"
        );
    }

    #[tokio::test]
    async fn test_surrogate_keys() {
        let backend = RedisBackend::new(Config::default(), None).await.unwrap();

        let key = make_uniq_key();

        // Cache response
        let resp = make_response("hello, world");
        backend
            .store_response(Item::new_with_skeys(
                key.clone(),
                resp,
                vec!["abc", "def"],
                Duration::from_secs(3),
            ))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        let body = hyper::body::to_bytes(&mut resp).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");

        // Delete by surrogate key
        backend
            .delete_responses(ItemKey::Surrogate("def".into()))
            .await
            .unwrap();

        // Try to fetch it back
        let resp = backend.get_response(key.clone()).await.unwrap();
        assert!(matches!(resp, None));
    }
}

fn parse_host_port(address: &str) -> Result<(String, u16)> {
    let (host, port) = address.split_once(':').unwrap_or((address, "6379"));
    if host.is_empty() {
        bail!("host is empty");
    }
    let port = port.parse().map_err(|_| anyhow!("invalid port"))?;
    Ok((host.to_string(), port))
}

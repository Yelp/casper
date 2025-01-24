use std::error::Error as StdError;
use std::io;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Context, Result};
use base64::Engine as _;
use bitflags::bitflags;
use fred::clients::Pool as RedisPool;
use fred::error::Error as RedisError;
use fred::interfaces::{ClientLike, KeysInterface};
use fred::types::config::{PerformanceConfig, ReconnectPolicy};
use fred::types::{Expiration, Key as RedisKey, SetOptions, Value as RedisValue};
use futures::future::{try_join, try_join_all};
use futures::stream::{self, StreamExt, TryStreamExt};
use moka::future::Cache;
use ntex::http::body::{Body, SizedStream};
use ntex::http::{Response, StatusCode};
use ntex::util::Bytes;
use once_cell::sync::Lazy;
use opentelemetry::{global, metrics::Counter};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use super::Config;
use crate::storage::{decode_headers, encode_headers, Item, ItemKey, Key, Storage};
use crate::types::EncryptedExt;
use crate::utils::aes::{aes256_decrypt, aes256_encrypt, AESDecoder};
use crate::utils::zstd::{compress_with_zstd, decompress_with_zstd, ZstdDecoder};

// TODO: Define format version

const SURROGATE_KEYS_TTL: i64 = 86400; // 1 day

// Do not compress data less than 100 bytes
const COMPRESSION_THRESHOLD: usize = 100;

#[derive(Clone)]
pub struct RedisBackend {
    name: String,
    config: Arc<Config>,
    pool: RedisPool,
    spawned_connect: Arc<AtomicBool>,
    internal_cache: Cache<Key, (SurrogateKeyItem, Instant)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ResponseItem {
    headers: Bytes,
    status_code: u16,
    timestamp: u64,
    surrogate_keys: Vec<Key>,
    body: Bytes,
    // Total original body length (before compression)
    body_length: usize,
    num_chunks: u32,
    flags: Flags,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct SurrogateKeyItem {
    timestamp: u64,
}

bitflags! {
    #[derive(Default, Serialize, Deserialize)]
    struct Flags: u32 {
        const UNUSED             = 0b00000001;
        const HEADERS_COMPRESSED = 0b00000010; // Headers compression
        const BODY_COMPRESSED    = 0b00000100; // Body compression
        const ENCRYPTED          = 0b00001000;
    }
}

const HEADERS_COMPRESSED: Flags = Flags::HEADERS_COMPRESSED;
const BODY_COMPRESSED: Flags = Flags::BODY_COMPRESSED;
const ENCRYPTED: Flags = Flags::ENCRYPTED;

struct RedisMetrics {
    pub internal_cache_counter: Counter<u64>,
}

static METRICS: Lazy<RedisMetrics> = Lazy::new(RedisMetrics::new);

impl RedisMetrics {
    fn new() -> Self {
        let meter = global::meter("redis");
        RedisMetrics {
            internal_cache_counter: meter
                .u64_counter("redis_internal_cache_requests")
                .with_description("Total number of Redis requests served from the internal cache.")
                .build(),
        }
    }

    fn internal_cache_counter_inc(&self, name: &str, status: &'static str) {
        let attributes = [
            opentelemetry::KeyValue::new("name", name.to_owned()),
            opentelemetry::KeyValue::new("status", status),
        ];
        self.internal_cache_counter.add(1, &attributes);
    }
}

impl RedisBackend {
    /// Creates a new Redis backend instance without connecting to the server.
    pub fn new(config: Config, name: impl Into<Option<String>>) -> Result<Self> {
        let (redis_config, conn_config) = config.clone().into_fred_configs()?;

        // Use default performance config and connection config (with tcp nodelay)
        let perf_config = PerformanceConfig::default();
        let policy = ReconnectPolicy::default();
        let pool = RedisPool::new(
            redis_config,
            Some(perf_config),
            Some(conn_config),
            Some(policy),
            config.pool_size,
        )?;

        let internal_cache_size = config.internal_cache_size;
        let backend = RedisBackend {
            name: name.into().unwrap_or_else(|| "redis".to_string()),
            config: Arc::new(config),
            pool,
            spawned_connect: Arc::new(AtomicBool::new(false)),
            internal_cache: Cache::builder()
                .max_capacity(internal_cache_size as u64)
                .weigher(|k: &Key, _: &(SurrogateKeyItem, Instant)| {
                    (k.len() + mem::size_of::<SurrogateKeyItem>() + mem::size_of::<Instant>())
                        .try_into()
                        .unwrap_or(u32::MAX)
                })
                .build(),
        };

        Ok(backend)
    }

    pub async fn connect(&self) -> Result<()> {
        // Nothing to do on lazy mode
        if !self.config.lazy && !self.spawned_connect.swap(true, Ordering::SeqCst) {
            let _handles = self.pool.connect();
            if let Err(err) = self.wait_for_connect().await {
                // Do not abort connection tasks, only return a error
                return Err(err.context("Failed to connect to Redis"));
            }
        }
        Ok(())
    }

    async fn wait_for_connect(&self) -> Result<()> {
        match self.config.wait_for_connect {
            Some(secs) if secs > 0.0 => {
                let dur = Duration::from_secs_f32(secs);
                timeout(dur, self.pool.wait_for_connect())
                    .await
                    .map_err(anyhow::Error::new)
                    .and_then(|r| r.map_err(anyhow::Error::new))
            }
            Some(_) => Ok(self.pool.wait_for_connect().await?),
            None => Ok(()),
        }
    }

    #[inline]
    fn lazy_connect(&self) {
        // Non-lazy instances should be already connected
        if self.config.lazy && !self.spawned_connect.swap(true, Ordering::SeqCst) {
            self.pool.connect();
        }
    }

    async fn get_response_inner(&self, key: Key) -> Result<Option<Response<Body>>> {
        // Fetch response item
        let res: Option<Vec<u8>> = self.pool.get(make_redis_key(&key)).await?;
        let response_item: ResponseItem = match res {
            Some(res) => flexbuffers::from_slice(&res)?,
            None => return Ok(None),
        };

        // Check surrogate keys in the internal cache first
        let mut surrogate_keys = response_item.surrogate_keys;
        if self.config.internal_cache_size > 0 {
            let int_cache_ttl = self.config.internal_cache_ttl;

            let mut surrogate_keys_new = Vec::with_capacity(surrogate_keys.len());
            for sk in surrogate_keys {
                match self.internal_cache.get(&sk).await {
                    // If we have a cached key that indicates expired record then don't go to Redis
                    Some((sk_item, _)) if response_item.timestamp <= sk_item.timestamp => {
                        METRICS.internal_cache_counter_inc(&self.name, "hit");
                        return Ok(None);
                    }
                    // Filter surrogate keys that fetched earlier and not expired
                    Some((_, t)) if t.elapsed().as_secs_f64() <= int_cache_ttl => {
                        METRICS.internal_cache_counter_inc(&self.name, "hit");
                    }
                    _ => {
                        surrogate_keys_new.push(sk);
                        METRICS.internal_cache_counter_inc(&self.name, "miss");
                    }
                }
            }
            surrogate_keys = surrogate_keys_new;
        }

        // Fetch surrogate keys
        if !surrogate_keys.is_empty() {
            // We cannot use "mget" operation in sharded mode because keys can be in different shards
            let skeys_vals = stream::iter(surrogate_keys.clone())
                .map(|sk| self.pool.get(make_redis_key(&sk)))
                .buffered(Self::MAX_CONCURRENCY)
                .collect::<Vec<Result<RedisValue, RedisError>>>()
                .await;

            for (sk, sk_value) in surrogate_keys.into_iter().zip(skeys_vals) {
                let sk_value =
                    sk_value.with_context(|| format!("Failed to fetch surrogate key {sk:?}"))?;
                if let Some(sk_data) = sk_value.as_bytes() {
                    let sk_item: SurrogateKeyItem = flexbuffers::from_slice(sk_data)?;

                    // Cache this surrogate key
                    if self.config.internal_cache_size > 0 {
                        self.internal_cache
                            .insert(sk, (sk_item, Instant::now()))
                            .await;
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

        let status = StatusCode::from_u16(response_item.status_code)?;
        let flags = response_item.flags;
        let mut raw_headers = response_item.headers;

        // Decrypt headers if required
        let encryption_key = self.config.encryption_key.as_ref();
        raw_headers = match (flags.contains(ENCRYPTED), encryption_key) {
            (true, Some(key)) => aes256_decrypt(raw_headers, key.clone())
                .await
                .context("failed to decrypt headers")?,
            (true, None) => return Err(anyhow!("response is encrypted")),
            (false, _) => raw_headers,
        };
        // Decompress headers if required
        if flags.contains(HEADERS_COMPRESSED) {
            raw_headers = decompress_with_zstd(raw_headers)
                .await
                .context("failed to decompress headers")?;
        }

        // Decode them
        let headers = decode_headers(&raw_headers).context("failed to decode headers")?;

        // If we have only one chunk, decode it in-place
        if response_item.num_chunks == 1 {
            let mut body = response_item.body;
            // Decrypt body
            if flags.contains(ENCRYPTED) {
                body = aes256_decrypt(body, encryption_key.unwrap().clone()).await?;
            }
            // Decompress body
            if flags.contains(BODY_COMPRESSED) {
                body = decompress_with_zstd(body).await?;
            }

            // Construct a new Response object
            let mut resp = Response::with_body(status, Body::Bytes(body));
            *resp.headers_mut() = headers;
            if flags.contains(ENCRYPTED) {
                resp.extensions_mut().insert(EncryptedExt(true));
            }
            return Ok(Some(resp));
        }

        // Make body stream to fetch chunks from Redis
        let num_chunks = response_item.num_chunks as usize;
        // First chunk is stored in the response item, skip it
        let chunks_stream = stream::iter(vec![(self.pool.clone(), key); num_chunks - 1])
            .enumerate()
            .then(move |(i, (client, key))| async move {
                let chunk_key = make_chunk_key(&key, i as u32 + 1);
                let data = client
                    .get::<Option<Vec<u8>>, _>(chunk_key)
                    .await
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                match data {
                    Some(data) => Ok(Bytes::from(data)),
                    None => Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("cannot find chunk {}/{}", i + 2, num_chunks),
                    )),
                }
            });
        let body_stream = stream::iter(vec![Ok(response_item.body)]).chain(chunks_stream);

        // Decrypt and/or decompress the body if required
        let body_size = response_item.body_length as u64;
        let body = match (flags.contains(ENCRYPTED), flags.contains(BODY_COMPRESSED)) {
            (true, true) => {
                // Decrypt and decompress
                let body_stream = AESDecoder::new(body_stream, encryption_key.unwrap().clone());
                let body_stream =
                    ZstdDecoder::new(body_stream).map_err(|err| Box::new(err) as Box<dyn StdError>);
                Body::Message(Box::new(SizedStream::new(body_size, Box::pin(body_stream))))
            }
            (true, false) => {
                // Decrypt only
                let body_stream = AESDecoder::new(body_stream, encryption_key.unwrap().clone())
                    .map_err(|err| Box::new(err) as Box<dyn StdError>);
                Body::Message(Box::new(SizedStream::new(body_size, Box::pin(body_stream))))
            }
            (false, true) => {
                // Decompress only
                let body_stream =
                    ZstdDecoder::new(body_stream).map_err(|err| Box::new(err) as Box<dyn StdError>);
                Body::Message(Box::new(SizedStream::new(body_size, Box::pin(body_stream))))
            }
            (false, false) => {
                // Do nothing
                let body_stream = body_stream.map_err(|err| Box::new(err) as Box<dyn StdError>);
                Body::Message(Box::new(SizedStream::new(body_size, Box::pin(body_stream))))
            }
        };

        // Construct a new Response object
        let mut resp = Response::with_body(status, body);
        *resp.headers_mut() = headers;
        if flags.contains(ENCRYPTED) {
            resp.extensions_mut().insert(EncryptedExt(true));
        }
        Ok(Some(resp))
    }

    async fn delete_responses_inner(&self, key: ItemKey) -> Result<()> {
        match key {
            ItemKey::Primary(key) => Ok(self.pool.del(make_redis_key(&key)).await?),
            ItemKey::Surrogate(skey) => {
                let sk_item = SurrogateKeyItem {
                    timestamp: current_timestamp(),
                };
                let sk_item_enc = flexbuffers::to_vec(sk_item)?;

                // Update internal cache
                if self.config.internal_cache_size > 0 {
                    self.internal_cache
                        .insert(skey.clone(), (sk_item, Instant::now()))
                        .await;
                }

                Ok(self
                    .pool
                    .set(
                        make_redis_key(&skey),
                        RedisValue::Bytes(sk_item_enc.into()),
                        Some(Expiration::EX(SURROGATE_KEYS_TTL)),
                        None,
                        false,
                    )
                    .await?)
            }
        }
    }

    async fn store_response_inner(&self, item: Item<'_>) -> Result<usize> {
        let mut stored_bytes = 0;
        let mut headers = Bytes::from(encode_headers(&item.headers)?);
        let mut body = item.body;
        let body_length = body.len();

        let max_ttl = self.config.max_ttl;
        let ttl = max_ttl
            .map(|max_ttl| std::cmp::max(max_ttl, item.ttl.as_secs()))
            .unwrap_or(item.ttl.as_secs());

        // If compression level is set, compress the body and headers and update flags
        let mut flags = Flags::default();
        if let Some(level) = self.config.compression_level {
            let (headers_comp, body_comp);
            if body.len() < COMPRESSION_THRESHOLD {
                // Compress only headers if the body is too small
                headers_comp = compress_with_zstd(headers.clone(), level).await?;
                body_comp = body.clone();
            } else {
                (headers_comp, body_comp) = try_join(
                    compress_with_zstd(headers.clone(), level),
                    compress_with_zstd(body.clone(), level),
                )
                .await?;
            }
            if headers_comp.len() < headers.len() {
                headers = headers_comp;
                flags |= HEADERS_COMPRESSED;
            }
            if body_comp.len() < body.len() {
                body = body_comp;
                flags |= BODY_COMPRESSED;
            }
        }

        // If encryption is enabled, encrypt the body and headers and update flags
        if let (true, Some(key)) = (item.encrypt, &self.config.encryption_key) {
            (headers, body) = try_join(
                aes256_encrypt(headers, key.clone()),
                aes256_encrypt(body, key.clone()),
            )
            .await?;
            flags.insert(ENCRYPTED);
        }

        // Split body to chunks and save chunks first
        let max_chunk_size = self.config.max_body_chunk_size;
        let mut num_chunks = 1;
        if max_chunk_size > 0 && body.len() > max_chunk_size {
            let body_tail = body.split_off(max_chunk_size);
            for (i, chunk) in body_tail.chunks(max_chunk_size).enumerate() {
                num_chunks += 1;
                // Store chunk in Redis
                self.pool
                    .set::<(), _, _>(
                        make_chunk_key(&item.key, i as u32 + 1),
                        RedisValue::Bytes(chunk.to_vec().into()),
                        Some(Expiration::EX(ttl as i64)),
                        None,
                        false,
                    )
                    .await?;
                stored_bytes += chunk.len();
            }
        }

        let timestamp = current_timestamp();
        let response_item = ResponseItem {
            status_code: item.status.as_u16(),
            timestamp,
            surrogate_keys: item.surrogate_keys.clone(),
            headers,
            body,
            body_length, // Original length before compression
            num_chunks,
            flags,
        };
        let response_item_enc = flexbuffers::to_vec(&response_item)?;
        let response_item_size = response_item_enc.len();

        // Store response item
        self.pool
            .set::<(), _, _>(
                make_redis_key(&item.key),
                RedisValue::Bytes(response_item_enc.into()),
                Some(Expiration::EX(ttl as i64)),
                None,
                false,
            )
            .await?;
        stored_bytes += response_item_size;

        // Update surrogate keys
        let int_cache_ttl = self.config.internal_cache_ttl;
        try_join_all(item.surrogate_keys.into_iter().map(|skey| async move {
            let refresh_ttl = match self.internal_cache.get(&skey).await {
                Some((_, t)) if t.elapsed().as_secs_f64() <= int_cache_ttl => {
                    // Do nothing, key is known
                    METRICS.internal_cache_counter_inc(&self.name, "hit");
                    true
                }
                _ => {
                    METRICS.internal_cache_counter_inc(&self.name, "miss");
                    // We set timestamp to the current time to not accidentally serve stalled items
                    // in case of surrogate key loss.
                    // Minus 1 second is needed to keep the current response fresh, because we invalidate
                    // everything up to (and including) the surrogate key timestamp.
                    let sk_item = SurrogateKeyItem {
                        timestamp: timestamp - 1,
                    };
                    let sk_item_enc = flexbuffers::to_vec(sk_item)?;

                    // Store new surrogate key atomically (NX option)
                    let is_executed: RedisValue = self
                        .pool
                        .set(
                            make_redis_key(&skey),
                            RedisValue::Bytes(sk_item_enc.into()),
                            Some(Expiration::EX(SURROGATE_KEYS_TTL)),
                            Some(SetOptions::NX),
                            false,
                        )
                        .await?;
                    is_executed.is_null()
                }
            };
            if refresh_ttl && rand::random::<u8>() % 100 < 1 {
                // Refresh TTL with 1% probability
                self.pool
                    .expire::<(), _>(make_redis_key(&skey), SURROGATE_KEYS_TTL, None)
                    .await?;
            }
            anyhow::Ok(())
        }))
        .await?;

        Ok(stored_bytes)
    }

    fn get_fetch_timeout(&self) -> Duration {
        Duration::from_secs_f32(self.config.timeouts.fetch_timeout)
    }

    fn get_store_timeout(&self) -> Duration {
        Duration::from_secs_f32(self.config.timeouts.store_timeout)
    }
}

impl Storage for RedisBackend {
    type Body = Body;
    type Error = anyhow::Error;

    fn name(&self) -> String {
        self.name.clone()
    }

    fn backend_type(&self) -> &'static str {
        "redis"
    }

    async fn connect(&self) -> Result<(), Self::Error> {
        RedisBackend::connect(self).await
    }

    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error> {
        self.lazy_connect();
        let fetch_timeout = self.get_fetch_timeout();
        timeout(fetch_timeout, self.get_response_inner(key.clone()))
            .await
            .map_err(anyhow::Error::new)
            .and_then(|x| x)
            .with_context(|| format!("Failed to fetch Response for key `{}`", hex::encode(key)))
    }

    async fn delete_responses(&self, key: ItemKey) -> Result<(), Self::Error> {
        self.lazy_connect();
        let store_timeout = self.get_store_timeout();
        timeout(store_timeout, self.delete_responses_inner(key.clone()))
            .await
            .map_err(anyhow::Error::new)
            .and_then(|x| x)
            .with_context(|| format!("Failed to delete Response(s) for key `{}`", key))
    }

    async fn store_response(&self, item: Item<'_>) -> Result<usize, Self::Error> {
        self.lazy_connect();
        let key = item.key.clone();
        let store_timeout = self.get_store_timeout();
        timeout(store_timeout, self.store_response_inner(item))
            .await
            .map_err(anyhow::Error::new)
            .and_then(|x| x)
            .with_context(|| format!("Failed to store Response with key `{}`", hex::encode(key)))
    }
}

#[inline]
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time is before UNIX_EPOCH")
        .as_secs()
}

#[inline]
fn make_redis_key(key: impl AsRef<[u8]>) -> RedisKey {
    RedisKey::from(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key))
}

#[inline]
fn make_chunk_key(key: impl AsRef<[u8]>, n: u32) -> RedisKey {
    let key = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key);
    RedisKey::from(format!("{{{}}}|{}", key, n))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use ntex::http::header::{HeaderName, HeaderValue};
    use ntex::http::Response;
    use ntex::util::Bytes;

    use super::{Config, RedisBackend};
    use crate::http::buffer_body;
    use crate::storage::{Item, ItemKey, Key, Storage};

    fn make_response(body: impl Into<Bytes>) -> Response<Bytes> {
        Response::Ok().message_body(body.into())
    }

    fn make_uniq_key() -> Key {
        static N: AtomicUsize = AtomicUsize::new(0);
        format!("key{}", N.fetch_add(1, Ordering::Relaxed)).into()
    }

    #[ntex::test]
    async fn test_backend() {
        let backend = RedisBackend::new(Config::default(), None).unwrap();
        backend.connect().await.unwrap();

        let mut resp = make_response("hello, world");
        resp.headers_mut().insert(
            HeaderName::from_static("hello"),
            HeaderValue::from_static("World"),
        );

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
        let body = buffer_body(resp.take_body()).await.unwrap().to_vec();
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

    #[ntex::test]
    async fn test_chunked_body() {
        let mut config = Config::default();
        config.max_body_chunk_size = 2; // Set max chunk size to 2 bytes
        let backend = RedisBackend::new(config, None).unwrap();
        backend.connect().await.unwrap();

        let key = make_uniq_key();

        // Cache response
        let resp = make_response("hello, world");
        backend
            .store_response(Item::new(key.clone(), resp, Duration::from_secs(3)))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        let body = buffer_body(resp.take_body()).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");
    }

    #[ntex::test]
    async fn test_compression() {
        let mut config = Config::default();
        config.compression_level = Some(22);
        let backend = RedisBackend::new(config, None).unwrap();
        backend.connect().await.unwrap();

        let key = make_uniq_key();

        // Cache response
        let mut resp = make_response("hello, world"); // body is too small to be compressed
        resp.headers_mut().insert(
            HeaderName::from_static("hello-world-header"),
            HeaderValue::from_static("Hello world header data"),
        );
        backend
            .store_response(Item::new(key.clone(), resp, Duration::from_secs(3)))
            .await
            .unwrap();

        // Fetch it back
        let mut resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        assert_eq!(
            resp.headers().get("Hello-World-Header").unwrap(),
            "Hello world header data"
        );
        let body = buffer_body(resp.take_body()).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");
    }

    #[ntex::test]
    async fn test_encryption() {
        let mut config = Config::default();
        config.encryption_key = Some(Bytes::from_static(&[16; 32]));
        let backend = RedisBackend::new(config, None).unwrap();
        backend.connect().await.unwrap();

        let key = make_uniq_key();

        // Cache response
        let mut resp = make_response("hello, world");
        resp.headers_mut().insert(
            HeaderName::from_static("x-header"),
            HeaderValue::from_static("value"),
        );
        let mut item = Item::new(key.clone(), resp, Duration::from_secs(3));
        item.encrypt = true;
        backend.store_response(item).await.unwrap();

        // Fetch it back
        let mut resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        assert_eq!(resp.headers().get("X-Header").unwrap(), "value".as_bytes());
        let body = buffer_body(resp.take_body()).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world");
    }

    #[ntex::test]
    async fn test_chunked_compression_encryption() {
        let mut config = Config::default();
        config.max_body_chunk_size = 2; // Set max chunk size to 2 bytes
        config.compression_level = Some(0); // Use zstd default compression level
        config.encryption_key = Some(Bytes::from_static(&[16; 32]));
        let backend = RedisBackend::new(config, None).unwrap();
        backend.connect().await.unwrap();

        let key = make_uniq_key();

        // Cache response
        let mut resp = make_response("hello, world!".repeat(10));
        resp.headers_mut().insert(
            HeaderName::from_static("x-header"),
            HeaderValue::from_static("value"),
        );
        let mut item = Item::new(key.clone(), resp, Duration::from_secs(3));
        item.encrypt = true;
        backend.store_response(item).await.unwrap();

        // Fetch it back
        let mut resp = backend.get_response(key.clone()).await.unwrap().unwrap();
        assert_eq!(resp.headers().get("X-Header").unwrap(), "value".as_bytes());
        let body = buffer_body(resp.take_body()).await.unwrap().to_vec();
        assert_eq!(String::from_utf8(body).unwrap(), "hello, world!".repeat(10));
    }

    #[ntex::test]
    async fn test_surrogate_keys() {
        let backend = RedisBackend::new(Config::default(), None).unwrap();
        backend.connect().await.unwrap();

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
        let body = buffer_body(resp.take_body()).await.unwrap().to_vec();
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

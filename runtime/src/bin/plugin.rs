use std::collections::HashMap;
use std::env::var;
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use hyper::{
    header::{HeaderName, HeaderValue},
    service::service_fn,
    Body, Request, Response, Server, StatusCode,
};
use ripemd::{Digest, Ripemd160};
use serde_json::{json, Value as JsonValue};
use tokio::sync::OnceCell;
use tower::make::Shared;

use casper_runtime::{
    backends::{RedisBackend, RedisConfig, RedisServerConfig, RedisTimeoutConfig},
    storage::{Item, ItemKey, Key, Storage},
};

#[derive(serde::Deserialize)]
struct MethodArg<'a> {
    #[serde(rename = "method")]
    name: &'a str,
}

#[derive(serde::Deserialize)]
struct KeyArgs {
    cache_key: String,
    namespace: String,
    cache_name: String,
    id: Option<String>,
    vary_headers: Option<String>,
}

#[derive(serde::Deserialize)]
struct StoreMethodArgs {
    #[serde(flatten)]
    key: KeyArgs,
    ttl: f64,
    headers: HashMap<String, JsonValue>,
}

#[derive(serde::Deserialize)]
struct PurgeMethodArgs {
    namespace: String,
    cache_name: String,
    id: Option<String>,
}

async fn create_backend(endpoint: String, cluster: bool) -> Result<RedisBackend> {
    let config = RedisConfig {
        timeouts: RedisTimeoutConfig {
            connect_timeout_ms: 3000, // 3 sec
            fetch_timeout_ms: 200,    // 200 ms
            store_timeout_ms: 5000,   // 5 sec
        },
        enable_tls: var("REDIS_TLS_ENABLED") == Ok("1".to_string()),
        server: if cluster {
            RedisServerConfig::Clustered {
                hosts: vec![(endpoint, 6379)],
            }
        } else {
            RedisServerConfig::Centralized {
                host: endpoint,
                port: 6379,
            }
        },
        ..Default::default()
    };

    Ok(RedisBackend::new(config).await?)
}

async fn get_backend(backend: u8) -> Result<&'static RedisBackend> {
    static BACKEND1: OnceCell<RedisBackend> = OnceCell::const_new();
    static BACKEND2: OnceCell<RedisBackend> = OnceCell::const_new();

    if backend == 1 {
        BACKEND1
            .get_or_try_init(|| async {
                if let Ok(endpoint) = var("REDIS_CLUSTER_ENDPOINT") {
                    create_backend(endpoint, true).await
                } else if let Ok(endpoint) = var("REDIS_ENDPOINT") {
                    create_backend(endpoint, false).await
                } else {
                    Err(anyhow!("Backend 1 does not configured"))
                }
            })
            .await
    } else if backend == 2 {
        BACKEND2
            .get_or_try_init(|| async {
                if let Ok(endpoint) = var("REDIS_CLUSTER_2_ENDPOINT") {
                    create_backend(endpoint, true).await
                } else if let Ok(endpoint) = var("REDIS_2_ENDPOINT") {
                    create_backend(endpoint, false).await
                } else {
                    Err(anyhow!("Backend 2 does not configured"))
                }
            })
            .await
    } else {
        panic!("wrong backend number")
    }
}

fn calculate_primary_key(key: &KeyArgs) -> Key {
    let mut hasher = Ripemd160::new();
    hasher.update(&key.cache_key);
    hasher.update(&key.namespace);
    hasher.update(&key.cache_name);
    if matches!(&key.id, Some(id) if !id.is_empty() && id != "null") {
        hasher.update(key.id.as_ref().unwrap());
    }
    if let Some(vary_headers) = &key.vary_headers {
        hasher.update(vary_headers);
    }
    hasher.finalize().to_vec().into()
}

async fn handler_impl(mut req: Request<Body>) -> Result<Response<Body>, anyhow::Error> {
    let mut data = hyper::body::to_bytes(req.body_mut()).await?;

    // Decode body format: `<len>|<args><body>`, where `len` is length of `args`
    let args_idx = data.iter().position(|&x| x == b'|').unwrap() + 1;
    let args_len = std::str::from_utf8(&data[..args_idx - 1]).context("invalid data format")?;
    let args_len = args_len.parse::<usize>().context("invalid data format")?;
    let body = data.split_off(args_idx + args_len);
    let method: MethodArg = serde_json::from_slice(&data[args_idx..])?;

    if method.name == "store_body_and_headers" {
        let args: StoreMethodArgs = serde_json::from_slice(&data[args_idx..])?;

        // Calculate primary key
        let key = calculate_primary_key(&args.key);

        // Make surrogate keys
        let namespace_cache = format!("{}|{}", args.key.namespace, args.key.cache_name);
        let mut surrogate_keys = vec![namespace_cache.clone()];
        if matches!(&args.key.id, Some(id) if !id.is_empty() && id != "null") {
            surrogate_keys.push(format!("{}|{}", namespace_cache, &args.key.id.unwrap()));
        }

        let ttl = Duration::from_secs_f64(args.ttl);

        // Build response for caching
        let mut resp = Response::new(Body::from(body));
        for (name, val) in args.headers {
            let name = HeaderName::from_str(&name)?;
            if let Some(vals) = val.as_array() {
                for val in vals {
                    resp.headers_mut().append(
                        name.clone(),
                        HeaderValue::from_str(
                            val.as_str()
                                .ok_or_else(|| anyhow!("invalid header value"))?,
                        )?,
                    );
                }
            } else {
                resp.headers_mut().append(
                    name,
                    HeaderValue::from_str(
                        val.as_str()
                            .ok_or_else(|| anyhow!("invalid header value"))?,
                    )?,
                );
            }
        }

        // Store response to backend
        get_backend(1)
            .await?
            .store_response(Item::new_with_skeys(key, &mut resp, surrogate_keys, ttl))
            .await?;

        return Ok(Response::new(Body::from("Ok")));
    }

    if method.name == "fetch_body_and_headers" {
        let args: KeyArgs = serde_json::from_slice(&data[args_idx..])?;

        // Calculate primary key
        let key = calculate_primary_key(&args);

        // Fetch response
        return match get_backend(1).await?.get_response(key).await? {
            Some(mut resp) => {
                let mut headers_map = serde_json::Map::new();
                for (name, val) in resp.headers() {
                    let name = name.as_str();
                    let val = String::from_utf8(val.as_bytes().to_vec())?;
                    match headers_map.get_mut(name) {
                        Some(JsonValue::Array(values)) => values.push(json!(val)),
                        Some(value) => *value = json!([value, val]),
                        None => {
                            headers_map.insert(name.to_string(), json!(val));
                        }
                    }
                }

                let mut headers = serde_json::to_vec(&headers_map)?;
                let body = hyper::body::to_bytes(resp.body_mut()).await?;

                // Encode body format: `<len>|<headers><body>`, where `len` is length of `headers`
                let mut buf = Vec::new();
                write!(&mut buf, "{}|", headers.len())?;
                buf.append(&mut headers);
                buf.append(&mut body.to_vec());
                Ok(Response::new(Body::from(buf)))
            }
            None => Ok(Response::new(Body::from(""))),
        };
    }

    if method.name == "purge" {
        let args: PurgeMethodArgs = serde_json::from_slice(&data[args_idx..])?;

        // Make surrogate key
        let mut surrogate_key = format!("{}|{}", args.namespace, args.cache_name);
        if matches!(&args.id, Some(id) if !id.is_empty() && id != "null") {
            surrogate_key.push('|');
            surrogate_key.push_str(&args.id.unwrap());
        }

        // Purge backend 1
        get_backend(1)
            .await?
            .delete_responses(ItemKey::Surrogate(surrogate_key.clone().into()))
            .await?;

        // If a 2nd backend is defined also purge
        if let Ok(backend2) = get_backend(2).await {
            backend2
                .delete_responses(ItemKey::Surrogate(surrogate_key.into()))
                .await?;
        }
        return Ok(Response::new(Body::from("Ok")));
    }

    if method.name == "stats" {
        let backend = get_backend(1).await?;

        let latency_metrics = backend.take_latency_metrics();
        let net_latency_metrics = backend.take_network_latency_metrics();

        let data = json!({
            "latency": {
                "min": latency_metrics.min,
                "max": latency_metrics.max,
                "avg": latency_metrics.avg,
            },
            "network_latency": {
                "min": net_latency_metrics.min,
                "max": net_latency_metrics.max,
                "avg": net_latency_metrics.avg,
            },
        });

        return Ok(Response::new(Body::from(data.to_string())));
    }

    Ok(Response::builder()
        .status(400)
        .body(Body::from("Bad request"))?)
}

async fn handler(req: Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
    match handler_impl(req).await {
        Ok(resp) => Ok(resp),
        Err(err) => {
            eprintln!("{:#}", err);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("{:?}", err)))
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 34567));

    // Initialize Redis backend
    if let Err(err) = get_backend(1).await {
        eprintln!("{:#}", err);
    }

    let server = Server::bind(&addr).serve(Shared::new(service_fn(handler)));

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

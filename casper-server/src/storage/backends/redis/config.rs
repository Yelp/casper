use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use fred::types::config::{Config as RedisConfig, ConnectionConfig, TcpConfig, TlsConnector};
use ntex::util::Bytes;
use serde::Deserialize;

/// Redis backend configuration
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
    pub max_ttl: Option<u64>,

    pub wait_for_connect: Option<f32>,
    #[serde(default)]
    pub lazy: bool,

    #[serde(default = "Config::default_internal_cache_size")]
    pub internal_cache_size: usize,
    #[serde(default = "Config::default_internal_cache_ttl")]
    pub internal_cache_ttl: f64,

    // Optional encryption key
    pub encryption_key: Option<Bytes>,
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
    fn into_redis_server_config(self) -> Result<fred::types::config::ServerConfig> {
        match self {
            ServerConfig::Centralized { endpoint } => {
                let (host, port) = parse_host_port(&endpoint)
                    .with_context(|| format!("invalid redis endpoint `{endpoint}`"))?;
                Ok(fred::types::config::ServerConfig::new_centralized(
                    host, port,
                ))
            }
            ServerConfig::Clustered { endpoints } => {
                let mut hosts = Vec::new();
                for endpoint in endpoints {
                    let (host, port) = parse_host_port(&endpoint)
                        .with_context(|| format!("invalid redis endpoint `{endpoint}`"))?;
                    hosts.push((host, port));
                }
                Ok(fred::types::config::ServerConfig::new_clustered(hosts))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct TimeoutConfig {
    /// A limit on the amount of time an application can take to establish a connection to Redis
    #[serde(default = "TimeoutConfig::default_connect_timeout")]
    pub connect_timeout: f32,

    /// A limit on the amount of time an application can take to fetch response or next chunk from Redis
    #[serde(default = "TimeoutConfig::default_fetch_timeout")]
    pub fetch_timeout: f32,

    /// A limit on the amount of time an application can take to store response in Redis
    #[serde(default = "TimeoutConfig::default_store_timeout")]
    pub store_timeout: f32,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        TimeoutConfig {
            connect_timeout: TimeoutConfig::default_connect_timeout(),
            fetch_timeout: TimeoutConfig::default_fetch_timeout(),
            store_timeout: TimeoutConfig::default_store_timeout(),
        }
    }
}

impl TimeoutConfig {
    const fn default_connect_timeout() -> f32 {
        3.0
    }

    const fn default_fetch_timeout() -> f32 {
        1.0
    }

    const fn default_store_timeout() -> f32 {
        2.0
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
            max_ttl: None,
            wait_for_connect: Some(0.0),
            lazy: false,
            internal_cache_size: Config::default_internal_cache_size(),
            internal_cache_ttl: Config::default_internal_cache_ttl(),
            encryption_key: None,
        }
    }
}

impl Config {
    fn default_pool_size() -> usize {
        2 * num_cpus::get()
    }

    const fn default_max_body_chunk_size() -> usize {
        1024 * 1024 // 1 MB
    }

    const fn default_internal_cache_size() -> usize {
        32 * 1024 * 1024 // 32 MB
    }

    const fn default_internal_cache_ttl() -> f64 {
        0.0
    }

    pub(super) fn into_fred_configs(self) -> Result<(RedisConfig, ConnectionConfig)> {
        let redis_config = RedisConfig {
            fail_fast: !self.lazy,
            username: self.username,
            password: self.password,
            server: self.server.into_redis_server_config()?,
            tls: if self.enable_tls {
                Some(TlsConnector::default_native_tls()?.into())
            } else {
                None
            },
            ..Default::default()
        };
        let conn_config = ConnectionConfig {
            connection_timeout: Duration::from_secs_f32(self.timeouts.connect_timeout),
            tcp: TcpConfig {
                nodelay: Some(true),
                ..Default::default()
            },
            ..Default::default()
        };
        Ok((redis_config, conn_config))
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

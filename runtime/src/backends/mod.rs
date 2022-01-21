use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context};
use once_cell::sync::OnceCell;

pub use memory::{Config as MemoryConfig, MemoryBackend};
pub use redis::{
    Config as RedisConfig, RedisBackend, ServerConfig as RedisServerConfig,
    TimeoutConfig as RedisTimeoutConfig,
};

static REGISTERED_BACKENDS: OnceCell<HashMap<String, Backend>> = OnceCell::new();

pub enum Backend {
    Memory(Arc<MemoryBackend>),
    Redis(Arc<RedisBackend>),
}

pub async fn register_backends(
    backends_config: HashMap<String, serde_yaml::Value>,
) -> anyhow::Result<()> {
    let mut registered_backends = HashMap::new();

    for (name, config) in backends_config {
        let backend_type = config
            .get("backend")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("backend is not set for storage `{name}`"))?;

        match backend_type {
            "memory" => {
                let config = serde_yaml::from_value::<MemoryConfig>(config).with_context(|| {
                    format!("invalid backend configuration for storage `{name}`")
                })?;
                registered_backends
                    .insert(name, Backend::Memory(Arc::new(MemoryBackend::new(&config))));
            }
            "redis" => {
                let config = serde_yaml::from_value::<RedisConfig>(config).with_context(|| {
                    format!("invalid backend configuration for storage `{name}`")
                })?;
                let backend = RedisBackend::new(config).await.with_context(|| {
                    format!("unable to initialize backend for storage `{name}`")
                })?;
                registered_backends.insert(name, Backend::Redis(Arc::new(backend)));
            }
            _ => bail!("unknown backend `{}` for storage `{}`", backend_type, name),
        }
    }

    REGISTERED_BACKENDS
        .set(registered_backends)
        .map_err(|_| anyhow!("register_backends() must be called once"))?;

    Ok(())
}

pub fn registered_backends() -> &'static HashMap<String, Backend> {
    REGISTERED_BACKENDS
        .get()
        .expect("register_backends() must be called first")
}

mod common;
mod memory;
mod redis;

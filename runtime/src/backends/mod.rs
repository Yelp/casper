use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context};
use once_cell::sync::OnceCell;

pub use dynamodb::{Config as DynamodDbBackendConfig, DynamodDbBackend};
pub use memory::{Config as MemoryBackendConfig, MemoryBackend};

static REGISTERED_BACKENDS: OnceCell<HashMap<String, Backend>> = OnceCell::new();

pub enum Backend {
    Memory(Arc<MemoryBackend>),
}

pub fn register_backends(backends_config: HashMap<String, toml::Value>) -> anyhow::Result<()> {
    let mut registered_backends = HashMap::new();

    for (name, config) in backends_config {
        let backend = config
            .get("backend")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("backend is not set for storage `{}`", name))?;

        match backend {
            "memory" => {
                let config = config.try_into::<memory::Config>().with_context(|| {
                    format!("invalid backend configuration for storage `{}`", name)
                })?;

                registered_backends
                    .insert(name, Backend::Memory(Arc::new(MemoryBackend::new(&config))));
            }
            _ => bail!("unknown backend `{}` for storage `{}`", backend, name),
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

mod dynamodb;
mod memory;

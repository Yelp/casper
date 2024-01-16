use anyhow::{anyhow, bail, Context, Result};
use memory::MemoryBackend;
use ntex::http::Response;
use redis::RedisBackend;

use super::{Body, Item, ItemKey, Key, Storage};

#[derive(Clone)]
pub enum Backend {
    Memory(MemoryBackend),
    Redis(RedisBackend),
}

impl Backend {
    pub fn new(name: String, config: serde_json::Value) -> Result<Self> {
        let backend_type = config
            .get("backend")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("backend type is not set for storage `{name}`"))?;

        let backend = match backend_type {
            "memory" => {
                let config =
                    serde_json::from_value::<memory::Config>(config).with_context(|| {
                        format!("invalid backend configuration for storage `{name}`")
                    })?;
                Backend::Memory(MemoryBackend::new(&config, name.clone()))
            }
            "redis" => {
                let config =
                    serde_json::from_value::<redis::Config>(config).with_context(|| {
                        format!("invalid backend configuration for storage `{name}`")
                    })?;
                let backend = RedisBackend::new(config, name.clone()).with_context(|| {
                    format!("unable to initialize backend for storage `{name}`")
                })?;
                Backend::Redis(backend)
            }
            _ => bail!("unknown backend `{}` for storage `{}`", backend_type, name),
        };

        Ok(backend)
    }
}

impl Storage for Backend {
    type Body = Body;
    type Error = anyhow::Error;

    #[inline]
    fn name(&self) -> String {
        match self {
            Backend::Memory(inner) => inner.name(),
            Backend::Redis(inner) => inner.name(),
        }
    }

    #[inline]
    fn backend_type(&self) -> &'static str {
        match self {
            Backend::Memory(inner) => inner.backend_type(),
            Backend::Redis(inner) => inner.backend_type(),
        }
    }

    #[inline]
    async fn connect(&self) -> Result<(), Self::Error> {
        match self {
            Backend::Memory(inner) => inner.connect().await,
            Backend::Redis(inner) => Storage::connect(inner).await,
        }
    }

    #[inline]
    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error> {
        match self {
            Backend::Memory(inner) => inner.get_response(key).await,
            Backend::Redis(inner) => inner.get_response(key).await,
        }
    }

    #[inline]
    async fn delete_responses(&self, key: ItemKey) -> Result<(), Self::Error> {
        match self {
            Backend::Memory(inner) => inner.delete_responses(key).await,
            Backend::Redis(inner) => inner.delete_responses(key).await,
        }
    }

    #[inline]
    async fn store_response<'a>(&self, item: Item<'a>) -> Result<(), Self::Error> {
        match self {
            Backend::Memory(inner) => inner.store_response(item).await,
            Backend::Redis(inner) => inner.store_response(item).await,
        }
    }

    #[inline]
    async fn get_responses(
        &self,
        keys: impl IntoIterator<Item = Key>,
    ) -> Vec<Result<Option<Response<Self::Body>>, Self::Error>> {
        match self {
            Backend::Memory(inner) => inner.get_responses(keys).await,
            Backend::Redis(inner) => inner.get_responses(keys).await,
        }
    }

    #[inline]
    async fn delete_responses_multi(
        &self,
        keys: impl IntoIterator<Item = ItemKey>,
    ) -> Vec<Result<(), Self::Error>> {
        match self {
            Backend::Memory(inner) => inner.delete_responses_multi(keys).await,
            Backend::Redis(inner) => inner.delete_responses_multi(keys).await,
        }
    }

    #[inline]
    async fn store_responses(
        &self,
        items: impl IntoIterator<Item = Item<'_>>,
    ) -> Vec<Result<(), Self::Error>> {
        match self {
            Backend::Memory(inner) => inner.store_responses(items).await,
            Backend::Redis(inner) => inner.store_responses(items).await,
        }
    }
}

mod memory;
mod redis;

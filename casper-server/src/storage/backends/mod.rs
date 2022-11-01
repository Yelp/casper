use anyhow::{anyhow, bail, Context, Result};
use futures::future::LocalBoxFuture;
use hyper::Response;
use memory::MemoryBackend;
use redis::RedisBackend;

use super::common::{compress_with_zstd, decode_headers, encode_headers};
use super::{Item, ItemKey, Key, Storage};

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
    type Body = hyper::Body;
    type Error = anyhow::Error;

    #[inline]
    fn name(&self) -> String {
        match self {
            Backend::Memory(inner) => inner.name(),
            Backend::Redis(inner) => inner.name(),
        }
    }

    #[inline]
    fn connect<'s, 'async_trait>(&'s self) -> LocalBoxFuture<'async_trait, Result<(), Self::Error>>
    where
        's: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Backend::Memory(inner) => inner.connect(),
            Backend::Redis(inner) => Storage::connect(inner),
        }
    }

    #[inline]
    fn get_response<'s, 'async_trait>(
        &'s self,
        key: Key,
    ) -> LocalBoxFuture<'async_trait, Result<Option<Response<Self::Body>>, Self::Error>>
    where
        's: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Backend::Memory(inner) => inner.get_response(key),
            Backend::Redis(inner) => inner.get_response(key),
        }
    }

    #[inline]
    fn delete_responses<'s, 'async_trait>(
        &'s self,
        key: ItemKey,
    ) -> LocalBoxFuture<'async_trait, Result<(), Self::Error>>
    where
        's: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Backend::Memory(inner) => inner.delete_responses(key),
            Backend::Redis(inner) => inner.delete_responses(key),
        }
    }

    #[inline]
    fn store_response<'r, 's, 'async_trait>(
        &'s self,
        item: Item<'r>,
    ) -> LocalBoxFuture<'async_trait, Result<(), Self::Error>>
    where
        'r: 'async_trait,
        's: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Backend::Memory(inner) => inner.store_response(item),
            Backend::Redis(inner) => inner.store_response(item),
        }
    }

    #[inline]
    fn get_responses<'s, 'async_trait, KI>(
        &'s self,
        keys: KI,
    ) -> LocalBoxFuture<'async_trait, Vec<Result<Option<Response<Self::Body>>, Self::Error>>>
    where
        's: 'async_trait,
        Self: 'async_trait,
        KI: IntoIterator<Item = Key> + 'async_trait,
    {
        match self {
            Backend::Memory(inner) => inner.get_responses(keys),
            Backend::Redis(inner) => inner.get_responses(keys),
        }
    }

    #[inline]
    fn delete_responses_multi<'s, 'async_trait, KI>(
        &'s self,
        keys: KI,
    ) -> LocalBoxFuture<'async_trait, Vec<Result<(), Self::Error>>>
    where
        's: 'async_trait,
        Self: 'async_trait,
        KI: IntoIterator<Item = ItemKey> + 'async_trait,
    {
        match self {
            Backend::Memory(inner) => inner.delete_responses_multi(keys),
            Backend::Redis(inner) => inner.delete_responses_multi(keys),
        }
    }

    #[inline]
    fn store_responses<'r, 's, 'async_trait, I>(
        &'s self,
        items: I,
    ) -> LocalBoxFuture<'async_trait, Vec<Result<(), Self::Error>>>
    where
        'r: 'async_trait,
        's: 'async_trait,
        Self: 'async_trait,
        I: IntoIterator<Item = Item<'r>> + 'async_trait,
    {
        match self {
            Backend::Memory(inner) => inner.store_responses(items),
            Backend::Redis(inner) => inner.store_responses(items),
        }
    }
}

mod memory;
mod redis;

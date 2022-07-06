use std::borrow::BorrowMut;

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
    pub fn new(name: String, config: serde_yaml::Value) -> Result<Self> {
        let backend_type = config
            .get("backend")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("backend type is not set for storage `{name}`"))?;

        let backend = match backend_type {
            "memory" => {
                let config =
                    serde_yaml::from_value::<memory::Config>(config).with_context(|| {
                        format!("invalid backend configuration for storage `{name}`")
                    })?;
                Backend::Memory(MemoryBackend::new(&config, name.clone()))
            }
            "redis" => {
                let config =
                    serde_yaml::from_value::<redis::Config>(config).with_context(|| {
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
    fn connect<'s, 'a>(&'s self) -> LocalBoxFuture<'a, Result<(), Self::Error>>
    where
        's: 'a,
        Self: 'a,
    {
        match self {
            Backend::Memory(inner) => inner.connect(),
            Backend::Redis(inner) => Storage::connect(inner),
        }
    }

    #[inline]
    fn get_response<'s, 'a>(
        &'s self,
        key: Key,
    ) -> LocalBoxFuture<'a, Result<Option<Response<Self::Body>>, Self::Error>>
    where
        's: 'a,
        Self: 'a,
    {
        match self {
            Backend::Memory(inner) => inner.get_response(key),
            Backend::Redis(inner) => inner.get_response(key),
        }
    }

    #[inline]
    fn delete_responses<'s, 'a>(
        &'s self,
        key: ItemKey,
    ) -> LocalBoxFuture<'a, Result<(), Self::Error>>
    where
        's: 'a,
        Self: 'a,
    {
        match self {
            Backend::Memory(inner) => inner.delete_responses(key),
            Backend::Redis(inner) => inner.delete_responses(key),
        }
    }

    #[inline]
    fn store_response<'s, 'a, R>(
        &'s self,
        item: Item<R>,
    ) -> LocalBoxFuture<'a, Result<(), Self::Error>>
    where
        's: 'a,
        Self: 'a,
        R: BorrowMut<Response<Self::Body>> + 'a,
    {
        match self {
            Backend::Memory(inner) => inner.store_response(item),
            Backend::Redis(inner) => inner.store_response(item),
        }
    }

    #[inline]
    fn get_responses<'s, 'a, KI>(
        &'s self,
        keys: KI,
    ) -> LocalBoxFuture<'a, Vec<Result<Option<Response<Self::Body>>, Self::Error>>>
    where
        's: 'a,
        Self: 'a,
        KI: IntoIterator<Item = Key> + 'a,
    {
        match self {
            Backend::Memory(inner) => inner.get_responses(keys),
            Backend::Redis(inner) => inner.get_responses(keys),
        }
    }

    #[inline]
    fn delete_responses_multi<'s, 'a, KI>(
        &'s self,
        keys: KI,
    ) -> LocalBoxFuture<'a, Vec<Result<(), Self::Error>>>
    where
        's: 'a,
        Self: 'a,
        KI: IntoIterator<Item = ItemKey> + 'a,
    {
        match self {
            Backend::Memory(inner) => inner.delete_responses_multi(keys),
            Backend::Redis(inner) => inner.delete_responses_multi(keys),
        }
    }

    #[inline]
    fn store_responses<'s, 'a, R, I>(
        &'s self,
        items: I,
    ) -> LocalBoxFuture<'a, Vec<Result<(), Self::Error>>>
    where
        's: 'a,
        Self: 'a,
        I: IntoIterator<Item = Item<R>> + 'a,
        R: BorrowMut<Response<Self::Body>> + 'a,
    {
        match self {
            Backend::Memory(inner) => inner.store_responses(items),
            Backend::Redis(inner) => inner.store_responses(items),
        }
    }
}

mod memory;
mod redis;

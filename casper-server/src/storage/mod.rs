use std::borrow::Cow;
use std::fmt;
use std::iter::IntoIterator;
use std::time::Duration;

use actix_http::body::{BoxBody, EitherBody, MessageBody};
use actix_http::header::HeaderMap;
use actix_http::{Response, StatusCode};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, StreamExt};

pub use backends::Backend;
pub(crate) use common::{compress_with_zstd, decode_headers, encode_headers};

pub(crate) const MAX_CONCURRENCY: usize = 100;

pub type Key = Bytes;

pub type Body = EitherBody<Bytes, BoxBody>;

pub struct Item<'a> {
    pub key: Key,
    pub status: StatusCode,
    pub headers: Cow<'a, HeaderMap>,
    pub body: Bytes,
    pub surrogate_keys: Vec<Key>,
    pub ttl: Duration,
}

impl Item<'static> {
    #[cfg(test)]
    pub fn new(key: impl Into<Key>, response: Response<Bytes>, ttl: Duration) -> Self {
        let (response, body) = response.into_parts();
        Item {
            key: key.into(),
            status: response.status(),
            headers: Cow::Owned(response.headers().clone()),
            body,
            surrogate_keys: Vec::new(),
            ttl,
        }
    }

    #[cfg(test)]
    pub fn new_with_skeys(
        key: impl Into<Key>,
        response: Response<Bytes>,
        surrogate_keys: Vec<impl Into<Key>>,
        ttl: Duration,
    ) -> Self {
        let (response, body) = response.into_parts();
        Item {
            key: key.into(),
            status: response.status(),
            headers: Cow::Owned(response.headers().clone()),
            body,
            surrogate_keys: surrogate_keys.into_iter().map(|sk| sk.into()).collect(),
            ttl,
        }
    }
}

#[derive(Clone)]
pub enum ItemKey {
    Primary(Key),
    Surrogate(Key),
}

impl fmt::Display for ItemKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ItemKey::Primary(key) => write!(f, "Primary({key:?})"),
            ItemKey::Surrogate(key) => write!(f, "Surrogate({key:?})"),
        }
    }
}

#[async_trait(?Send)]
pub trait Storage {
    type Body: MessageBody;
    type Error;

    fn name(&self) -> String;

    async fn connect(&self) -> Result<(), Self::Error>;

    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error>;

    async fn delete_responses(&self, key: ItemKey) -> Result<(), Self::Error>;

    async fn store_response<'a>(&self, item: Item<'a>) -> Result<(), Self::Error>;

    //
    // Provided implementation
    //

    async fn get_responses<KI>(
        &self,
        keys: KI,
    ) -> Vec<Result<Option<Response<Self::Body>>, Self::Error>>
    where
        KI: IntoIterator<Item = Key>,
    {
        // Create list of pending futures to poll them in parallel
        stream::iter(keys.into_iter().map(|key| self.get_response(key)))
            .buffered(MAX_CONCURRENCY)
            .collect()
            .await
    }

    async fn delete_responses_multi<KI>(&self, keys: KI) -> Vec<Result<(), Self::Error>>
    where
        KI: IntoIterator<Item = ItemKey>,
    {
        // Create list of pending futures to poll them in parallel
        stream::iter(keys.into_iter().map(|key| self.delete_responses(key)))
            .buffered(MAX_CONCURRENCY)
            .collect()
            .await
    }

    async fn store_responses<'a, I>(&self, items: I) -> Vec<Result<(), Self::Error>>
    where
        I: IntoIterator<Item = Item<'a>>,
    {
        // Create list of pending futures to poll them in parallel
        stream::iter(items.into_iter().map(|it| self.store_response(it)))
            .buffered(MAX_CONCURRENCY)
            .collect()
            .await
    }
}

mod backends;
mod common;

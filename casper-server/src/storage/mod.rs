use std::borrow::Cow;
use std::fmt;
use std::iter::IntoIterator;
use std::time::Duration;

use futures::stream::{self, StreamExt};
pub(crate) use ntex::http::body::Body;
use ntex::http::body::MessageBody;
use ntex::http::{HeaderMap, Response, StatusCode};
use ntex::util::Bytes;

pub use backends::Backend;
pub(crate) use common::{decode_headers, encode_headers};

pub type Key = Bytes;

#[derive(Debug)]
pub struct Item<'a> {
    pub key: Key,
    pub status: StatusCode,
    pub headers: Cow<'a, HeaderMap>,
    pub body: Bytes,
    pub surrogate_keys: Vec<Key>,
    pub ttl: Duration,
    pub encrypt: bool,
}

impl Item<'static> {
    #[cfg(test)]
    pub fn new(key: impl Into<Key>, response: Response<Bytes>, ttl: Duration) -> Self {
        let (response, body) = response.into_parts();
        Item {
            key: key.into(),
            status: response.status(),
            headers: Cow::Owned(response.headers().clone()),
            body: body.as_ref().unwrap().clone(),
            surrogate_keys: Vec::new(),
            ttl,
            encrypt: false,
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
            body: body.as_ref().unwrap().clone(),
            surrogate_keys: surrogate_keys.into_iter().map(|sk| sk.into()).collect(),
            ttl,
            encrypt: false,
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

pub trait Storage {
    type Body: MessageBody;
    type Error;

    const MAX_CONCURRENCY: usize = 100;

    fn name(&self) -> String;

    fn backend_type(&self) -> &'static str;

    async fn connect(&self) -> Result<(), Self::Error>;

    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error>;

    async fn delete_responses(&self, key: ItemKey) -> Result<(), Self::Error>;

    async fn store_response(&self, item: Item<'_>) -> Result<usize, Self::Error>;

    //
    // Provided implementation
    //

    async fn get_responses(
        &self,
        keys: impl IntoIterator<Item = Key>,
    ) -> Vec<Result<Option<Response<Self::Body>>, Self::Error>> {
        // Create list of pending futures to poll them in parallel
        stream::iter(keys.into_iter().map(|key| self.get_response(key)))
            .buffered(Self::MAX_CONCURRENCY)
            .collect()
            .await
    }

    async fn delete_responses_multi(
        &self,
        keys: impl IntoIterator<Item = ItemKey>,
    ) -> Vec<Result<(), Self::Error>> {
        // Create list of pending futures to poll them in parallel
        stream::iter(keys.into_iter().map(|key| self.delete_responses(key)))
            .buffered(Self::MAX_CONCURRENCY)
            .collect()
            .await
    }

    async fn store_responses(
        &self,
        items: impl IntoIterator<Item = Item<'_>>,
    ) -> Vec<Result<usize, Self::Error>> {
        // Create list of pending futures to poll them in parallel
        stream::iter(items.into_iter().map(|it| self.store_response(it)))
            .buffered(Self::MAX_CONCURRENCY)
            .collect()
            .await
    }
}

mod backends;
mod common;

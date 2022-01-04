#![allow(dead_code)]

use std::borrow::BorrowMut;
use std::error::Error as StdError;
use std::iter::IntoIterator;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bstr::BString;
use hyper::body::HttpBody;
use hyper::{Body, Response};
use mlua::{
    AnyUserData, ExternalResult, Result as LuaResult, Table as LuaTable, UserData, UserDataMethods,
};

use crate::response::LuaResponse;

pub type Key = BString;

pub struct Item<R> {
    pub key: Key,
    pub response: R,
    pub surrogate_keys: Vec<Key>,
    pub ttl: Option<Duration>,
}

impl<R> Item<R> {
    pub fn new(key: impl Into<Key>, response: R, ttl: Option<Duration>) -> Self {
        Item {
            key: key.into(),
            response,
            surrogate_keys: Vec::new(),
            ttl,
        }
    }

    pub fn new_with_skeys(
        key: impl Into<Key>,
        response: R,
        surrogate_keys: Vec<impl Into<Key>>,
        ttl: Option<Duration>,
    ) -> Self {
        Item {
            key: key.into(),
            response,
            surrogate_keys: surrogate_keys.into_iter().map(|sk| sk.into()).collect(),
            ttl,
        }
    }
}

pub enum ItemKey {
    Primary(Key),
    Surrogate(Key),
}

#[async_trait]
pub trait Storage {
    type Body: HttpBody;
    type Error;

    async fn get_responses<KI>(
        &self,
        keys: KI,
    ) -> Result<Vec<Option<Response<Self::Body>>>, Self::Error>
    where
        KI: IntoIterator<Item = Key> + Send,
        <KI as IntoIterator>::IntoIter: Send;

    async fn delete_responses<KI>(&self, keys: KI) -> Result<(), Self::Error>
    where
        KI: IntoIterator<Item = ItemKey> + Send,
        <KI as IntoIterator>::IntoIter: Send;

    async fn cache_responses<R, I>(&self, items: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = Item<R>> + Send,
        <I as IntoIterator>::IntoIter: Send,
        R: BorrowMut<Response<Self::Body>> + Send;

    //
    // Provided implementation
    //

    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error> {
        let mut responses = self.get_responses([key]).await?;
        Ok(responses.pop().flatten())
    }

    async fn delete_response(&self, key: Key) -> Result<(), Self::Error> {
        self.delete_responses([ItemKey::Primary(key)]).await
    }

    async fn cache_response<R>(&self, item: Item<R>) -> Result<(), Self::Error>
    where
        R: BorrowMut<Response<Self::Body>> + Send,
    {
        self.cache_responses([item]).await
    }
}

pub struct LuaStorage<T: Storage>(Arc<T>);

impl<T: Storage> LuaStorage<T> {
    pub fn new(storage: Arc<T>) -> Self {
        LuaStorage(storage)
    }
}

impl<T> UserData for LuaStorage<T>
where
    T: Storage<Body = Body> + Send + Sync + 'static,
    <T as Storage>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        //
        // Get
        //
        methods.add_async_function(
            "get_response",
            |_, (this, key): (AnyUserData, Key)| async move {
                let this = this.borrow::<Self>()?;
                let resp = this.0.get_response(key).await.to_lua_err()?;
                Ok(resp.map(|resp| {
                    let mut resp = LuaResponse::new(resp);
                    resp.is_cached = true;
                    resp
                }))
            },
        );

        methods.add_async_function(
            "get_responses",
            |_, (this, keys): (AnyUserData, Vec<Key>)| async move {
                let this = this.borrow::<Self>()?;
                let responses = this.0.get_responses(keys).await.to_lua_err()?;
                Ok(responses
                    .into_iter()
                    .map(|resp| {
                        resp.map(|resp| {
                            let mut resp = LuaResponse::new(resp);
                            resp.is_cached = true;
                            resp
                        })
                    })
                    .collect::<Vec<_>>())
            },
        );

        //
        // Delete
        //
        methods.add_async_function(
            "delete_response",
            |_, (this, key): (AnyUserData, Key)| async move {
                let this = this.borrow::<Self>()?;
                this.0.delete_response(key).await.to_lua_err()
            },
        );

        methods.add_async_function(
            "delete_responses",
            |_, (this, keys): (AnyUserData, LuaTable)| async move {
                let this = this.borrow::<Self>()?;
                if keys.raw_len() > 0 {
                    // Primary cache keys provided
                    let keys = keys
                        .raw_sequence_values::<Key>()
                        .collect::<LuaResult<Vec<_>>>()?;
                    this.0
                        .delete_responses(keys.into_iter().map(ItemKey::Primary))
                        .await
                        .to_lua_err()?;
                } else {
                    let surrogate_keys = keys
                        .raw_get::<_, Option<Vec<Key>>>("surrogate_keys")?
                        .unwrap_or_default()
                        .into_iter()
                        .map(ItemKey::Surrogate);
                    this.0.delete_responses(surrogate_keys).await.to_lua_err()?;
                }
                Ok(())
            },
        );

        //
        // Cache
        //
        methods.add_async_function(
            "cache_response",
            |_, (this, item): (AnyUserData, LuaTable)| async move {
                let this = this.borrow::<Self>()?;

                let key: Key = item.raw_get("key")?;
                let resp: AnyUserData = item.raw_get("response")?;
                let surrogate_keys: Option<Vec<Key>> = item.raw_get("surrogate_keys")?;
                let ttl: Option<f32> = item.raw_get("ttl")?;

                let mut resp = resp.borrow_mut::<LuaResponse>()?;

                // Read response body and save it to restore after caching
                let body = hyper::body::to_bytes(resp.body_mut()).await.to_lua_err()?;
                *resp.body_mut() = Body::from(body.clone());

                this.0
                    .cache_response(Item {
                        key,
                        response: resp.response_mut(),
                        surrogate_keys: surrogate_keys.unwrap_or_default(),
                        ttl: ttl.map(Duration::from_secs_f32),
                    })
                    .await
                    .to_lua_err()?;

                *resp.body_mut() = Body::from(body);

                Ok(())
            },
        );

        methods.add_async_function(
            "cache_responses",
            |_, (this, lua_items): (AnyUserData, Vec<LuaTable>)| async move {
                let this = this.borrow::<Self>()?;

                let (mut items, mut responses_any) = (Vec::new(), Vec::new());
                for it in lua_items {
                    let key: Key = it.raw_get("key")?;
                    let surrogate_keys: Option<Vec<Key>> = it.raw_get("surrogate_keys")?;
                    let ttl: Option<f32> = it.raw_get("ttl")?;
                    items.push((key, surrogate_keys, ttl));

                    let resp: AnyUserData = it.raw_get("response")?;
                    responses_any.push(resp);
                }

                // Convert each response from `AnyUserData` to an instance of `LuaResponse`
                let mut responses = Vec::new();
                for resp in &mut responses_any {
                    let mut resp = AnyUserData::borrow_mut::<LuaResponse>(resp)?;

                    // Read body and save it
                    // TODO: Enable concurrency
                    let body = hyper::body::to_bytes(resp.body_mut()).await.to_lua_err()?;
                    *resp.body_mut() = Body::from(body.clone());

                    responses.push((resp, body));
                }

                this.0
                    .cache_responses(
                        items
                            .into_iter()
                            .zip(&mut responses)
                            .map(|((key, surrogate_keys, ttl), (resp, _))| Item {
                                key,
                                response: resp.response_mut(),
                                surrogate_keys: surrogate_keys.unwrap_or_default(),
                                ttl: ttl.map(Duration::from_secs_f32),
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .to_lua_err()?;

                // Restore body
                for (mut resp, body) in responses {
                    *resp.body_mut() = Body::from(body);
                }

                Ok(())
            },
        );
    }
}

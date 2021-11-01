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

pub struct Item<K, R, SK> {
    pub key: K,
    pub response: R,
    pub surrogate_keys: Vec<SK>,
    pub ttl: Option<Duration>,
}

impl<K, R> Item<K, R, Vec<u8>> {
    #[cfg(test)]
    pub fn new(key: K, response: R, ttl: Option<Duration>) -> Self {
        Item {
            key,
            response,
            surrogate_keys: Vec::new(),
            ttl,
        }
    }
}

impl<K, R, SK> Item<K, R, SK> {
    pub fn new_with_skeys(
        key: K,
        response: R,
        surrogate_keys: Vec<SK>,
        ttl: Option<Duration>,
    ) -> Self {
        Item {
            key,
            response,
            surrogate_keys,
            ttl,
        }
    }
}

pub enum ItemKey<K> {
    Primary(K),
    Surrogate(K),
}

#[async_trait]
pub trait Storage {
    type Body: HttpBody;
    type Error;

    async fn get_responses<K, KI>(
        &self,
        keys: KI,
    ) -> Result<Vec<Option<Response<Self::Body>>>, Self::Error>
    where
        KI: IntoIterator<Item = K> + Send,
        <KI as IntoIterator>::IntoIter: Send,
        K: AsRef<[u8]> + Send;

    async fn delete_responses<K, KI>(&self, keys: KI) -> Result<(), Self::Error>
    where
        KI: IntoIterator<Item = ItemKey<K>> + Send,
        <KI as IntoIterator>::IntoIter: Send,
        K: AsRef<[u8]> + Send;

    async fn cache_responses<K, R, SK, I>(&self, items: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = Item<K, R, SK>> + Send,
        <I as IntoIterator>::IntoIter: Send,
        K: AsRef<[u8]> + Send,
        R: BorrowMut<Response<Self::Body>> + Send,
        SK: AsRef<[u8]> + Send;

    //
    // Provided implementation
    //

    async fn get_response<K>(&self, key: K) -> Result<Option<Response<Self::Body>>, Self::Error>
    where
        K: AsRef<[u8]> + Send,
    {
        let mut responses = self.get_responses([key]).await?;
        Ok(responses.pop().flatten())
    }

    async fn delete_response<K>(&self, key: K) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]> + Send,
    {
        self.delete_responses([ItemKey::Primary(key)]).await
    }

    async fn cache_response<K, R, SK>(&self, item: Item<K, R, SK>) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]> + Send,
        R: BorrowMut<Response<Self::Body>> + Send,
        SK: AsRef<[u8]> + Send,
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
            |_, (this, key): (AnyUserData, BString)| async move {
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
            |_, (this, keys): (AnyUserData, Vec<BString>)| async move {
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
            |_, (this, key): (AnyUserData, BString)| async move {
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
                        .raw_sequence_values::<BString>()
                        .collect::<LuaResult<Vec<_>>>()?;
                    this.0
                        .delete_responses(keys.into_iter().map(ItemKey::Primary))
                        .await
                        .to_lua_err()?;
                } else {
                    let surrogate_keys: Option<Vec<BString>> = keys.raw_get("surrogate_keys")?;
                    let surrogate_keys = surrogate_keys
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

                let key: BString = item.raw_get("key")?;
                let resp: AnyUserData = item.raw_get("response")?;
                let surrogate_keys: Option<Vec<BString>> = item.raw_get("surrogate_keys")?;
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
            |_, (this, items): (AnyUserData, Vec<LuaTable>)| async move {
                let this = this.borrow::<Self>()?;

                // Convert `Vec<LuaTable>` to a Vector of (key, response, surrogate_keys, ttl)
                let mut items_pre = Vec::new();
                for it in items {
                    let key: BString = it.raw_get("key")?;
                    let resp: AnyUserData = it.raw_get("response")?;
                    let surrogate_keys: Option<Vec<BString>> = it.raw_get("surrogate_keys")?;
                    let ttl: Option<f32> = it.raw_get("ttl")?;
                    items_pre.push((key, resp, surrogate_keys, ttl));
                }

                // Convert each `response` from `AnyUserData` to an instance of `LuaResponse`
                let mut items_ready = Vec::new();
                for (key, resp, surrogate_keys, ttl) in items_pre.iter_mut() {
                    let mut resp = AnyUserData::borrow_mut::<LuaResponse>(resp)?;

                    // Read body and save it
                    // TODO: Enable concurrency
                    let body = hyper::body::to_bytes(resp.body_mut()).await.to_lua_err()?;
                    *resp.body_mut() = Body::from(body.clone());

                    items_ready.push((key, resp, body, surrogate_keys, ttl));
                }

                this.0
                    .cache_responses(
                        items_ready
                            .iter_mut()
                            .map(|(key, resp, _, surrogate_keys, ttl)| Item {
                                key,
                                response: resp.response_mut(),
                                surrogate_keys: surrogate_keys.take().unwrap_or_default(),
                                ttl: ttl.map(Duration::from_secs_f32),
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .to_lua_err()?;

                // Restore body
                for (_, mut resp, body, _, _) in items_ready {
                    *resp.body_mut() = Body::from(body);
                }

                Ok(())
            },
        );
    }
}

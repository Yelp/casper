use std::borrow::BorrowMut;
use std::error::Error as StdError;
use std::fmt;
use std::iter::IntoIterator;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bstr::BString;
use futures::stream::{self, StreamExt};
use hyper::{body::HttpBody, Body, Response};
use mlua::{
    AnyUserData, ExternalResult, FromLua, Lua, Result as LuaResult, String as LuaString,
    Table as LuaTable, UserData, UserDataMethods, Value as LuaValue,
};
use ripemd::{Digest, Ripemd160};

use crate::response::LuaResponse;

pub type Key = BString;

pub struct Item<R> {
    pub key: Key,
    pub response: R,
    pub surrogate_keys: Vec<Key>,
    pub ttl: Duration,
}

impl<R> Item<R> {
    #[cfg(test)]
    pub fn new(key: impl Into<Key>, response: R, ttl: Duration) -> Self {
        Item {
            key: key.into(),
            response,
            surrogate_keys: Vec::new(),
            ttl,
        }
    }

    #[cfg(test)]
    pub fn new_with_skeys(
        key: impl Into<Key>,
        response: R,
        surrogate_keys: Vec<impl Into<Key>>,
        ttl: Duration,
    ) -> Self {
        Item {
            key: key.into(),
            response,
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
            ItemKey::Primary(key) => write!(f, "Primary({})", hex::encode(key)),
            ItemKey::Surrogate(key) => write!(f, "Surrogate({})", hex::encode(key)),
        }
    }
}

#[async_trait]
pub trait Storage {
    type Body: HttpBody + Send;
    type Error: Send;

    async fn get_response(&self, key: Key) -> Result<Option<Response<Self::Body>>, Self::Error>;

    async fn delete_responses(&self, key: ItemKey) -> Result<(), Self::Error>;

    async fn store_response<R>(&self, item: Item<R>) -> Result<(), Self::Error>
    where
        R: BorrowMut<Response<Self::Body>> + Send;

    //
    // Provided implementation
    //

    async fn get_responses<KI>(
        &self,
        keys: KI,
    ) -> Vec<Result<Option<Response<Self::Body>>, Self::Error>>
    where
        KI: IntoIterator<Item = Key> + Send,
        <KI as IntoIterator>::IntoIter: Send,
    {
        // Create list of pending futures to poll them in parallel
        stream::iter(keys.into_iter().map(|key| self.get_response(key)))
            .buffered(100)
            .collect()
            .await
    }

    async fn delete_responses_multi<KI>(&self, keys: KI) -> Vec<Result<(), Self::Error>>
    where
        KI: IntoIterator<Item = ItemKey> + Send,
        <KI as IntoIterator>::IntoIter: Send,
    {
        // Create list of pending futures to poll them in parallel
        stream::iter(keys.into_iter().map(|key| self.delete_responses(key)))
            .buffered(100)
            .collect()
            .await
    }

    async fn store_responses<R, I>(&self, items: I) -> Vec<Result<(), Self::Error>>
    where
        I: IntoIterator<Item = Item<R>> + Send,
        <I as IntoIterator>::IntoIter: Send,
        R: BorrowMut<Response<Self::Body>> + Send,
    {
        // Create list of pending futures to poll them in parallel
        stream::iter(items.into_iter().map(|it| self.store_response(it)))
            .buffered(100)
            .collect()
            .await
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
            |lua, (this, key): (AnyUserData, LuaValue)| async move {
                let this = this.borrow::<Self>()?;
                let key = calculate_primary_key(lua, key)?;
                let resp = this.0.get_response(key).await.to_lua_err()?;
                Ok(resp.map(|resp| {
                    let mut resp = LuaResponse::new(resp);
                    resp.is_cached = true;
                    resp
                }))
            },
        );

        //
        // Delete
        //
        methods.add_async_function(
            "delete_response",
            |lua, (this, key): (AnyUserData, LuaValue)| async move {
                let this = this.borrow::<Self>()?;
                let key = calculate_primary_key(lua, key)?;
                this.0
                    .delete_responses(ItemKey::Primary(key))
                    .await
                    .to_lua_err()
            },
        );

        // Temporary commented
        methods.add_async_function(
            "delete_responses",
            |lua, (this, keys): (AnyUserData, LuaTable)| async move {
                let this = this.borrow::<Self>()?;

                let primary_keys: Option<Vec<LuaValue>> = keys.raw_get("primary_keys")?;
                let surrogate_keys: Option<Vec<Key>> = keys.raw_get("surrogate_keys")?;

                let mut item_keys = Vec::with_capacity(
                    primary_keys.as_ref().map(|x| x.len()).unwrap_or(0)
                        + surrogate_keys.as_ref().map(|x| x.len()).unwrap_or(0),
                );

                if let Some(keys) = primary_keys {
                    for key in keys {
                        item_keys.push(ItemKey::Primary(calculate_primary_key(lua, key)?));
                    }
                }
                if let Some(keys) = surrogate_keys {
                    item_keys.extend(keys.into_iter().map(ItemKey::Surrogate));
                }

                let results = this.0.delete_responses_multi(item_keys).await;
                for r in results {
                    r.to_lua_err()?;
                }

                Ok(())
            },
        );

        //
        // Cache
        //
        methods.add_async_function(
            "store_response",
            |lua, (this, item): (AnyUserData, LuaTable)| async move {
                let this = this.borrow::<Self>()?;

                let key: LuaValue = item.raw_get("key")?;
                let resp: AnyUserData = item.raw_get("response")?;
                let surrogate_keys: Option<Vec<Key>> = item.raw_get("surrogate_keys")?;
                let ttl: f32 = item.raw_get("ttl")?;

                let mut resp = resp.borrow_mut::<LuaResponse>()?;

                // Read Response body and save it to restore after caching
                let body = hyper::body::to_bytes(resp.body_mut()).await.to_lua_err()?;
                *resp.body_mut() = Body::from(body.clone());

                this.0
                    .store_response(Item {
                        key: calculate_primary_key(lua, key)?,
                        response: resp.response_mut(),
                        surrogate_keys: surrogate_keys.unwrap_or_default(),
                        ttl: Duration::from_secs_f32(ttl),
                    })
                    .await
                    .to_lua_err()?;

                // Restore body
                *resp.body_mut() = Body::from(body);

                Ok(())
            },
        );
    }
}

/// Calculates primary key from Lua Value
/// The Value can be a string or a list of strings
fn calculate_primary_key(lua: &Lua, key: LuaValue) -> LuaResult<Key> {
    let mut hasher = Ripemd160::new();
    match key {
        LuaValue::Table(t) => {
            for v in t.raw_sequence_values::<LuaString>() {
                hasher.update(v?.as_bytes());
            }
        }
        _ => {
            let s = LuaString::from_lua(key, lua)?;
            hasher.update(s.as_bytes());
        }
    }
    Ok(hasher.finalize().to_vec().into())
}

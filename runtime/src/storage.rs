use std::borrow::BorrowMut;
use std::error::Error as StdError;
use std::iter::IntoIterator;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hyper::body::HttpBody;
use hyper::{Body, Response};
use mlua::{
    AnyUserData, ExternalResult, MultiValue, String as LuaString, Table as LuaTable, UserData,
    UserDataMethods, Value as LuaValue, Variadic,
};

use crate::response::LuaResponse;

#[async_trait(?Send)]
pub trait Storage {
    type Body: HttpBody;
    type Error;

    async fn get_responses<K, KI>(
        &self,
        keys: KI,
    ) -> Result<Vec<Option<Response<Self::Body>>>, Self::Error>
    where
        K: AsRef<[u8]>,
        KI: IntoIterator<Item = K>;

    async fn delete_responses<K, KI>(&self, keys: KI) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]>,
        KI: IntoIterator<Item = K>;

    async fn cache_responses<K, R, I>(&self, items: I) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]>,
        R: BorrowMut<Response<Self::Body>>,
        I: IntoIterator<Item = (K, R, Option<Duration>)>;

    //
    // Provided implementation
    //

    async fn get_response<K>(&self, key: &K) -> Result<Option<Response<Self::Body>>, Self::Error>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        let mut responses = self.get_responses([key]).await?;
        Ok(responses.pop().flatten())
    }

    async fn delete_response<K>(&self, key: &K) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]> + ?Sized,
    {
        self.delete_responses([key]).await
    }

    async fn cache_response<K, R>(
        &self,
        key: &K,
        resp: R,
        ttl: Option<Duration>,
    ) -> Result<(), Self::Error>
    where
        K: AsRef<[u8]> + ?Sized,
        R: BorrowMut<Response<Self::Body>>,
    {
        self.cache_responses([(key, resp, ttl)]).await
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
    T: Storage<Body = Body> + 'static,
    <T as Storage>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        //
        // Get
        //
        methods.add_async_function(
            "get_response",
            |_, (this, key): (AnyUserData, LuaString)| async move {
                let this = this.borrow::<Self>()?;
                let resp = this.0.get_response(&key).await.to_lua_err()?;
                Ok(resp.map(|resp| {
                    let mut resp = LuaResponse::new(resp);
                    resp.is_cached = true;
                    resp
                }))
            },
        );

        methods.add_async_function(
            "get_responses",
            |_, (this, keys): (AnyUserData, Vec<LuaString>)| async move {
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
            |_, (this, key): (AnyUserData, LuaString)| async move {
                let this = this.borrow::<Self>()?;
                this.0.delete_response(&key).await.to_lua_err()
            },
        );

        methods.add_async_function(
            "delete_responses",
            |_, (this, keys): (AnyUserData, Vec<LuaString>)| async move {
                let this = this.borrow::<Self>()?;
                this.0.delete_responses(keys).await.to_lua_err()
            },
        );

        //
        // Cache
        //
        methods.add_async_function("cache_response", |lua, args: MultiValue| async move {
            let (this, key, resp, ttl): (AnyUserData, LuaString, AnyUserData, Option<f32>) =
                lua.unpack_multi(args)?;
            let this = this.borrow::<Self>()?;
            let mut resp = resp.borrow_mut::<LuaResponse>()?;
            let ttl = ttl.map(Duration::from_secs_f32);
            this.0
                .cache_response(&key, resp.response_mut(), ttl)
                .await
                .to_lua_err()
        });

        methods.add_async_function(
            "cache_responses",
            |lua, (this, items): (AnyUserData, Vec<LuaTable>)| async move {
                let this = this.borrow::<Self>()?;

                // Convert `Vec<LuaTable>` to a Vector of extracted params: (key, response, ttl)
                let mut items_pre = Vec::new();
                for it in items {
                    let values = it
                        .raw_sequence_values::<LuaValue>()
                        .collect::<mlua::Result<Variadic<LuaValue>>>()?;
                    let (key, resp, ttl): (LuaString, AnyUserData, Option<f32>) =
                        lua.unpack_multi(lua.pack_multi(values)?)?;
                    items_pre.push((key, resp, ttl.map(Duration::from_secs_f32)));
                }

                // Convert each `response` from `AnyUserData` to an instance of `LuaResponse`
                let mut items_ready = Vec::new();
                for (key, resp, ttl) in &items_pre {
                    let resp = resp.borrow_mut::<LuaResponse>()?;
                    items_ready.push((key, resp, *ttl));
                }

                this.0
                    .cache_responses(
                        items_ready
                            .iter_mut()
                            .map(|(key, resp, ttl)| (key, resp.response_mut(), *ttl)),
                    )
                    .await
                    .to_lua_err()
            },
        );
    }
}

use std::error::Error as StdError;
use std::iter::IntoIterator;
use std::ops::DerefMut;
use std::time::{Duration, Instant};

use hyper::{Body, Response};
use mlua::{
    AnyUserData, ExternalResult, FromLua, Lua, Result as LuaResult, String as LuaString, Table,
    UserData, UserDataMethods, Value,
};
use ripemd::{Digest, Ripemd160};

use super::http::LuaResponse;
use crate::http::filter_hop_headers;
use crate::storage::{Item, ItemKey, Key, Storage};

pub struct LuaStorage<T: Storage>(T);

impl<T: Storage> LuaStorage<T> {
    pub fn new(storage: T) -> Self {
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
        #[allow(clippy::await_holding_refcell_ref)]
        methods.add_async_function(
            "get_response",
            |lua, (this, key): (AnyUserData, Value)| async move {
                let start = Instant::now();
                let this = this.borrow::<Self>()?;

                let key = calculate_primary_key(lua, key)?;
                let resp = this.0.get_response(key).await.to_lua_err()?;

                storage_counter_add!(1, "name" => this.0.name(), "operation" => "get");
                storage_histogram_rec!(start, "name" => this.0.name(), "operation" => "get");

                Ok(resp.map(|resp| {
                    let mut resp = LuaResponse::new(resp);
                    resp.is_stored = true;
                    resp
                }))
            },
        );

        //
        // Delete
        //
        #[allow(clippy::await_holding_refcell_ref)]
        methods.add_async_function(
            "delete_response",
            |lua, (this, key): (AnyUserData, Value)| async move {
                let start = Instant::now();
                let this = this.borrow::<Self>()?;
                let key = calculate_primary_key(lua, key)?;

                this.0
                    .delete_responses(ItemKey::Primary(key))
                    .await
                    .to_lua_err()?;

                storage_counter_add!(1, "name" => this.0.name(), "operation" => "delete");
                storage_histogram_rec!(start, "name" => this.0.name(), "operation" => "delete");

                Ok(())
            },
        );

        #[allow(clippy::await_holding_refcell_ref)]
        methods.add_async_function(
            "delete_responses",
            |lua, (this, keys): (AnyUserData, Table)| async move {
                let start = Instant::now();
                let this = this.borrow::<Self>()?;

                let primary_keys: Option<Vec<Value>> = keys.raw_get("primary_keys")?;
                let surrogate_keys: Option<Vec<LuaString>> = keys.raw_get("surrogate_keys")?;

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
                    item_keys.extend(
                        keys.into_iter()
                            .map(|s| ItemKey::Surrogate(Key::copy_from_slice(s.as_bytes()))),
                    );
                }

                let items_count: u64 = item_keys.len() as u64;
                let results = this.0.delete_responses_multi(item_keys).await;
                for r in results {
                    r.to_lua_err()?;
                }

                storage_counter_add!(items_count, "name" => this.0.name(), "operation" => "delete");
                storage_histogram_rec!(start, "name" => this.0.name(), "operation" => "delete");

                Ok(())
            },
        );

        //
        // Store
        //
        #[allow(clippy::await_holding_refcell_ref)]
        methods.add_async_function(
            "store_response",
            |lua, (this, item): (AnyUserData, Table)| async move {
                let start = Instant::now();
                let this = this.borrow::<Self>()?;

                let key: Value = item.raw_get("key")?;
                let resp: AnyUserData = item.raw_get("response")?;
                let surrogate_keys: Option<Vec<LuaString>> = item.raw_get("surrogate_keys")?;
                let ttl: f32 = item.raw_get("ttl")?;

                let mut resp = resp.borrow_mut::<LuaResponse>()?;

                // Read Response body and save it to restore after caching
                let body = hyper::body::to_bytes(resp.body_mut()).await.to_lua_err()?;
                *resp.body_mut() = Body::from(body.clone());

                // Remove hop by hop headers
                filter_hop_headers(resp.headers_mut());

                // Convert surrogate keys
                let surrogate_keys = surrogate_keys
                    .unwrap_or_default()
                    .into_iter()
                    .map(|s| Key::copy_from_slice(s.as_bytes()))
                    .collect();

                this.0
                    .store_response(Item {
                        key: calculate_primary_key(lua, key)?,
                        response: resp.deref_mut() as &mut Response<Body>,
                        surrogate_keys,
                        ttl: Duration::from_secs_f32(ttl),
                    })
                    .await
                    .to_lua_err()?;

                // Restore body
                *resp.body_mut() = Body::from(body);

                storage_counter_add!(1, "name" => this.0.name(), "operation" => "store");
                storage_histogram_rec!(start, "name" => this.0.name(), "operation" => "store");

                Ok(())
            },
        );
    }
}

/// Calculates primary key from Lua Value
/// The Value can be a string or a list of strings
fn calculate_primary_key(lua: &Lua, key: Value) -> LuaResult<Key> {
    let mut hasher = Ripemd160::new();
    match key {
        Value::Table(t) => {
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

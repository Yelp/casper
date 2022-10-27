use std::error::Error as StdError;
use std::iter::IntoIterator;
use std::time::{Duration, Instant};
use std::{borrow::Cow, ops::Deref};

use hyper::Body;
use mlua::{
    AnyUserData, FromLua, Lua, Result as LuaResult, String as LuaString, Table, UserData,
    UserDataMethods, Value,
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

impl<T: Storage> Deref for LuaStorage<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
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
                let this = this.borrow::<Self>()?;
                let start = Instant::now();

                let key = calculate_primary_key(lua, key)?;
                let resp = this.get_response(key).await.map_err(Into::into);

                storage_counter_add!(1, "name" => this.name(), "operation" => "get");
                storage_histogram_rec!(start, "name" => this.name(), "operation" => "get");

                let resp = lua_try!(resp);

                Ok(Ok(resp.map(|resp| {
                    let mut resp = LuaResponse::from(resp);
                    resp.is_stored = true;
                    resp
                })))
            },
        );

        methods.add_async_function(
            "get_responses",
            |lua, (this, keys): (AnyUserData, Table)| async move {
                let this = this.borrow::<Self>()?;
                let start = Instant::now();

                let keys = keys
                    .raw_sequence_values::<Value>()
                    .map(|key| key.and_then(|k| calculate_primary_key(lua, k)))
                    .collect::<LuaResult<Vec<_>>>()?;
                let items_count = keys.len() as u64;
                let results = this.get_responses(keys).await;

                storage_counter_add!(items_count, "name" => this.name(), "operation" => "get");
                storage_histogram_rec!(start, "name" => this.name(), "operation" => "get");

                // Convert results to a table of: { Response | string | boolean }
                // In case of error we return string
                // If response is not found then `false`
                results
                    .into_iter()
                    .map(|res| match res {
                        Ok(Some(resp)) => {
                            let mut resp = LuaResponse::from(resp);
                            resp.is_stored = true;
                            lua.create_userdata(resp).map(Value::UserData)
                        }
                        Ok(None) => Ok(Value::Boolean(false)),
                        Err(err) => lua
                            .create_string(&err.into().to_string())
                            .map(Value::String),
                    })
                    .collect::<LuaResult<Vec<_>>>()
            },
        );

        //
        // Delete
        //
        #[allow(clippy::await_holding_refcell_ref)]
        methods.add_async_function(
            "delete_response",
            |lua, (this, key): (AnyUserData, Value)| async move {
                let this = this.borrow::<Self>()?;
                let start = Instant::now();

                let key = calculate_primary_key(lua, key)?;
                let result = this.delete_responses(ItemKey::Primary(key)).await;

                storage_counter_add!(1, "name" => this.name(), "operation" => "delete");
                storage_histogram_rec!(start, "name" => this.name(), "operation" => "delete");

                lua_try!(result.map_err(Into::into));
                Ok(Ok(Value::Nil))
            },
        );

        #[allow(clippy::await_holding_refcell_ref)]
        methods.add_async_function(
            "delete_responses",
            |lua, (this, keys): (AnyUserData, Table)| async move {
                let this = this.borrow::<Self>()?;
                let start = Instant::now();

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
                let results = this.delete_responses_multi(item_keys).await;

                storage_counter_add!(items_count, "name" => this.name(), "operation" => "delete");
                storage_histogram_rec!(start, "name" => this.name(), "operation" => "delete");

                for r in results {
                    lua_try!(r.map_err(Into::into));
                }
                Ok(Ok(Value::Nil))
            },
        );

        //
        // Store
        //
        #[allow(clippy::await_holding_refcell_ref)]
        methods.add_async_function(
            "store_response",
            |lua, (this, item): (AnyUserData, Table)| async move {
                let this = this.borrow::<Self>()?;
                let start = Instant::now();

                let key: Value = item.raw_get("key")?;
                let resp: AnyUserData = item.raw_get("response")?;
                let surrogate_keys: Option<Vec<LuaString>> = item.raw_get("surrogate_keys")?;
                let ttl: f32 = item.raw_get("ttl")?;

                let mut resp = resp.borrow_mut::<LuaResponse>()?;

                // Read Response body (it's consumed and saved)
                let body = lua_try!(resp.body_mut().buffer().await).unwrap_or_default();

                // Remove hop by hop headers
                filter_hop_headers(resp.headers_mut());

                // Convert surrogate keys
                let surrogate_keys = surrogate_keys
                    .unwrap_or_default()
                    .into_iter()
                    .map(|s| Key::copy_from_slice(s.as_bytes()))
                    .collect();

                let result = this
                    .store_response(Item {
                        key: calculate_primary_key(lua, key)?,
                        status: resp.status(),
                        headers: Cow::Borrowed(resp.headers()),
                        body: body.clone(),
                        surrogate_keys,
                        ttl: Duration::from_secs_f32(ttl),
                    })
                    .await;

                storage_counter_add!(1, "name" => this.name(), "operation" => "store");
                storage_histogram_rec!(start, "name" => this.name(), "operation" => "store");

                lua_try!(result.map_err(Into::into));
                Ok(Ok(Value::Nil))
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

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use mlua::{chunk, Lua, Result};

    use super::*;
    use crate::storage::Backend;

    #[tokio::test]
    async fn test_storage() -> Result<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        let backend_config = serde_yaml::from_str(
            r#"
            backend: memory
            max_size: 1000000
        "#,
        )
        .unwrap();
        let backend = Backend::new("test".to_string(), backend_config).unwrap();
        let storage = LuaStorage::new(backend);

        lua.globals()
            .set("Response", lua.create_proxy::<LuaResponse>()?)?;

        lua.load(chunk! {
            // Try to get non-existent response
            local resp = $storage:get_response({"abc"})
            assert(resp1 == nil)

            // Store response and fetch it back
            resp = Response.new({
                status = 201,
                headers = {
                    hello = "world",
                },
                body = "test response 1",
            })
            local err = $storage:store_response({
                key = {"abc"},
                response = resp,
                surrogate_keys = {"skey1", "skey2"},
                ttl = 10,
            })
            assert(err == nil)
            resp = $storage:get_response({"abc"})
            assert(resp.status == 201)
            assert(resp:header("hello") == "world")
            assert(resp.body:data() == "test response 1")

            // Delete response
            $storage:delete_responses({surrogate_keys = {"skey2"}})
            resp = $storage:get_response({"abc"})
            assert(resp == nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }
}

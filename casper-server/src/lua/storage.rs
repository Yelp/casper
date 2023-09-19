use std::borrow::Cow;
use std::cell::RefCell;
use std::error::Error as StdError;
use std::iter::IntoIterator;
use std::time::{Duration, Instant};

use mlua::{
    ErrorContext, FromLua, Lua, Result as LuaResult, String as LuaString, Table, UserData,
    UserDataMethods, UserDataRefMut, Value,
};
use tracing::{instrument, Span};

use super::http::LuaResponse;
use crate::http::filter_hop_headers;
use crate::storage::{Body, Item, ItemKey, Key, Storage};

pub struct LuaStorage<T: Storage>(T);

impl<T: Storage> LuaStorage<T> {
    pub const fn new(storage: T) -> Self {
        LuaStorage(storage)
    }
}

type LuaDoubleResult<T> = LuaResult<Result<T, String>>;

impl<T> LuaStorage<T>
where
    T: Storage<Body = Body> + 'static,
    <T as Storage>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    /// Fetches a response from the storage
    ///
    /// Returns `nil` if response is not found.
    /// In case of error returns a second value with error message.
    #[instrument(
        skip_all,
        fields(storage.name = self.0.name(), storage.backend = self.0.backend_type(), otel.status_message)
    )]
    async fn get_response<'l>(
        &self,
        lua: &'l Lua,
        key: Value<'l>,
    ) -> LuaDoubleResult<Option<LuaResponse>> {
        let start = Instant::now();

        let key = calculate_primary_key(lua, key).context("failed to calculate primary key")?;
        let resp = self.0.get_response(key).await.map_err(|err| {
            let err = err.into();
            Span::current().record("otel.status_message", err.to_string());
            err
        });

        storage_counter_add!(1, "name" => self.0.name(), "operation" => "get");
        storage_histogram_rec!(start, "name" => self.0.name(), "operation" => "get");

        let resp = lua_try!(resp);
        Ok(Ok(resp.map(|resp| {
            let mut resp = LuaResponse::from(resp);
            resp.is_stored = true;
            resp
        })))
    }

    /// Fetches responses from the storage
    ///
    /// Returns a table of: { Response | string | false }
    ///   string - error message
    ///   `false` - if response is not found
    #[instrument(
        skip_all,
        fields(storage.name = self.0.name(), storage.backend = self.0.backend_type(), otel.status_code)
    )]
    async fn get_responses<'l>(&self, lua: &'l Lua, keys: Table<'l>) -> LuaResult<Vec<Value<'l>>> {
        let start = Instant::now();

        let keys = keys
            .sequence_values::<Value>()
            .map(|key| key.and_then(|k| calculate_primary_key(lua, k)))
            .collect::<LuaResult<Vec<_>>>()
            .context("failed to calculate primary keys")?;
        let items_count = keys.len() as u64;
        let results = self.0.get_responses(keys).await;

        storage_counter_add!(items_count, "name" => self.0.name(), "operation" => "get_multi");
        storage_histogram_rec!(start, "name" => self.0.name(), "operation" => "get_multi");

        // If any of them failed, mark span as error
        if results.iter().any(|r| r.is_err()) {
            Span::current().record("otel.status_code", "ERROR");
        }

        // Convert results to a table of: { Response | string | false }
        // In case of error we return string
        // If response is not found then `false`
        results
            .into_iter()
            .map(|res| match res {
                Ok(Some(resp)) => {
                    let mut resp = LuaResponse::from(resp);
                    resp.is_stored = true;
                    Ok(Value::UserData(lua.create_userdata(resp)?))
                }
                Ok(None) => Ok(Value::Boolean(false)),
                Err(err) => Ok(Value::String(lua.create_string(err.into().to_string())?)),
            })
            .collect::<LuaResult<Vec<_>>>()
    }

    /// Deletes responses from the storage
    ///
    /// Returns `true` if all responses were deleted.
    ///
    /// In case of errors returns `false` and a table of: { string | true }
    ///   string - error message
    ///   `true` - if response was deleted
    #[instrument(
        skip_all,
        fields(storage.name = self.0.name(), storage.backend = self.0.backend_type(), otel.status_code)
    )]
    async fn delete_responses<'l>(
        &self,
        lua: &'l Lua,
        keys: Table<'l>,
    ) -> LuaResult<(bool, Option<Vec<Value<'l>>>)> {
        let start = Instant::now();

        let primary_keys: Option<Vec<Value>> = keys.raw_get("keys").context("invalid `keys`")?;
        let surrogate_keys: Option<Vec<LuaString>> = keys
            .raw_get("surrogate_keys")
            .context("invalid `surrogate_keys`")?;

        let mut item_keys = Vec::with_capacity(
            primary_keys.as_ref().map(|x| x.len()).unwrap_or(0)
                + surrogate_keys.as_ref().map(|x| x.len()).unwrap_or(0),
        );

        if let Some(keys) = primary_keys {
            for key in keys {
                let key =
                    calculate_primary_key(lua, key).context("failed to calculate primary key")?;
                item_keys.push(ItemKey::Primary(key));
            }
        }
        if let Some(keys) = surrogate_keys {
            item_keys.extend(
                keys.into_iter()
                    .map(|s| ItemKey::Surrogate(Key::copy_from_slice(s.as_bytes()))),
            );
        }

        let items_count: u64 = item_keys.len() as u64;
        let results = self.0.delete_responses_multi(item_keys).await;

        storage_counter_add!(items_count, "name" => self.0.name(), "operation" => "delete");
        storage_histogram_rec!(start, "name" => self.0.name(), "operation" => "delete");

        if results.iter().all(|r| r.is_ok()) {
            return Ok((true, None));
        }

        let results = results
            .into_iter()
            .map(|res| match res {
                Ok(_) => Ok(Value::Boolean(true)),
                Err(err) => Ok(Value::String(lua.create_string(&err.into().to_string())?)),
            })
            .collect::<LuaResult<Vec<_>>>()?;
        Span::current().record("otel.status_code", "ERROR");
        Ok((false, Some(results)))
    }

    /// Stores a response in the storage.
    ///
    /// Returns `true` if the response was stored.
    /// In case of errors returns `nil` and a string with error message.
    #[instrument(
        skip_all,
        fields(storage.name = self.0.name(), storage.backend = self.0.backend_type(), otel.status_message)
    )]
    async fn store_response<'l>(&self, lua: &'l Lua, item: Table<'l>) -> LuaDoubleResult<bool> {
        let start = Instant::now();

        let key: Value = item.raw_get("key").context("invalid `key`")?;
        let mut resp: UserDataRefMut<LuaResponse> =
            item.raw_get("response").context("invalid `response`")?;
        let surrogate_keys: Option<Vec<LuaString>> = item
            .raw_get("surrogate_keys")
            .context("invalid `surrogate_keys`")?;
        let ttl: f32 = item.raw_get("ttl").context("invalid `ttl`")?;

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

        let result = self
            .0
            .store_response(Item {
                key: calculate_primary_key(lua, key).context("failed to calculate primary key")?,
                status: resp.status(),
                headers: Cow::Borrowed(resp.headers()),
                body,
                surrogate_keys,
                ttl: Duration::from_secs_f32(ttl),
            })
            .await;

        storage_counter_add!(1, "name" => self.0.name(), "operation" => "store");
        storage_histogram_rec!(start, "name" => self.0.name(), "operation" => "store");

        Ok(result.map(|_| true).map_err(|err| {
            let err = err.into().to_string();
            Span::current().record("otel.status_message", &err);
            err
        }))
    }

    /// Stores responses in the storage.
    ///
    /// Returns `true` if all the responses were stored.
    /// In case of errors returns `false` and a table of: { string | true }
    ///   string - error message
    ///   `true` - if response was stored
    #[instrument(
        skip_all,
        fields(storage.name = self.0.name(), storage.backend = self.0.backend_type(), otel.status_code)
    )]
    async fn store_responses<'l>(
        &self,
        lua: &'l Lua,
        lua_items: Table<'l>,
    ) -> LuaResult<(bool, Option<Vec<Value<'l>>>)> {
        let start = Instant::now();

        // Read rest of the fields
        let mut items = Vec::with_capacity(lua_items.raw_len());
        for (i, item) in lua_items.sequence_values::<Table>().enumerate() {
            let item = item?;
            let key: Value = item
                .raw_get("key")
                .with_context(|_| format!("invalid `key` #{}", i + 1))?;
            let mut resp: UserDataRefMut<LuaResponse> = item
                .raw_get("response")
                .with_context(|_| format!("invalid `response` #{}", i + 1))?;
            let surrogate_keys: Option<Vec<LuaString>> = item
                .raw_get("surrogate_keys")
                .with_context(|_| format!("invalid `surrogate_keys` #{}", i + 1))?;
            let ttl: f32 = item
                .raw_get("ttl")
                .with_context(|_| format!("invalid `ttl` #{}", i + 1))?;

            // Read Response body (it's consumed and saved)
            let body = resp.body_mut().buffer().await?.unwrap_or_default();

            // Remove hop by hop headers
            filter_hop_headers(resp.headers_mut());

            // Calculate primary key
            let key = calculate_primary_key(lua, key)
                .with_context(|_| format!("failed to calculate primary key #{}", i + 1))?;

            // Convert surrogate keys
            let surrogate_keys = surrogate_keys
                .unwrap_or_default()
                .into_iter()
                .map(|s| Key::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>();

            items.push((i, key, resp, body, surrogate_keys, ttl));
        }

        // Transform items elements from tuple to Item struct
        let items = items
            .iter()
            .map(|(_, key, resp, body, surrogate_keys, ttl)| Item {
                key: key.clone(),
                status: resp.status(),
                headers: Cow::Borrowed(resp.headers()),
                body: body.clone(),
                surrogate_keys: surrogate_keys.clone(),
                ttl: Duration::from_secs_f32(*ttl),
            })
            .collect::<Vec<_>>();

        let items_len = items.len();
        let results = self.0.store_responses(items).await;

        storage_counter_add!(items_len as u64, "name" => self.0.name(), "operation" => "store_multi");
        storage_histogram_rec!(start, "name" => self.0.name(), "operation" => "store_multi");

        // If all responses were stored then return `true`
        if results.iter().all(|r| r.is_ok()) {
            return Ok((true, None));
        }

        let results = results
            .into_iter()
            .map(|res| match res {
                Ok(_) => Ok(Value::Boolean(true)),
                Err(err) => Ok(Value::String(lua.create_string(&err.into().to_string())?)),
            })
            .collect::<LuaResult<Vec<_>>>()?;
        Span::current().record("otel.status_code", "ERROR");
        Ok((false, Some(results)))
    }
}

impl<T> UserData for LuaStorage<T>
where
    T: Storage<Body = Body> + 'static,
    <T as Storage>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_method("get_response", |lua, this, args| {
            this.get_response(lua, args)
        });

        methods.add_async_method("get_responses", |lua, this, args| {
            this.get_responses(lua, args)
        });

        methods.add_async_method("delete_responses", |lua, this, args| {
            this.delete_responses(lua, args)
        });

        methods.add_async_method("store_response", |lua, this, args| {
            this.store_response(lua, args)
        });

        methods.add_async_method("store_responses", |lua, this, args| {
            this.store_responses(lua, args)
        });
    }
}

/// Calculates primary key from Lua Value
/// The Value can be a string or a list of strings
fn calculate_primary_key(lua: &Lua, key: Value) -> LuaResult<Key> {
    thread_local! {
        static BLAKE3: RefCell<blake3::Hasher> = RefCell::new(blake3::Hasher::new());
    }

    BLAKE3.with(|hasher| {
        let mut hasher = hasher.borrow_mut();
        let hasher = hasher.reset();

        match key {
            Value::Table(t) => {
                for v in t.sequence_values::<LuaString>() {
                    hasher.update(v?.as_bytes());
                }
            }
            _ => {
                let s = LuaString::from_lua(key, lua)?;
                hasher.update(s.as_bytes());
            }
        }

        Ok(Key::from(hasher.finalize().as_bytes().to_vec()))
    })
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    use super::*;
    use crate::storage::Backend;

    #[ntex::test]
    async fn test_storage() -> Result<()> {
        let lua = Lua::new();

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
            assert(resp1 == nil, "response should not exist")

            // Store response and fetch it back
            resp = Response.new({
                status = 201,
                headers = { hello = "world" },
                body = "test response 1",
            })
            local ok, err = $storage:store_response({
                key = {"a", "bc"}, // key parts should be concatenated
                response = resp,
                surrogate_keys = {"skey1", "skey2"},
                ttl = 10,
            })
            assert(ok and err == nil)
            resp = $storage:get_response("abc")
            assert(resp.status == 201)
            assert(resp:header("hello") == "world")
            assert(resp.body:to_string() == "test response 1")

            // Delete response
            $storage:delete_responses({surrogate_keys = {"skey2"}})
            resp, err = $storage:get_response({"abc"})
            assert(resp == nil and err == nil)
        })
        .exec_async()
        .await
    }

    #[ntex::test]
    async fn test_storage_multi() -> Result<()> {
        let lua = Lua::new();

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
            // Try to get non-existent responses
            local responses = $storage:get_responses({{"abc"}, "def"})
            assert(responses[1] == false, "response#1 should not exist")
            assert(responses[2] == false, "response#2 should not exist")

            // Store few responses with different keys and surrogate keys
            local ok, err = $storage:store_responses({
                {
                    key = {"a", "bc"}, // key parts should be concatenated
                    response = Response.new({
                        status = 201,
                        headers = { hello = "world" },
                        body = "test response 1",
                    }),
                    surrogate_keys = {"skey1", "skey2"},
                    ttl = 10,
                },
                {
                    key = "def",
                    response = Response.new({
                        status = 202,
                        headers = { welcome = "rust" },
                        body = "test response 2",
                    }),
                    surrogate_keys = {"skey2", "skey3"},
                    ttl = 10,
                }
            })
            assert(ok == true and err == nil, "responses should be stored")

            // Fetch them back
            responses = $storage:get_responses({"abc", "cde", "def"})
            assert(responses[1].status == 201)
            assert(responses[1]:header("hello") == "world")
            assert(responses[1].body:to_string() == "test response 1")
            assert(responses[2] == false, "response#2 should not exist")
            assert(responses[3].status == 202)
            assert(responses[3]:header("welcome") == "rust")
            assert(responses[3].body:to_string() == "test response 2")

            // Delete responses
            $storage:delete_responses({keys = {"abc"}})
            responses = $storage:get_responses({"abc"})
            assert(responses[1] == false, "response should not exist")

            $storage:delete_responses({surrogate_keys = {"skey3"}})
            responses = $storage:get_responses({"def"})
            assert(responses[1] == false, "response should not exist")
        })
        .exec_async()
        .await
    }

    // TODO: test wrong arguments (panic)
}

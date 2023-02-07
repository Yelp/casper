use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};

use actix_http::header::{HeaderMap, HeaderName, HeaderValue};
use mlua::{
    AnyUserData, ExternalError, ExternalResult, FromLua, Function, IntoLua, Lua, MetaMethod,
    RegistryKey, Result as LuaResult, String as LuaString, Table, UserData, UserDataMethods, Value,
    Variadic,
};

use super::super::Regex;

pub trait LuaHttpHeadersExt {
    /// Returns a first value of the header of Nil if not found.
    fn get<'lua>(&self, lua: &'lua Lua, name: &str) -> LuaResult<Value<'lua>>;

    /// Returns a table with the header values of Nil if not found.
    fn get_all<'lua>(&self, lua: &'lua Lua, name: &str) -> LuaResult<Value<'lua>>;

    /// Returns a number of values of the header.
    fn get_cnt(&self, lua: &Lua, name: &str) -> LuaResult<usize>;

    /// Checks if the header matches a regular expression specified by `pattern`.
    fn is_match(&self, lua: &Lua, name: &str, pattern: String) -> LuaResult<bool>;

    /// Removes all values of the header.
    fn del(&mut self, name: &str) -> LuaResult<()>;

    /// Appends a value to the http header.
    fn add(&mut self, name: &str, value: &[u8]) -> LuaResult<()>;

    /// Sets the header value removing all existing values.
    fn set(&mut self, name: &str, value: &[u8]) -> LuaResult<()>;

    /// Converts internal representation of headers to a Lua table with a specific metatable
    /// for case-insensitive access.
    fn to_table<'lua>(&self, lua: &'lua Lua, names_filter: Value<'lua>) -> LuaResult<Table<'lua>>;
}

#[derive(Clone, Debug, Default)]
pub struct LuaHttpHeaders(HeaderMap);

impl LuaHttpHeaders {
    #[inline]
    pub fn new() -> Self {
        LuaHttpHeaders(HeaderMap::new())
    }

    #[inline]
    pub fn with_capacity(n: usize) -> Self {
        LuaHttpHeaders(HeaderMap::with_capacity(n))
    }

    #[inline]
    pub fn into_inner(self) -> HeaderMap {
        self.0
    }
}

impl Deref for LuaHttpHeaders {
    type Target = HeaderMap;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LuaHttpHeaders {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'lua> FromLua<'lua> for LuaHttpHeaders {
    fn from_lua(value: Value<'lua>, lua: &'lua Lua) -> LuaResult<Self> {
        match value {
            Value::Nil => Ok(Self::new()),
            Value::Table(table) => {
                let mut headers = Self::with_capacity(table.raw_len() as usize);
                for kv in table.pairs::<String, Value>() {
                    let (name, value) = kv?;
                    // Maybe `value` is a list of header values
                    if let Value::Table(values) = value {
                        let name = HeaderName::from_bytes(name.as_bytes()).into_lua_err()?;
                        for value in values.raw_sequence_values::<LuaString>() {
                            headers.append(
                                name.clone(),
                                HeaderValue::from_bytes(value?.as_bytes()).into_lua_err()?,
                            );
                        }
                    } else {
                        let value = lua.unpack::<LuaString>(value)?;
                        headers.append(
                            HeaderName::from_bytes(name.as_bytes()).into_lua_err()?,
                            HeaderValue::from_bytes(value.as_bytes()).into_lua_err()?,
                        );
                    }
                }
                Ok(headers)
            }
            Value::UserData(ud) => {
                if let Ok(headers) = ud.borrow::<Self>() {
                    Ok(headers.clone())
                } else {
                    Err("cannot make headers from wrong userdata".into_lua_err())
                }
            }
            val => {
                let type_name = val.type_name();
                let msg = format!("cannot make headers from {type_name}");
                Err(msg.into_lua_err())
            }
        }
    }
}

impl From<HeaderMap> for LuaHttpHeaders {
    #[inline]
    fn from(v: HeaderMap) -> Self {
        LuaHttpHeaders(v)
    }
}

impl From<LuaHttpHeaders> for HeaderMap {
    #[inline]
    fn from(headers: LuaHttpHeaders) -> Self {
        headers.into_inner()
    }
}

impl UserData for LuaHttpHeaders {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_function("new", |lua, arg: Value| LuaHttpHeaders::from_lua(arg, lua));

        methods.add_method("get", |lua, this, name: String| {
            LuaHttpHeadersExt::get(&this.0, lua, &name)
        });

        methods.add_method("get_all", |lua, this, name: String| {
            LuaHttpHeadersExt::get_all(&this.0, lua, &name)
        });

        methods.add_method("get_cnt", |lua, this, name: String| {
            LuaHttpHeadersExt::get_cnt(&this.0, lua, &name)
        });

        methods.add_method(
            "is_match",
            |lua, this, (name, pattern): (String, String)| {
                LuaHttpHeadersExt::is_match(&this.0, lua, &name, pattern)
            },
        );

        methods.add_method_mut("del", |_, this, name: String| this.del(&name));

        methods.add_method_mut("add", |_, this, (name, value): (String, LuaString)| {
            this.add(&name, value.as_bytes())
        });

        methods.add_method_mut("set", |_, this, (name, value): (String, LuaString)| {
            this.set(&name, value.as_bytes())
        });

        methods.add_method("to_table", |lua, this, filter| this.to_table(lua, filter));

        methods.add_meta_method(MetaMethod::Index, |lua, this, name: String| {
            LuaHttpHeadersExt::get_all(&this.0, lua, &name)
        });

        methods.add_meta_method_mut(
            MetaMethod::NewIndex,
            |lua, this, (name, value): (String, Value)| {
                match value {
                    Value::Table(t) => {
                        let name = HeaderName::from_bytes(name.as_bytes()).into_lua_err()?;
                        for (i, v) in t.raw_sequence_values::<LuaString>().enumerate() {
                            let hdr_value =
                                HeaderValue::from_bytes(v?.as_bytes()).into_lua_err()?;
                            if i == 0 {
                                this.insert(name.clone(), hdr_value);
                            } else {
                                this.append(name.clone(), hdr_value);
                            }
                        }
                    }
                    Value::Nil => {
                        this.remove(&name);
                    }
                    _ => {
                        this.set(&name, LuaString::from_lua(value, lua)?.as_bytes())?;
                    }
                }
                Ok(())
            },
        );

        methods.add_meta_function(MetaMethod::Iter, |lua, ud: AnyUserData| {
            let this = ud.borrow::<Self>()?;
            let it = LuaHttpHeadersIter {
                names: this.0.keys().cloned().collect(),
                next: 0,
            };

            let next = lua.create_function(move |lua, ud: Table| {
                let this = ud.raw_get::<_, AnyUserData>(1)?;
                let this = this.borrow_mut::<Self>()?;
                let it = ud.raw_get::<_, AnyUserData>(2)?;
                let mut it = it.borrow_mut::<LuaHttpHeadersIter>()?;

                it.next += 1;
                match it.names.get(it.next - 1) {
                    Some(hdr_name) => {
                        let name = Value::String(lua.create_string(hdr_name.as_str())?);
                        let values = LuaHttpHeadersExt::get_all(&this.0, lua, hdr_name.as_str())?;
                        Ok(Variadic::from_iter([name, values]))
                    }
                    None => Ok(Variadic::new()),
                }
            })?;

            Ok((next, [ud.clone(), lua.create_userdata(it)?]))
        });
    }
}

struct LuaHttpHeadersIter {
    names: Vec<HeaderName>,
    next: usize,
}

impl UserData for LuaHttpHeadersIter {}

impl LuaHttpHeadersExt for HeaderMap {
    fn get<'lua>(&self, lua: &'lua Lua, name: &str) -> LuaResult<Value<'lua>> {
        if let Some(val) = self.get(name) {
            return lua.create_string(val.as_bytes()).map(Value::String);
        }
        Ok(Value::Nil)
    }

    fn get_all<'lua>(&self, lua: &'lua Lua, name: &str) -> LuaResult<Value<'lua>> {
        let vals = self.get_all(name);
        let vals = vals
            .map(|val| lua.create_string(val.as_bytes()))
            .collect::<LuaResult<Vec<_>>>()?;
        if vals.is_empty() {
            return Ok(Value::Nil);
        }
        vals.into_lua(lua)
    }

    fn get_cnt(&self, _: &Lua, name: &str) -> LuaResult<usize> {
        Ok(self.get_all(name).count())
    }

    fn is_match(&self, lua: &Lua, name: &str, pattern: String) -> LuaResult<bool> {
        if let Ok(regex) = Regex::new(lua, pattern) {
            for hdr_val in self.get_all(name) {
                if let Ok(val) = hdr_val.to_str() {
                    if regex.is_match(val) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn del(&mut self, name: &str) -> LuaResult<()> {
        self.remove(name);
        Ok(())
    }

    fn add(&mut self, name: &str, value: &[u8]) -> LuaResult<()> {
        let name = HeaderName::from_bytes(name.as_bytes()).into_lua_err()?;
        let value = HeaderValue::from_bytes(value).into_lua_err()?;
        self.append(name, value);
        Ok(())
    }

    fn set(&mut self, name: &str, value: &[u8]) -> LuaResult<()> {
        let name = HeaderName::from_bytes(name.as_bytes()).into_lua_err()?;
        let value = HeaderValue::from_bytes(value).into_lua_err()?;
        self.insert(name, value);
        Ok(())
    }

    fn to_table<'lua>(&self, lua: &'lua Lua, names_filter: Value<'lua>) -> LuaResult<Table<'lua>> {
        let names_filter = match names_filter {
            Value::Nil => None,
            Value::Table(t) => Some(
                t.raw_sequence_values::<LuaString>()
                    .map(|s| s.and_then(|s| HeaderName::from_bytes(s.as_bytes()).into_lua_err()))
                    .collect::<LuaResult<HashSet<_>>>()?,
            ),
            val => {
                let type_name = val.type_name();
                let reason = format!("invalid names filter: expected table, got {type_name}");
                Err(reason.into_lua_err())?
            }
        };

        let mut headers = HashMap::new();
        for (name, value) in self {
            if let Some(ref names) = names_filter {
                if !names.contains(name) {
                    continue;
                }
            }

            headers
                .entry(name.to_string())
                .or_insert_with(Vec::new)
                .push(lua.create_string(value.as_bytes())?);
        }

        let lua_headers = Table::from_lua(headers.into_lua(lua)?, lua)?;
        set_headers_metatable(lua, lua_headers.clone())?;

        Ok(lua_headers)
    }
}

fn set_headers_metatable(lua: &Lua, headers: Table) -> LuaResult<()> {
    struct MetatableHelperKey(RegistryKey);

    if let Some(key) = lua.app_data_ref::<MetatableHelperKey>() {
        return lua.registry_value::<Function>(&key.0)?.call(headers);
    }

    // Create new metatable helper and cache it
    let metatable_helper: Function = lua
        .load(
            r#"
            local headers = ...
            local metatable = {
                -- A mapping from a normalized (all lowercase) header name to its
                -- first-seen case, populated the first time a header is seen.
                normalized_to_original_case = {},
            }

            -- Add existing keys
            for key in pairs(headers) do
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                metatable.normalized_to_original_case[normalized_key] = key
            end

            -- When looking up a key that doesn't exist from the headers table, check
            -- if we've seen this header with a different casing, and return it if so.
            metatable.__index = function(tbl, key)
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                local original_key = metatable.normalized_to_original_case[normalized_key]
                if original_key ~= nil and original_key ~= key then
                    return tbl[original_key]
                end
                return nil
            end

            -- When adding a new key to the headers table, check if we've seen this
            -- header with a different casing, and set that key instead.
            metatable.__newindex = function(tbl, key, value)
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                local original_key = metatable.normalized_to_original_case[normalized_key]
                if original_key == nil then
                    metatable.normalized_to_original_case[normalized_key] = key
                    original_key = key
                end
                rawset(tbl, original_key, value)
            end

            setmetatable(headers, metatable)
        "#,
        )
        .into_function()?;

    // Store the helper in the Lua registry
    let registry_key = lua.create_registry_value(metatable_helper.clone())?;
    lua.set_app_data(MetatableHelperKey(registry_key));

    metatable_helper.call(headers)
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    use super::LuaHttpHeaders;

    #[test]
    fn test_headers() -> Result<()> {
        let lua = Lua::new();

        lua.globals()
            .set("Headers", lua.create_proxy::<LuaHttpHeaders>()?)?;

        lua.load(chunk! {
            local headers = Headers.new()

            headers:add("hello", "world")
            assert(headers:get("hello") == "world")
            assert(headers:get_cnt("hello") == 1)
            assert(headers:get_cnt("none") == 0)
            assert(headers:is_match("hello", ""))

            headers:add("hello", "bla")
            assert(table.concat(headers:get_all("hello")) == "worldbla")

            // Test matching
            assert(headers:is_match("hello", ".+"))
            assert(headers:is_match("hello", "world"))

            // Test iter
            local res = {}
            for k, v in headers do
                table.insert(res, k)
                table.insert(res, v[1])
                table.insert(res, v[2])
            end
            assert(table.concat(res) == "helloworldbla")

            // Test index
            assert(headers["HeLLO"][1] == "world")

            // Test newindex
            headers["hello"] = nil
            assert(headers:get("hello") == nil)
            headers["test"] = {"abc", 321}
            assert(table.concat(headers:get_all("tesT")) == "abc321")
            headers["test2"] = "cba"
            assert(table.concat(headers:get_all("Test2")) == "cba")
            headers:del("test2")
            assert(headers["test2"] == nil)

            headers:set("foo", "bar")
            assert(table.concat(headers:get_all("foo")) == "bar")
            headers:set("foo", "bax")
            assert(table.concat(headers:get_all("foo")) == "bax")

            // Test to_table
            local t = headers:to_table()
            assert(table.concat(t.TEST) == "abc321")
            t = headers:to_table({"foo"})
            assert(t.test == nil)
            assert(table.concat(t.foo) == "bax")
        })
        .exec()?;

        Ok(())
    }
}

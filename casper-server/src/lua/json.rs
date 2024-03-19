use std::rc::Rc;
use std::result::Result as StdResult;

use mlua::{
    AnyUserData, Error as LuaError, ExternalResult, Function, Integer as LuaInteger, IntoLuaMulti,
    Lua, LuaSerdeExt, MetaMethod, Result, String as LuaString, Table, UserData, UserDataMethods,
    UserDataRefMut, Value,
};
use ouroboros::self_referencing;
use serde::{Serialize, Serializer};

use super::FlexBytes;

/*
local bytes = require("@core/bytes")
type Bytes = bytes.Bytes

--- @class Json
--- @tag module
---
--- Built-in module for working with JSON.
local json = {}

type JsonObjectMeta = {
    __index: (JsonObject, string|number) -> ValueOrJsonObject,
    __iter: (JsonObject) -> ((any) -> (string?, any), any),
}

--- @class JsonObject
--- Represents a Rust native JSON object (map or array) in Lua.
local jsonObject = {}
jsonObject.prototype = {} :: JsonObjectMeta

export type JsonObject = typeof(setmetatable(jsonObject, jsonObject.prototype))

--- @type ValueOrJsonObject nil | boolean | string | number | JsonObject
--- @within Json
--- A simple Lua value or a complex `JsonObject`.
export type ValueOrJsonObject = nil | boolean | string | number | JsonObject
*/

#[derive(Clone)]
pub(crate) struct JsonObject {
    root: Rc<serde_json::Value>,
    current: *const serde_json::Value,
}

impl Serialize for JsonObject {
    fn serialize<S: Serializer>(&self, serializer: S) -> StdResult<S::Ok, S::Error> {
        self.current().serialize(serializer)
    }
}

impl JsonObject {
    fn new(root: &Rc<serde_json::Value>, current: *const serde_json::Value) -> Self {
        let root = root.clone();
        Self { root, current }
    }

    /// Returns a reference to the current value.
    const fn current(&self) -> &serde_json::Value {
        unsafe { &*self.current }
    }

    /// Returns a new `JsonObject` which points to the value at the given key.
    ///
    /// This operation is cheap and does not clone the underlying data.
    fn get(&self, key: Value) -> Option<JsonObject> {
        let current = self.current();
        let value = match key {
            Value::Integer(index) if index > 0 => current.get(index as usize - 1),
            Value::String(key) => key.to_str().ok().and_then(|s| current.get(s)),
            _ => None,
        }?;
        Some(Self::new(&self.root, value))
    }

    /// Returns a new `JsonObject` by following the given JSON Pointer path.
    fn pointer(&self, path: &str) -> Option<JsonObject> {
        Some(JsonObject {
            root: self.root.clone(),
            current: self.current().pointer(path)?,
        })
    }

    /// Converts this `JsonObject` into a Lua `Value`.
    fn into_lua(self, lua: &Lua) -> Result<Value> {
        match self.current() {
            serde_json::Value::Null => Ok(Value::NULL),
            serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
            serde_json::Value::Number(n) => {
                if let Some(n) = n.as_i64() {
                    Ok(Value::Number(n as _))
                } else if let Some(n) = n.as_f64() {
                    Ok(Value::Number(n))
                } else {
                    Err(LuaError::ToLuaConversionError {
                        from: "number",
                        to: "integer or float",
                        message: Some("number is too big to fit in a Lua integer".to_owned()),
                    })
                }
            }
            serde_json::Value::String(s) => Ok(Value::String(lua.create_string(s)?)),
            array @ serde_json::Value::Array(_) => Ok(Value::UserData(
                lua.create_ser_userdata(JsonObject::new(&self.root, array))?,
            )),
            object @ serde_json::Value::Object(_) => Ok(Value::UserData(
                lua.create_ser_userdata(JsonObject::new(&self.root, object))?,
            )),
        }
    }
}

impl From<serde_json::Value> for JsonObject {
    fn from(value: serde_json::Value) -> Self {
        let root = Rc::new(value);
        let current = Rc::as_ptr(&root);
        Self { root, current }
    }
}

impl UserData for JsonObject {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        /*
        --- @within JsonObject
        --- Follows the given JSON Pointer and returns the value.
        ---
        --- @param path json pointer
        function jsonObject:pointer(path: string): ValueOrJsonObject
            local _ = path
            return nil :: any
        end
        */
        methods.add_method("pointer", |lua, this, path: String| {
            this.pointer(&path)
                .map(|obj| obj.into_lua(lua))
                .unwrap_or(Ok(Value::Nil))
        });

        /*
        --- @within JsonObject
        --- Dumps this object to a Lua table.
        function jsonObject:dump(): any
            return nil :: any
        end
        */
        methods.add_method("dump", |lua, this, ()| lua.to_value(this.current()));

        /*
        --- @within JsonObject
        --- Returns the value at the given key.
        ---
        --- @param key string | number
        function jsonObject.prototype.__index(self: JsonObject, key: string|number): ValueOrJsonObject
            local _, _ = self, key
            return nil :: any
        end
        */
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: Value| {
            this.get(key)
                .map(|obj| obj.into_lua(lua))
                .unwrap_or(Ok(Value::Nil))
        });

        /*
        --- @within JsonObject
        --- Iterate over the key value pairs of this object.
        function jsonObject.prototype.__iter(self: JsonObject): ((any) -> (string?, any), any)
            local _ = self
            return nil :: any
        end
        */
        methods.add_meta_method(MetaMethod::Iter, |lua, this, ()| {
            match this.current() {
                serde_json::Value::Array(_) => {
                    let next = Function::wrap(|lua, mut it: UserDataRefMut<LuaJsonArrayIter>| {
                        it.next += 1;
                        match it.value.get(Value::Integer(it.next - 1)) {
                            Some(next_value) => {
                                (it.next - 1, next_value.into_lua(lua)?).into_lua_multi(lua)
                            }
                            None => ().into_lua_multi(lua),
                        }
                    });
                    let iter_ud = AnyUserData::wrap(LuaJsonArrayIter {
                        value: this.clone(),
                        next: 1, // index starts at 1
                    });
                    (next, iter_ud).into_lua_multi(lua)
                }
                serde_json::Value::Object(_) => {
                    let next = Function::wrap(|lua, mut it: UserDataRefMut<LuaJsonMapIter>| {
                        let root = it.borrow_value().root.clone();
                        it.with_iter_mut(move |iter| match iter.next() {
                            Some((key, value)) => {
                                let key = lua.create_string(key)?;
                                let value = JsonObject::new(&root, value).into_lua(lua)?;
                                (key, value).into_lua_multi(lua)
                            }
                            None => ().into_lua_multi(lua),
                        })
                    });
                    let iter_builder = LuaJsonMapIterBuilder {
                        value: this.clone(),
                        iter_builder: |value| value.current().as_object().unwrap().iter(),
                    };
                    let iter_ud = AnyUserData::wrap(iter_builder.build());
                    (next, iter_ud).into_lua_multi(lua)
                }
                _ => ().into_lua_multi(lua),
            }
        });
    }
}

struct LuaJsonArrayIter {
    value: JsonObject,
    next: LuaInteger,
}

impl UserData for LuaJsonArrayIter {}

#[self_referencing]
struct LuaJsonMapIter {
    value: JsonObject,

    #[borrows(value)]
    #[covariant]
    iter: serde_json::map::Iter<'this>,
}

impl UserData for LuaJsonMapIter {}

/*
--- @within Json
--- Decodes a JSON string to a Lua value.
--- Returns `nil` and an error message if the input is not a valid JSON string.
---
--- @param data string | Bytes
function json.decode(data: string | Bytes): (any, string?)
    local _ = data
    return nil :: any
end
*/
fn decode<'l>(lua: &'l Lua, data: FlexBytes) -> Result<StdResult<Value<'l>, String>> {
    let json: serde_json::Value = lua_try!(serde_json::from_slice(data.as_ref()).into_lua_err());
    Ok(Ok(lua.to_value(&json)?))
}

/*
--- @within Json
--- Decodes a JSON string to a simple Lua value or a complex `JsonObject`.
--- Returns `nil` and an error message if the input is not a valid JSON string.
---
--- @param data string | Bytes
function json.decode_native(data: string | Bytes): (ValueOrJsonObject, string?)
    local _ = data
    return nil :: any
end
*/
fn decode_native<'l>(lua: &'l Lua, data: FlexBytes) -> Result<StdResult<Value<'l>, String>> {
    let json: serde_json::Value = lua_try!(serde_json::from_slice(data.as_ref()).into_lua_err());
    Ok(Ok(lua_try!(JsonObject::from(json).into_lua(lua))))
}

/*
--- @within Json
--- Encodes a Lua value to a JSON string.
--- Returns `nil` and an error message if the input value cannot be encoded.
---
--- @param value Lua value
function json.encode(value: any): (string?, string?)
    local _ = value
    return nil :: any
end
*/
fn encode<'l>(lua: &'l Lua, value: Value) -> Result<LuaString<'l>> {
    let data = serde_json::to_vec(&value).into_lua_err()?;
    lua.create_string(data)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("decode", lua.create_function(decode)?),
        ("decode_native", lua.create_function(decode_native)?),
        ("encode", lua.create_function(encode)?),
    ])
}

/*
return json
*/

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_module() -> Result<()> {
        let lua = Lua::new();

        let json = super::create_module(&lua)?;
        lua.load(chunk! {
            local data = $json.encode({a = 1, b = "2", c = {3,4,5,"6"}})
            local value = $json.decode(data)
            assert(type(value) == "table")
            assert(type(value["a"] == "number"))
            assert(type(value["b"] == "string"))
            assert(type(value["c"] == "table"))
            assert(value["a"] == 1 and value["b"] == "2")

            local native_value = $json.decode_native(data)
            assert(type(native_value) == "userdata")
            assert(type(native_value["a"] == "number"))
            assert(type(native_value["b"] == "string"))
            assert(type(native_value["c"] == "userdata"))
            assert(native_value["a"] == 1 and native_value["b"] == "2")
            assert(native_value["c"][1] == 3)

            // Pointers
            assert(native_value:pointer("/a") == 1)
            assert(native_value:pointer("/c/0") == 3)

            // Test preserving data types
            local float_data = "{\"f\":[[],{},0.0,1.0,3]}"
            assert($json.encode($json.decode_native(float_data)) == float_data)

            // Test iteration
            local result = {}
            for k, v in native_value do
                if type(v) ~= "userdata" then
                    table.insert(result, tostring(k))
                    table.insert(result, tostring(v))
                end
            end
            assert(table.concat(result, ",") == "a,1,b,2")

            result = {}
            for i, v in native_value["c"] do
                table.insert(result, tostring(i))
                table.insert(result, tostring(v))
            end
            assert(table.concat(result, ",") == "1,3,2,4,3,5,4,6")

            // Test converting to a lua table
            local lua_value = native_value:dump()
            assert(type(lua_value) == "table")
            assert(type(lua_value["c"] == "table"))
        })
        .exec()?;

        Ok(())
    }
}

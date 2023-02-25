use std::rc::Rc;

use mlua::{
    AnyUserData, ExternalResult, Integer as LuaInteger, Lua, LuaSerdeExt, MetaMethod, Result,
    String as LuaString, Table, UserData, UserDataMethods, Value, Variadic,
};
use ouroboros::self_referencing;
use serde::Serialize;

#[derive(Clone)]
struct JsonValue {
    root: Rc<serde_json::Value>,
    current: *const serde_json::Value,
}

impl Serialize for JsonValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.current().serialize(serializer)
    }
}

impl JsonValue {
    /// Returns a reference to the current value.
    const fn current(&self) -> &serde_json::Value {
        unsafe { &*self.current }
    }

    /// Returns a new non-cloned `JsonValue` which points to the value at the given key.
    fn get(&self, key: Value) -> Option<JsonValue> {
        let current = self.current();
        let value = match key {
            Value::Integer(index) if index > 0 => current.get(index as usize - 1),
            Value::String(key) => key.to_str().ok().and_then(|s| current.get(s)),
            _ => None,
        }?;
        Some(JsonValue {
            root: self.root.clone(),
            current: value,
        })
    }

    fn pointer(&self, path: &str) -> Option<JsonValue> {
        Some(JsonValue {
            root: self.root.clone(),
            current: self.current().pointer(path)?,
        })
    }

    /// Converts a `JsonValue` to a Lua `Value`.
    fn to_lua<'lua>(value: Option<&Self>, lua: &'lua Lua) -> Result<Value<'lua>> {
        let value = match value {
            Some(value) => value,
            None => return Ok(Value::Nil),
        };
        match value.current() {
            serde_json::Value::Null => Ok(lua.null()),
            serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
            serde_json::Value::Number(n) => {
                if let Some(n) = n.as_i64() {
                    Ok(Value::Number(n as _))
                } else if let Some(n) = n.as_f64() {
                    Ok(Value::Number(n))
                } else {
                    Err(mlua::Error::ToLuaConversionError {
                        from: "number",
                        to: "integer or float",
                        message: Some("number is too big to fit in a Lua integer".to_owned()),
                    })
                }
            }
            serde_json::Value::String(s) => Ok(Value::String(lua.create_string(s)?)),
            array @ serde_json::Value::Array(_) => {
                Ok(Value::UserData(lua.create_ser_userdata(JsonValue {
                    root: value.root.clone(),
                    current: array,
                })?))
            }
            object @ serde_json::Value::Object(_) => {
                Ok(Value::UserData(lua.create_ser_userdata(JsonValue {
                    root: value.root.clone(),
                    current: object,
                })?))
            }
        }
    }
}

impl UserData for JsonValue {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("pointer", |lua, this, path: String| {
            JsonValue::to_lua(this.pointer(&path).as_ref(), lua)
        });

        // Recursively converts this userdata to a Lua table.
        methods.add_method("dump", |lua, this, ()| lua.to_value(this.current()));

        methods.add_meta_method(MetaMethod::Index, |lua, this, key: Value| {
            JsonValue::to_lua(this.get(key).as_ref(), lua)
        });

        methods.add_meta_method(MetaMethod::Iter, |lua, this, ()| {
            match this.current() {
                serde_json::Value::Array(_) => {
                    let next = lua.create_function(move |lua, ud: AnyUserData| {
                        let mut it = ud.borrow_mut::<LuaJsonArrayIter>()?;
                        it.next += 1;
                        match it.value.get(Value::Integer(it.next - 1)) {
                            Some(next_value) => Ok(Variadic::from_iter([
                                Value::Integer(it.next - 1),
                                JsonValue::to_lua(Some(&next_value), lua)?,
                            ])),
                            None => Ok(Variadic::new()),
                        }
                    })?;

                    let iter_ud = lua.create_userdata(LuaJsonArrayIter {
                        value: this.clone(),
                        next: 1, // index starts at 1
                    })?;
                    Ok((Value::Function(next), Some(iter_ud)))
                }
                serde_json::Value::Object(_) => {
                    let next = lua.create_function(move |lua, ud: AnyUserData| {
                        let mut iter = ud.borrow_mut::<LuaJsonObjectIter>()?;
                        let root = iter.borrow_value().root.clone();
                        iter.with_iter_mut(|it| match it.next() {
                            Some((key, value)) => Ok(Variadic::from_iter([
                                Value::String(lua.create_string(key)?),
                                JsonValue::to_lua(
                                    Some(&JsonValue {
                                        root,
                                        current: value,
                                    }),
                                    lua,
                                )?,
                            ])),
                            None => Ok(Variadic::new()),
                        })
                    })?;

                    let iter_ud = lua.create_userdata(
                        LuaJsonObjectIterBuilder {
                            value: this.clone(),
                            iter_builder: |this| match this.current() {
                                serde_json::Value::Object(object) => object.iter(),
                                _ => unreachable!(),
                            },
                        }
                        .build(),
                    )?;
                    Ok((Value::Function(next), Some(iter_ud)))
                }
                _ => Ok((Value::Nil, None)),
            }
        });
    }
}

struct LuaJsonArrayIter {
    value: JsonValue,
    next: LuaInteger,
}

impl UserData for LuaJsonArrayIter {}

#[self_referencing]
struct LuaJsonObjectIter {
    value: JsonValue,
    #[borrows(value)]
    #[covariant]
    iter: serde_json::map::Iter<'this>,
}

impl UserData for LuaJsonObjectIter {}

fn decode_json<'l>(lua: &'l Lua, data: Option<LuaString>) -> Result<Value<'l>> {
    match data {
        Some(data) => {
            let json: serde_json::Value = serde_json::from_slice(data.as_bytes()).into_lua_err()?;
            lua.to_value(&json)
        }
        None => Ok(Value::Nil),
    }
}

fn decode_json_native<'l>(lua: &'l Lua, data: Option<LuaString>) -> Result<Value<'l>> {
    match data {
        Some(data) => {
            let json: serde_json::Value = serde_json::from_slice(data.as_bytes()).into_lua_err()?;
            let root = Rc::new(json);
            let current = Rc::as_ptr(&root);
            JsonValue::to_lua(Some(&JsonValue { root, current }), lua)
        }
        None => Ok(Value::Nil),
    }
}

fn encode_json<'l>(lua: &'l Lua, value: Value) -> Result<LuaString<'l>> {
    let data = serde_json::to_vec(&value).into_lua_err()?;
    lua.create_string(&data)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("decode", lua.create_function(decode_json)?),
        ("decode_native", lua.create_function(decode_json_native)?),
        ("encode", lua.create_function(encode_json)?),
    ])
}

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

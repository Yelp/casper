use std::rc::Rc;
use std::result::Result as StdResult;

use mlua::{
    AnyUserData, Error as LuaError, Integer as LuaInteger, IntoLuaMulti, Lua, LuaSerdeExt,
    MetaMethod, MultiValue, Result, SerializeOptions, Table, UserData, UserDataMethods,
    UserDataRefMut, Value, Variadic,
};
use ntex::util::Bytes;
use ouroboros::self_referencing;
use serde::Serialize;

#[derive(Clone)]
struct YamlValue {
    root: Rc<serde_yaml::Value>,
    current: *const serde_yaml::Value, // borrows `root`
}

impl Serialize for YamlValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.current().serialize(serializer)
    }
}

impl YamlValue {
    fn new(root: &Rc<serde_yaml::Value>, current: *const serde_yaml::Value) -> Self {
        let root = root.clone();
        Self { root, current }
    }

    /// Returns a reference to the current value.
    const fn current(&self) -> &serde_yaml::Value {
        // Safety: `current` is a pointer to nested `root` data which is guaranteed to be valid.
        unsafe { &*self.current }
    }

    /// Returns a new `YamlValue` instance which points to the value at the given key.
    fn get(&self, key: Value) -> Option<Self> {
        let current = self.current();
        let value = match key {
            Value::Integer(index) if index > 0 => current.get(index as usize - 1),
            Value::String(key) => key.to_str().ok().and_then(|s| current.get(&*s)),
            Value::UserData(ud) => current.get(ud.borrow::<Self>().ok()?.current()),
            _ => None,
        }?;
        Some(Self::new(&self.root, value))
    }

    /// Converts `YamlValue` to a Lua `Value`.
    fn into_lua(self, lua: &Lua) -> Result<Value> {
        match self.current() {
            serde_yaml::Value::Null => Ok(Value::NULL),
            serde_yaml::Value::Bool(b) => Ok(Value::Boolean(*b)),
            serde_yaml::Value::Number(n) => {
                if let Some(n) = n.as_i64() {
                    Ok(Value::Number(n as _))
                } else if let Some(n) = n.as_f64() {
                    Ok(Value::Number(n))
                } else {
                    Err(LuaError::ToLuaConversionError {
                        from: "number".to_string(),
                        to: "integer or float",
                        message: Some("number is too big to fit in a Lua integer".to_owned()),
                    })
                }
            }
            serde_yaml::Value::String(s) => Ok(Value::String(lua.create_string(s)?)),
            sequence @ serde_yaml::Value::Sequence(_) => Ok(Value::UserData(
                lua.create_ser_userdata(YamlValue::new(&self.root, sequence))?,
            )),
            mapping @ serde_yaml::Value::Mapping(_) => Ok(Value::UserData(
                lua.create_ser_userdata(YamlValue::new(&self.root, mapping))?,
            )),
            serde_yaml::Value::Tagged(tagged_value) => {
                YamlValue::new(&self.root, &tagged_value.value).into_lua(lua)
            }
        }
    }
}

impl UserData for YamlValue {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // Recursively converts this userdata to a Lua table.
        methods.add_method("dump", |lua, this, ()| lua.to_value(this.current()));

        methods.add_method("get", |lua, this, keys: MultiValue| {
            let keys = keys
                .into_iter()
                .map(|k| match k {
                    Value::Integer(i) if i >= 1 => Ok(Some(IndexKey::Index(i as usize - 1))),
                    Value::Integer(i) => Ok(Some(IndexKey::String(i.to_string()))),
                    Value::Number(n) => Ok(Some(IndexKey::String(n.to_string()))),
                    Value::String(s) => Ok(Some(IndexKey::String(s.to_string_lossy()))),
                    _ => Ok(None),
                })
                .collect::<Result<Option<Vec<_>>>>()?;

            match keys.and_then(|keys| traverse_value(this.current(), &keys)) {
                Some(value) => YamlValue::new(&this.root, value).into_lua(lua),
                None => Ok(Value::Nil),
            }
        });

        methods.add_meta_method(MetaMethod::Index, |lua, this, key: Value| {
            match this.get(key) {
                Some(value) => value.into_lua(lua),
                None => Ok(Value::Nil),
            }
        });

        methods.add_meta_method(MetaMethod::Iter, |lua, this, ()| {
            match this.current() {
                serde_yaml::Value::Sequence(_) => {
                    let next =
                        lua.create_function(|lua, mut it: UserDataRefMut<LuaYamlSequenceIter>| {
                            it.next += 1;
                            match it.value.get(Value::Integer(it.next - 1)) {
                                Some(next_value) => Ok(Variadic::from_iter([
                                    Value::Integer(it.next - 1),
                                    next_value.into_lua(lua)?,
                                ])),
                                None => Ok(Variadic::new()),
                            }
                        })?;

                    let iter_ud = AnyUserData::wrap(LuaYamlSequenceIter {
                        value: this.clone(),
                        next: 1, // index starts at 1
                    });

                    (next, iter_ud).into_lua_multi(lua)
                }
                serde_yaml::Value::Mapping(_) => {
                    let next =
                        lua.create_function(|lua, mut it: UserDataRefMut<LuaYamlMapIter>| {
                            let root = it.borrow_value().root.clone();
                            it.with_iter_mut(move |iter| match iter.next() {
                                Some((key, value)) => {
                                    let key = YamlValue::new(&root, key).into_lua(lua)?;
                                    let value = YamlValue::new(&root, value).into_lua(lua)?;
                                    Ok(Variadic::from_iter([key, value]))
                                }
                                None => Ok(Variadic::new()),
                            })
                        })?;

                    let iter_ud = AnyUserData::wrap(
                        LuaYamlMapIterBuilder {
                            value: this.clone(),
                            iter_builder: |value| value.current().as_mapping().unwrap().iter(),
                        }
                        .build(),
                    );

                    (next, iter_ud).into_lua_multi(lua)
                }
                _ => ().into_lua_multi(lua),
            }
        });
    }
}

struct LuaYamlSequenceIter {
    value: YamlValue,
    next: LuaInteger,
}

#[self_referencing]
struct LuaYamlMapIter {
    value: YamlValue,

    #[borrows(value)]
    #[covariant]
    iter: serde_yaml::mapping::Iter<'this>,
}

pub enum IndexKey {
    Index(usize),
    String(String),
}

fn traverse_value<'a>(
    value: &'a serde_yaml::Value,
    keys: &[IndexKey],
) -> Option<&'a serde_yaml::Value> {
    let next_value = match keys.first() {
        Some(IndexKey::Index(i)) => value.get(i)?,
        Some(IndexKey::String(s)) => value.get(s)?,
        None => return Some(value),
    };
    traverse_value(next_value, &keys[1..])
}

fn try_with_slice<R>(value: Value, f: impl FnOnce(&[u8]) -> R) -> Result<R> {
    match value {
        Value::String(s) => Ok(f(&s.as_bytes())),
        Value::UserData(ud) if ud.is::<Bytes>() => {
            let bytes = ud.borrow::<Bytes>()?;
            Ok(f(bytes.as_ref()))
        }
        _ => Err(LuaError::FromLuaConversionError {
            from: value.type_name(),
            to: "string".to_string(),
            message: None,
        }),
    }
}

fn from_lua_options(t: Option<Table>) -> SerializeOptions {
    let mut options = SerializeOptions::default();
    if let Some(t) = t {
        if let Ok(enabled) = t.raw_get::<bool>("set_array_mt") {
            options.set_array_metatable = enabled;
        }
    }
    options
}

async fn read(
    lua: Lua,
    (path, options): (String, Option<Table>),
) -> Result<StdResult<Value, String>> {
    let data = lua_try!(tokio::fs::read(path).await);
    let mut yaml: serde_yaml::Value = lua_try!(serde_yaml::from_slice(&data));
    lua_try!(yaml.apply_merge());
    lua.to_value_with(&yaml, from_lua_options(options)).map(Ok)
}

async fn write(_: Lua, (path, value): (String, Value)) -> Result<StdResult<bool, String>> {
    let data = lua_try!(serde_yaml::to_string(&value));
    lua_try!(tokio::fs::write(path, data).await);
    Ok(Ok(true))
}

fn encode_yaml(_: &Lua, value: Value) -> Result<StdResult<String, String>> {
    Ok(Ok(lua_try!(serde_yaml::to_string(&value))))
}

fn decode_yaml(
    lua: &Lua,
    (data, options): (Value, Option<Table>),
) -> Result<StdResult<Value, String>> {
    let mut yaml: serde_yaml::Value = match data {
        Value::Nil => return Ok(Err("input is nil".to_string())),
        _ => lua_try!(try_with_slice(data, |s| serde_yaml::from_slice(s))?),
    };
    lua_try!(yaml.apply_merge());
    lua.to_value_with(&yaml, from_lua_options(options)).map(Ok)
}

fn decode_yaml_native(lua: &Lua, data: Value) -> Result<StdResult<Value, String>> {
    let mut yaml: serde_yaml::Value = match data {
        Value::Nil => return Ok(Err("input is nil".to_string())),
        _ => lua_try!(try_with_slice(data, |s| serde_yaml::from_slice(s))?),
    };
    lua_try!(yaml.apply_merge());
    let root = Rc::new(yaml);
    let current = Rc::as_ptr(&root);
    Ok(Ok(lua_try!(YamlValue { root, current }.into_lua(lua))))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("encode", lua.create_function(encode_yaml)?),
        ("decode", lua.create_function(decode_yaml)?),
        ("decode_native", lua.create_function(decode_yaml_native)?),
        ("read", lua.create_async_function(read)?),
        ("write", lua.create_async_function(write)?),
    ])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result, Value};

    #[test]
    fn test_encode() -> Result<()> {
        let lua = Lua::new();

        let null = Value::NULL;
        let yaml = super::create_module(&lua)?;
        lua.load(chunk! {
            assert($yaml.encode({a = 1}) == "a: 1\n")
            assert($yaml.encode({1, "2"}) == "- 1\n- '2'\n")
            assert($yaml.encode({{b = $null}}), "- b: null\n")

            local ok, err = $yaml.encode(_G)
            assert(ok == nil and err:find("cannot serialize") ~= nil)
        })
        .exec()
    }

    #[test]
    fn test_decode() -> Result<()> {
        let lua = Lua::new();

        let yaml = super::create_module(&lua)?;
        lua.load(chunk! {
            local data = $yaml.encode({a = 1, b = "2", c = {3, {d = 4}}})
            local value = $yaml.decode(data)
            assert(type(value) == "table")
            assert(type(value["a"] == "number"), "a is not a number")
            assert(type(value["b"] == "string"), "b is not a string")
            assert(type(value["c"] == "table"), "c is not a table")
            assert(value["a"] == 1, "a is not 1")
            assert(value["b"] == "2", "b is not 2")
            assert(value["c"][1] == 3, "c[1] is not 3")
            assert(value["c"][2].d == 4, "c[2].d is not 4")

            // More complex cases
            local value, err = $yaml.decode("a: &template\n  b: 1\n  c: 2\nd: *template\n")
            assert(value.a.b == 1)
            assert(value.d.c == 2)

            local value, err = $yaml.decode("a: &template\n  b: 1\n  c: 2\nd:\n  <<: *template\n  c: 3\n")
            assert(value.a.b == 1)
            assert(value.a.c == 2)
            assert(value.d.c == 3)

            // Error cases
            local value, err = $yaml.decode("\t:abc")
            assert(value == nil)
            assert(err:find("found character that cannot start any token") ~= nil)
        })
        .exec()
    }

    #[test]
    fn test_decode_with_options() -> Result<()> {
        let lua = Lua::new();

        let yaml = super::create_module(&lua)?;
        lua.load(chunk! {
            local data = $yaml.encode({1, 2, 3})
            local value = $yaml.decode(data)
            assert(getmetatable(value) ~= nil)
            value = $yaml.decode(data, { set_array_mt = false })
            assert(getmetatable(value) == nil)
        })
        .exec()
    }

    #[test]
    fn test_decode_native() -> Result<()> {
        let lua = Lua::new();

        let yaml = super::create_module(&lua)?;
        lua.load(chunk! {
            local data = $yaml.encode({a = 1, b = "2", c = {3, {d = 4}}})
            local value = $yaml.decode_native(data)
            assert(type(value) == "userdata")
            assert(type(value["a"] == "number"), "a is not a number")
            assert(type(value["b"] == "string"), "b is not a string")
            assert(type(value["c"] == "userdata"), "c is not a userdata")
            assert(value["a"] == 1, "a is not 1")
            assert(value["b"] == "2", "b is not 2")
            assert(value["c"][1] == 3, "c[1] is not 3")
            assert(value["c"][2].d == 4, "c[2].d is not 4")

            // Test iteration
            local result = {}
            for k, v in value do
                if type(v) ~= "userdata" then
                    table.insert(result, tostring(k)..","..tostring(v))
                end
            end
            table.sort(result)
            assert(table.concat(result, ",") == "a,1,b,2")

            result = {}
            for i, v in value["c"] do
                table.insert(result, tostring(i)..","..tostring(v))
            end
            table.sort(result)
            assert(table.concat(result, ","):sub(1, 16) == "1,3,2,YamlValue:")

            // Test dump
            local lua_value = value.c[2]:dump()
            assert(type(lua_value) == "table")
            assert(type(lua_value.d) == "number")
            assert(lua_value.d == 4)
        })
        .exec()
    }

    #[test]
    fn test_traverse_value() -> Result<()> {
        let lua = Lua::new();

        let null = Value::NULL;
        let yaml = super::create_module(&lua)?;
        lua.load(chunk! {
            local data = $yaml.encode({a = 1, b = "2", c = {3, {d = 4, e = $null}}})
            local value = $yaml.decode_native(data)

            assert(value:get("a") == 1)
            assert(value:get("c", 1) == 3)
            assert(value:get("c", 2, "d") == 4)
            assert(value:get("c", 2, "e") == $null)
            assert(value:get("c", 3, "d") == nil)
            assert(value:get("c", {}) == nil)
        })
        .exec()
    }

    #[tokio::test]
    async fn test_read_write() -> Result<()> {
        let lua = Lua::new();

        let null = Value::NULL;
        let yaml = super::create_module(&lua)?;
        let dir = tempfile::tempdir()?;
        let dir = dir.path().to_str().unwrap();
        lua.load(chunk! {
            local data = {a = 1, b = "2", c = {3, {d = 4, e = $null}}}
            local ok, err = $yaml.write($dir.."/test.yaml", data)
            assert(ok and err == nil)
            // Read the file back
            local value, err = $yaml.read($dir.."/test.yaml")
            assert(err == nil, err)
            assert(value.a == 1)
            assert(value.b == "2")
            assert(value.c[2].d == 4)
        })
        .exec_async()
        .await
    }
}

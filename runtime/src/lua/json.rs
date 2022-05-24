use mlua::{
    ExternalResult, Lua, LuaSerdeExt, Result, SerializeOptions, String as LuaString, Table, Value,
};

fn decode_json<'l>(lua: &'l Lua, data: Option<LuaString>) -> Result<Value<'l>> {
    match data {
        Some(data) => {
            let json: serde_json::Value = serde_json::from_slice(data.as_bytes()).to_lua_err()?;
            lua.to_value_with(&json, SerializeOptions::new().set_array_metatable(false))
        }
        None => Ok(Value::Nil),
    }
}

fn encode_json<'l>(lua: &'l Lua, value: Value) -> Result<LuaString<'l>> {
    let json: serde_json::Value = lua.from_value(value)?;
    let data = serde_json::to_vec(&json).to_lua_err()?;
    lua.create_string(&data)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("decode", lua.create_function(decode_json)?),
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
            local data = $json.encode({a = 1, b = "2", c = {3}})
            local value = $json.decode(data)
            assert(type(value) == "table")
            assert(type(value["a"] == "number"))
            assert(type(value["b"] == "string"))
            assert(type(value["c"] == "table"))
            assert(value["a"] == 1 and value["b"] == "2")
        })
        .exec()?;

        Ok(())
    }
}

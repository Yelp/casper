use anyhow::Context;
use mlua::{
    ExternalError, ExternalResult, Lua, LuaSerdeExt, MultiValue, Result, SerializeOptions, Table,
    Value,
};

use crate::config_loader::{self, IndexKey};

pub async fn get_config<'a>(
    lua: &'a Lua,
    (config, keys): (String, MultiValue<'a>),
) -> Result<Value<'a>> {
    let keys = keys
        .into_iter()
        .map(|k| match k {
            Value::Nil => Ok(IndexKey::None),
            Value::Integer(i) if i >= 0 => Ok(IndexKey::Usize(i as usize)),
            Value::Integer(i) => Ok(IndexKey::String(i.to_string())),
            Value::Number(n) => Ok(IndexKey::String(n.to_string())),
            Value::String(s) => Ok(IndexKey::String(s.to_string_lossy().to_string())),
            _ => Err(format!("invalid key: {}", k.type_name()).to_lua_err()),
        })
        .collect::<Result<Vec<_>>>()?;

    let value = config_loader::get_config(&config, &keys, None, None)
        .await
        .with_context(|| format!("failed to read '{config}'"))
        .to_lua_err()?;

    let options = SerializeOptions::new()
        .serialize_none_to_null(false)
        .set_array_metatable(false)
        .serialize_unit_to_null(false);

    lua.to_value_with(&value, options)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    let get_config = lua.create_async_function(get_config)?;
    lua.create_table_from([("get_config", get_config)])
}

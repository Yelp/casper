use mlua::{ExternalResult, Lua, Result, Table};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn normalize_uri(_: &Lua, uri: String) -> Result<String> {
    Ok(crate::utils::normalize_uri(uri).to_lua_err()?.to_string())
}

fn random_string(_: &Lua, (len, mode): (usize, Option<String>)) -> Result<String> {
    Ok(crate::utils::random_string(len, mode.as_deref()))
}

fn hash(_: &Lua, value: String) -> Result<u32> {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    Ok(hasher.finish() as u32)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("normalize_uri", lua.create_function(normalize_uri)?),
        ("random_string", lua.create_function(random_string)?),
        ("hash", lua.create_function(hash)?),
    ])
}

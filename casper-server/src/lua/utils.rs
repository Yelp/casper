use mlua::{ExternalResult, Lua, Result, Table};

fn normalize_uri(_: &Lua, uri: String) -> Result<String> {
    Ok(crate::utils::normalize_uri(uri).to_lua_err()?.to_string())
}

fn random_string(_: &Lua, (len, mode): (usize, Option<String>)) -> Result<String> {
    Ok(crate::utils::random_string(len, mode.as_deref()))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("normalize_uri", lua.create_function(normalize_uri)?),
        ("random_string", lua.create_function(random_string)?),
    ])
}

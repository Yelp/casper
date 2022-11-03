use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use mlua::{Lua, Result as LuaResult, Table};

fn random_string(_: &Lua, (len, mode): (usize, Option<String>)) -> LuaResult<String> {
    Ok(crate::utils::random_string(len, mode.as_deref()))
}

fn hash(_: &Lua, value: String) -> LuaResult<u32> {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    Ok(hasher.finish() as u32)
}

pub fn create_module(lua: &Lua) -> LuaResult<Table> {
    lua.create_table_from([
        ("random_string", lua.create_function(random_string)?),
        ("hash", lua.create_function(hash)?),
    ])
}

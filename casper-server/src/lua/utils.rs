use mlua::{Lua, Result as LuaResult, Table};

fn random_string(_: &Lua, (len, mode): (usize, Option<String>)) -> LuaResult<String> {
    Ok(crate::utils::random_string(len, mode.as_deref()))
}

pub fn create_module(lua: &Lua) -> LuaResult<Table> {
    lua.create_table_from([("random_string", lua.create_function(random_string)?)])
}

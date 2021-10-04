use mlua::{Function, Lua, Result as LuaResult, Table};

use crate::response::LuaResponse;

pub fn init_core(lua: &Lua) -> LuaResult<Table> {
    let core = lua.create_table()?;
    core.set("Response", lua.create_function(LuaResponse::constructor)?)?;
    Ok(core)
}

pub fn make_core_module(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(|lua, ()| init_core(lua))
}

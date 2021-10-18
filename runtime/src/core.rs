use mlua::{Function, Lua, Result as LuaResult, Table};
use std::sync::Arc;

use crate::backends::{self, Backend};
use crate::response::LuaResponse;
use crate::storage::LuaStorage;

pub fn init_core(lua: &Lua) -> LuaResult<Table> {
    let core = lua.create_table()?;

    core.set("Response", lua.create_function(LuaResponse::constructor)?)?;

    // Create storage backends in Lua
    let storage = lua.create_table()?;
    for (name, backend) in backends::registered_backends() {
        storage.set(
            name.as_str(),
            LuaStorage::new(match backend {
                Backend::Memory(m) => Arc::clone(m),
            }),
        )?;
    }
    core.set("storage", storage)?;

    Ok(core)
}

pub fn make_core_module(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(|lua, ()| init_core(lua))
}

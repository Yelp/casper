use mlua::{Function, Lua, Result as LuaResult, Table};
use once_cell::sync::Lazy;
use std::sync::Arc;

use crate::backends::MemoryBackend;
use crate::response::LuaResponse;
use crate::storage::LuaStorage;

static MEMORY_BACKEND: Lazy<Arc<MemoryBackend>> = Lazy::new(|| Arc::new(MemoryBackend::new(100)));

pub fn init_core(lua: &Lua) -> LuaResult<Table> {
    let core = lua.create_table()?;

    core.set("Response", lua.create_function(LuaResponse::constructor)?)?;

    // Init Memory storage backend
    core.set(
        "storage",
        lua.create_userdata(LuaStorage::new(Arc::clone(&*MEMORY_BACKEND)))?,
    )?;

    Ok(core)
}

pub fn make_core_module(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(|lua, ()| init_core(lua))
}

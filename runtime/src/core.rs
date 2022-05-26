use std::process;
use std::sync::Arc;
use std::time::Duration;

use mlua::{Function, Lua, Result as LuaResult, Table};

use crate::backends::{self, Backend};
use crate::lua;
use crate::response::LuaResponse;
use crate::storage::LuaStorage;

pub fn init_core(lua: &Lua) -> LuaResult<Table> {
    let core = lua.create_table()?;

    core.set("Response", lua.create_function(LuaResponse::constructor)?)?;

    // Create storage backends in Lua
    let storage = lua.create_table()?;
    for (name, backend) in backends::registered_backends() {
        match backend {
            Backend::Memory(b) => {
                storage.set(name.as_str(), LuaStorage::new(Arc::clone(b)))?;
            }
            Backend::Redis(b) => {
                storage.set(name.as_str(), LuaStorage::new(Arc::clone(b)))?;
            }
        }
    }
    core.set("storage", storage)?;

    // Modules
    core.set("config", lua::config::create_module(lua)?)?;
    core.set("datetime", lua::datetime::create_module(lua)?)?;
    core.set("fs", lua::fs::create_module(lua)?)?;
    core.set("json", lua::json::create_module(lua)?)?;
    core.set("metrics", lua::metrics::create_module(lua)?)?;
    core.set("regex", lua::regex::create_module(lua)?)?;
    core.set("tasks", lua::tasks::create_module(lua)?)?;
    core.set("udp", lua::udp::create_module(lua)?)?;
    core.set("utils", lua::utils::create_module(lua)?)?;

    // Variables
    let hostname = sys_info::hostname().expect("couldn't get hostname");
    core.set("hostname", hostname)?;
    core.set("pid", process::id())?;

    // Helper functions
    core.set(
        "sleep",
        lua.create_async_function(|_, secs: f64| async move {
            Ok(tokio::time::sleep(Duration::from_secs_f64(secs)).await)
        })?,
    )?;
    core.set(
        "yield",
        lua.create_async_function(|_, ()| async { Ok(tokio::task::yield_now().await) })?,
    )?;

    Ok(core)
}

pub fn make_core_module(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(|lua, ()| init_core(lua))
}

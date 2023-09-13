use std::env;
use std::process;
use std::time::Duration;

use mlua::{Lua, LuaSerdeExt, Result as LuaResult, Table};

use super::{LuaRequest, LuaResponse};

pub fn create_module(lua: &Lua) -> LuaResult<Table> {
    // Register data types
    super::bytes::register_types(lua)?;

    let core = lua.create_table()?;

    core.set("Request", lua.create_proxy::<LuaRequest>()?)?;
    core.set("Response", lua.create_proxy::<LuaResponse>()?)?;

    // Modules
    core.set("crypto", super::crypto::create_module(lua)?)?;
    core.set("csv", super::csv::create_module(lua)?)?;
    core.set("datetime", super::datetime::create_module(lua)?)?;
    core.set("fs", super::fs::create_module(lua)?)?;
    core.set("http", super::http::create_module(lua)?)?;
    core.set("json", super::json::create_module(lua)?)?;
    core.set("log", super::log::create_module(lua)?)?;
    core.set("metrics", super::metrics::create_module(lua)?)?;
    core.set("regex", super::regex::create_module(lua)?)?;
    core.set("tasks", super::tasks::create_module(lua)?)?;
    core.set("trace", super::trace::create_module(lua)?)?;
    core.set("udp", super::udp::create_module(lua)?)?;
    core.set("uri", super::uri::create_module(lua)?)?;
    core.set("utils", super::utils::create_module(lua)?)?;
    core.set("yaml", super::yaml::create_module(lua)?)?;

    // Variables
    let hostname = sys_info::hostname().expect("couldn't get hostname");
    core.set("hostname", hostname)?;
    core.set("pid", process::id())?;

    // Helper functions
    core.set(
        "sleep",
        lua.create_async_function(|_, secs: f64| async move {
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            Ok(())
        })?,
    )?;
    core.set(
        "yield",
        lua.create_async_function(|_, ()| async {
            tokio::task::yield_now().await;
            Ok(())
        })?,
    )?;
    core.set(
        "getenv",
        lua.create_function(|_, key: String| Ok(env::var(key).ok()))?,
    )?;

    // Other bits
    core.set("null", lua.null())?;
    core.set("array_metatable", lua.array_metatable())?;

    Ok(core)
}

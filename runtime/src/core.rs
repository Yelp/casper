use std::sync::Arc;

use mlua::{Function, Lua, Result as LuaResult, Table};

use crate::backends::{self, Backend};
use crate::config_loader;
use crate::regex;
use crate::response::LuaResponse;
use crate::storage::LuaStorage;
use crate::utils;

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

    // Create `config` module
    let config = lua.create_table()?;
    config.set(
        "get_config",
        lua.create_async_function(config_loader::lua::get_config)?,
    )?;
    core.set("config", config)?;

    // Create `regex` module
    let regex = lua.create_table()?;
    regex.set("new", lua.create_function(regex::regex_new)?)?;
    core.set("regex", regex)?;

    // Create `utils` module
    let utils = lua.create_table()?;
    utils.set(
        "normalize_uri",
        lua.create_function(utils::lua::normalize_uri)?,
    )?;
    core.set("utils", utils)?;

    Ok(core)
}

pub fn make_core_module(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(|lua, ()| init_core(lua))
}

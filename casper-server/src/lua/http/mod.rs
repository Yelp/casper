pub use body::LuaBody;
pub use headers::{LuaHttpHeaders, LuaHttpHeadersExt};
pub use request::LuaRequest;
pub use response::LuaResponse;

use mlua::{Lua, Result as LuaResult, Table};

// Re-export for inner mods
use body::EitherBody;
use client::LuaHttpClient;

pub fn create_module(lua: &Lua) -> LuaResult<Table> {
    lua.create_table_from([("Client", lua.create_proxy::<LuaHttpClient>()?)])
}

mod body;
mod client;
mod headers;
mod request;
mod response;

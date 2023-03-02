use std::ops::Deref;

use mlua::{Lua, OwnedTable};

#[derive(Clone, Debug)]
pub(crate) struct LuaContext(pub(crate) OwnedTable);

impl Deref for LuaContext {
    type Target = OwnedTable;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl LuaContext {
    pub(crate) fn new(lua: &Lua) -> Self {
        let ctx = lua
            .create_table()
            .expect("Failed to create Lua context table");
        LuaContext(ctx.into_owned())
    }
}

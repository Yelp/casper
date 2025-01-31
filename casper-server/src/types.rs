use std::ops::Deref;

use mlua::{IntoLua, Lua, Result as LuaResult, Table as LuaTable, Value};

// Value stored in response extensions to indicate that response is encrypted
#[derive(Clone, Copy, Debug, Default)]
pub struct EncryptedExt(pub bool);

#[derive(Clone, Debug)]
pub(crate) struct LuaContext(pub(crate) LuaTable);

impl Deref for LuaContext {
    type Target = LuaTable;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoLua for LuaContext {
    #[inline]
    fn into_lua(self, lua: &Lua) -> LuaResult<Value> {
        self.0.into_lua(lua)
    }
}

impl LuaContext {
    pub(crate) fn new(lua: &Lua) -> Self {
        let ctx = lua
            .create_table()
            .expect("Failed to create Lua context table");
        LuaContext(ctx)
    }
}

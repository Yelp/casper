use std::ops::Deref;

use mlua::{IntoLua, Lua, OwnedTable, Result as LuaResult, Value};

// Value stored in response extensions to indicate that response is encrypted
#[derive(Clone, Copy, Debug, Default)]
pub struct EncryptedExt(pub bool);

#[derive(Clone, Debug)]
pub(crate) struct LuaContext(pub(crate) OwnedTable);

impl Deref for LuaContext {
    type Target = OwnedTable;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'lua> IntoLua<'lua> for LuaContext {
    #[inline]
    fn into_lua(self, lua: &'lua Lua) -> LuaResult<Value<'lua>> {
        self.0.into_lua(lua)
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

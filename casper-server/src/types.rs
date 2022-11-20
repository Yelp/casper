use std::ops::Deref;
use std::sync::Arc;

use mlua::{Lua, RegistryKey, Table};

#[derive(Clone)]
pub(crate) struct LuaContext(pub(crate) Arc<RegistryKey>);

impl Deref for LuaContext {
    type Target = RegistryKey;

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
        let key = lua
            .create_registry_value(ctx)
            .map(Arc::new)
            .expect("Failed to store Lua context table in the registry");
        LuaContext(key)
    }

    pub(crate) fn get<'lua>(&self, lua: &'lua Lua) -> Table<'lua> {
        lua.registry_value::<Table>(&self.0)
            .expect("Unable to get Lua context table from the registry")
    }
}

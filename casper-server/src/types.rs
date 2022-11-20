use std::ops::Deref;
use std::rc::Rc;

use mlua::{Lua, RegistryKey, Table};

#[derive(Clone, Debug)]
pub(crate) struct LuaContext(pub(crate) Rc<RegistryKey>);

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
            .expect("Failed to store Lua context table in the registry");
        LuaContext(Rc::new(key))
    }

    pub(crate) fn get<'lua>(&self, lua: &'lua Lua) -> Table<'lua> {
        lua.registry_value::<Table>(&self.0)
            .expect("Unable to get Lua context table from the registry")
    }
}

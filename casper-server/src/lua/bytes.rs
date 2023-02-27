use bytes::Bytes;
use mlua::{Lua, MetaMethod, Result as LuaResult, UserDataMethods};

pub fn register_types(lua: &Lua) -> LuaResult<()> {
    lua.register_userdata_type::<Bytes>(|reg| {
        reg.add_method("len", |_, this, ()| Ok(this.len()));
        reg.add_method("is_empty", |_, this, ()| Ok(this.is_empty()));
        reg.add_method("to_string", |lua, this, ()| lua.create_string(this));

        reg.add_meta_method(MetaMethod::ToString, |lua, this, ()| {
            lua.create_string(this)
        });
    })
}

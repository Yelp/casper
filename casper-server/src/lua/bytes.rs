use mlua::{Lua, MetaMethod, Result as LuaResult, UserDataMethods};
use ntex::util::Bytes;

/*
--- @class Bytes
---
--- Built-in type for working with bytes
local Bytes = {}
Bytes.__index = Bytes

export type Bytes = typeof(setmetatable({}, Bytes))
*/

pub fn register_types(lua: &Lua) -> LuaResult<()> {
    lua.register_userdata_type::<Bytes>(|reg| {
        /*
        --- @within Bytes
        --- Returns the length of the bytes object.
        function Bytes:len(): number
            return nil :: any
        end
        */
        reg.add_method("len", |_, this, ()| Ok(this.len()));

        /*
        --- @within Bytes
        --- Returns true if the bytes object is empty.
        function Bytes:is_empty(): boolean
            return nil :: any
        end
        */
        reg.add_method("is_empty", |_, this, ()| Ok(this.is_empty()));

        /*
        --- @within Bytes
        --- Returns the bytes object as a Lua string.
        function Bytes:to_string(): string
            return nil :: any
        end
        */
        reg.add_method("to_string", |lua, this, ()| lua.create_string(this));

        /*
        --- @within Bytes
        --- Returns the bytes object as a Lua string.
        function Bytes:__tostring(): string
            return nil :: any
        end
        */
        reg.add_meta_method(MetaMethod::ToString, |lua, this, ()| {
            lua.create_string(this)
        });
    })
}

/*
return {}
*/

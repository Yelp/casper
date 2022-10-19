use std::rc::{Rc, Weak};

use mlua::Lua;

pub(crate) trait LuaExt {
    fn strong(&self) -> Rc<Lua>;
    fn weak(&self) -> Weak<Lua>;
}

impl LuaExt for Lua {
    #[inline]
    #[track_caller]
    fn strong(&self) -> Rc<Lua> {
        self.app_data_ref::<Weak<Lua>>()
            .expect("Failed to get `Weak<Lua>`")
            .upgrade()
            .expect("Failed to upgrade `Weak<Lua>`")
    }

    #[inline]
    #[track_caller]
    fn weak(&self) -> Weak<Lua> {
        self.app_data_ref::<Weak<Lua>>()
            .expect("Failed to get `Weak<Lua>`")
            .clone()
    }
}

pub(crate) trait WeakLuaExt {
    fn to_strong(&self) -> Rc<Lua>;
}

impl WeakLuaExt for Weak<Lua> {
    #[inline]
    fn to_strong(&self) -> Rc<Lua> {
        self.upgrade().expect("Failed to upgrade `Weak<Lua>`")
    }
}

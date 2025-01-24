use std::ops::Deref;

use mlua::{Lua, MetaMethod, Result, Table, UserData, UserDataMethods, UserDataRef};
use time::OffsetDateTime;

#[derive(Clone, Copy, Debug)]
struct DateTime(OffsetDateTime);

impl Deref for DateTime {
    type Target = OffsetDateTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DateTime {
    pub fn now(_: &Lua, _: ()) -> Result<Self> {
        Ok(DateTime(OffsetDateTime::now_utc()))
    }
}

impl UserData for DateTime {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("unix_timestamp", |_, this, ()| {
            Ok(((this.unix_timestamp_nanos() / 1000) as f64) / 1_000_000.0)
        });

        methods.add_method("elapsed", |_, this, ()| {
            Ok((OffsetDateTime::now_utc() - this.0).as_seconds_f64())
        });

        methods.add_meta_method(MetaMethod::Sub, |_, this, other: UserDataRef<Self>| {
            Ok((this.0 - other.0).as_seconds_f64())
        });
    }
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([("now", lua.create_function(DateTime::now)?)])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_module() -> Result<()> {
        let lua = Lua::new();

        let datetime = super::create_module(&lua)?;
        lua.load(chunk! {
            local start = $datetime.now()
            local timestamp = start:unix_timestamp()
            assert(timestamp > 0)
            local elapsed = start:elapsed()
            assert(elapsed > 0)
            local time2 = $datetime.now()
            assert(time2 - start >= 0)
        })
        .exec()?;

        Ok(())
    }
}

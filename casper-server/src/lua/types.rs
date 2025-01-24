use mlua::{ErrorContext, FromLua, Lua, Result as LuaResult, String as LuaString, Value};
use ntex::util::Bytes;

/// A Lua string or a byte array.
pub(crate) enum FlexBytes {
    String(LuaString),
    Bytes(Bytes),
}

impl FlexBytes {
    /// Returns the Bytes (owned).
    pub fn into_bytes(self) -> Bytes {
        match self {
            FlexBytes::String(s) => Bytes::from(s.as_bytes().to_vec()),
            FlexBytes::Bytes(b) => b,
        }
    }

    pub fn borrow_bytes<T>(&self, f: impl FnOnce(&[u8]) -> T) -> T {
        match self {
            FlexBytes::String(s) => f(&s.as_bytes()),
            FlexBytes::Bytes(b) => f(b.as_ref()),
        }
    }
}

impl FromLua for FlexBytes {
    fn from_lua(value: Value, lua: &Lua) -> LuaResult<Self> {
        let flexbytes = match value {
            Value::UserData(ud) => FlexBytes::Bytes(
                ud.borrow::<Bytes>()
                    .context("expected `Bytes` or string")?
                    .clone(),
            ),
            value => FlexBytes::String(
                LuaString::from_lua(value, lua).context("expected `Bytes` or string")?,
            ),
        };
        Ok(flexbytes)
    }
}

#[cfg(test)]
mod tests {
    use super::FlexBytes;
    use mlua::{chunk, IntoLua, Lua, Result};

    #[test]
    fn test() -> Result<()> {
        let lua = Lua::new();

        let flexbytes = lua.unpack::<FlexBytes>("hello".into_lua(&lua)?)?;
        flexbytes.borrow_bytes(|b| assert_eq!(b, b"hello"));

        let f = lua.create_function(|_, _: FlexBytes| Ok(()))?;
        lua.load(chunk! {
            local ok, err

            ok, err = pcall($f, "hello")
            assert(err == nil)

            // Try invalid types
            ok, err = pcall($f, newproxy())
            assert(tostring(err):find("bad argument #1: expected `Bytes` or string") ~= nil)

            ok, err = pcall($f, false)
            assert(tostring(err):find("bad argument #1: expected `Bytes` or string") ~= nil)
            assert(tostring(err):find("error converting Lua boolean to string %(expected string or number%)") ~= nil)
        })
        .exec()
        .unwrap();

        Ok(())
    }
}

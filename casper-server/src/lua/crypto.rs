use mlua::{ExternalResult, Lua, Result, String as LuaString, Table, Value};
use ntex::util::Bytes;
use openssl::hash::{hash, MessageDigest};

async fn digest<'lua>(lua: &'lua Lua, input: Value<'lua>, digest: MessageDigest) -> Result<String> {
    let bytes = match input {
        Value::UserData(ud) => ud.borrow::<Bytes>()?.clone(),
        _ => Bytes::from(lua.unpack::<LuaString>(input)?.as_bytes().to_vec()),
    };
    let hashsum = ntex::rt::spawn_blocking(move || hash(digest, bytes.as_ref()))
        .await
        .expect("failed to join thread")
        .into_lua_err()?;
    Ok(hex::encode(hashsum))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([(
        "sha256",
        lua.create_async_function(|lua, input| digest(lua, input, MessageDigest::sha256()))?,
    )])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[ntex::test]
    async fn test_module() -> Result<()> {
        let lua = Lua::new();

        let crypto = super::create_module(&lua)?;
        lua.load(chunk! {
            assert($crypto.sha256("hello") == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        })
        .exec_async()
        .await?;

        Ok(())
    }
}

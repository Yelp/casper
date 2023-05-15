use mlua::{ExternalResult, Lua, Result, String as LuaString, Table, Value};
use ntex::util::Bytes;
use openssl::hash::MessageDigest;

async fn hash<'lua>(lua: &'lua Lua, input: Value<'lua>, digest: MessageDigest) -> Result<String> {
    let bytes = match input {
        Value::UserData(ud) => ud.borrow::<Bytes>()?.clone(),
        _ => Bytes::from(lua.unpack::<LuaString>(input)?.as_bytes().to_vec()),
    };
    let hashsum = ntex::rt::spawn_blocking(move || openssl::hash::hash(digest, bytes.as_ref()))
        .await
        .expect("failed to join thread")
        .into_lua_err()?;
    Ok(hex::encode(hashsum))
}

pub async fn blake3<'lua>(lua: &'lua Lua, input: Value<'lua>) -> Result<String> {
    let bytes = match input {
        Value::UserData(ud) => ud.borrow::<Bytes>()?.clone(),
        _ => Bytes::from(lua.unpack::<LuaString>(input)?.as_bytes().to_vec()),
    };
    let hashsum = ntex::rt::spawn_blocking(move || blake3::hash(bytes.as_ref()));
    Ok(hex::encode(
        hashsum.await.expect("failed to join thread").as_bytes(),
    ))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        (
            "sha256",
            lua.create_async_function(|lua, input| hash(lua, input, MessageDigest::sha256()))?,
        ),
        ("blake3", lua.create_async_function(blake3)?),
    ])
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
            assert($crypto.blake3("hello") == "ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f")
        })
        .exec_async()
        .await?;

        Ok(())
    }
}

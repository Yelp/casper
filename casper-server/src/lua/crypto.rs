use std::result::Result as StdResult;

use mlua::{ExternalResult, Lua, Result, String as LuaString, Table, Value};
use ntex::util::Bytes;
use openssl::hash::MessageDigest;
use serde_json::{
    from_slice as json_from_slice, to_vec_pretty as json_to_vec_pretty, Value as JsonValue,
};

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

fn blake3(lua: &Lua, input: Value) -> Result<String> {
    let hash = match input {
        Value::UserData(ud) => blake3::hash(ud.borrow::<Bytes>()?.as_ref()),
        _ => blake3::hash(lua.unpack::<LuaString>(input)?.as_bytes()),
    };
    Ok(hex::encode(hash.as_bytes()))
}

fn json_digest(lua: &Lua, input: Value) -> Result<StdResult<String, String>> {
    let json_val: JsonValue = match input {
        Value::UserData(ud) => lua_try!(json_from_slice(ud.borrow::<Bytes>()?.as_ref())),
        _ => lua_try!(json_from_slice(lua.unpack::<LuaString>(input)?.as_bytes())),
    };
    let hash = blake3::hash(lua_try!(json_to_vec_pretty(&json_val)).as_ref());
    Ok(Ok(hex::encode(hash.as_bytes())))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        (
            "sha256",
            lua.create_async_function(|lua, input| hash(lua, input, MessageDigest::sha256()))?,
        ),
        ("blake3", lua.create_function(blake3)?),
        ("json_digest", lua.create_function(json_digest)?),
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
            local digest1 = $crypto.json_digest("{\"foo\":\"bar\",\"a\":\"b\"}")
            local digest2 = $crypto.json_digest("{\"a\":\"b\",\"foo\":\"bar\"}")
            assert(digest1 == digest2, "json digest should be deterministic")
        })
        .exec_async()
        .await?;

        Ok(())
    }
}

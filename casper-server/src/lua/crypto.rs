use std::result::Result as StdResult;

use mlua::{ExternalResult, Lua, Result, Table};
use ntex::rt::spawn_blocking;
use ntex::util::Bytes;
use openssl::hash::MessageDigest;
use serde_json::{
    from_slice as json_from_slice, to_vec_pretty as json_to_vec_pretty, Value as JsonValue,
};

use super::FlexBytes;

/*
local bytes = require("@core/bytes")

type Bytes = bytes.Bytes

--- @class Crypto
---
--- Module with various crypto functions
local crypto = {}
*/

async fn hash(input: Bytes, digest: MessageDigest) -> Result<String> {
    let hashsum = spawn_blocking(move || openssl::hash::hash(digest, &input))
        .await
        .expect("failed to join thread")
        .into_lua_err()?;
    Ok(hex::encode(hashsum))
}

/*
--- @within Crypto
--- Returns the SHA256 hash of the input.
---
--- @param input The input data to calculate hash.
function crypto.sha256(input: Bytes | string): string
    return nil :: any
end
*/
async fn sha256<'lua>(_: &'lua Lua, input: FlexBytes<'lua>) -> Result<String> {
    hash(input.into_bytes(), MessageDigest::sha256()).await
}

/*
--- @within Crypto
--- Returns the BLAKE3 hash of the input.
---
--- @param input The input data to calculate hash.
function crypto.blake3(input: Bytes | string): string
    return nil :: any
end
*/
fn blake3(_: &Lua, input: FlexBytes) -> Result<String> {
    Ok(hex::encode(blake3::hash(input.as_ref()).as_bytes()))
}

/*
--- @within Crypto
--- Returns the BLAKE3 hash of the json data.
--- The hash is calculated from normalized json where all keys sorted.
---
--- ### Example usage
---
--- ```lua
--- local crypto = require("@core/crypto")
--- local digest1 = crypto.json_digest(`{"foo":"bar","a":"b"}`)
--- local digest2 = crypto.json_digest(`{"a":"b","foo":"bar"}`)
--- assert(digest1 == digest2, "json digest should be deterministic")
--- ```
---
--- @param input The input (valid json string) to calculate hash.
function crypto.json_digest(input: Bytes | string): string
    return nil :: any
end
*/
fn json_digest(_: &Lua, input: FlexBytes) -> Result<StdResult<String, String>> {
    let json_val: JsonValue = lua_try!(json_from_slice(input.as_ref()));
    let hash = blake3::hash(lua_try!(json_to_vec_pretty(&json_val)).as_ref());
    Ok(Ok(hex::encode(hash.as_bytes())))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("sha256", lua.create_async_function(sha256)?),
        ("blake3", lua.create_function(blake3)?),
        ("json_digest", lua.create_function(json_digest)?),
    ])
}

/*
return crypto
*/

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

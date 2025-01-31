use std::result::Result as StdResult;

use bstr::BString;
use mlua::{ExternalResult, Lua, Result, String as LuaString, Table};
use ntex::rt::spawn_blocking;
use openssl::hash::MessageDigest;
use openssl::symm::{Cipher, Crypter, Mode};
use serde_json::{
    from_slice as json_from_slice, to_vec_pretty as json_to_vec_pretty, Value as JsonValue,
};

use super::FlexBytes;

/*
local bytes = require("@core/bytes")

type Bytes = bytes.Bytes

--- @class crypto
--- @tag module
---
--- Module with various crypto functions
local crypto = {}
*/

/*
--- @within crypto
--- Returns the SHA1 hash of the input.
--- If `raw` is `true`, returns the raw hash instead of hex-encoded string.
---
--- @param input The input data to calculate hash.
function crypto.sha1(input: Bytes | string, raw: boolean?): string
    return nil :: any
end
*/
fn sha1(lua: &Lua, (input, raw): (FlexBytes, Option<bool>)) -> Result<LuaString> {
    let hashsum = input
        .borrow_bytes(|b| openssl::hash::hash(MessageDigest::sha1(), b))
        .into_lua_err()?;
    if !raw.unwrap_or(false) {
        return lua.create_string(hex::encode(hashsum));
    }
    lua.create_string(hashsum.as_ref())
}

/*
--- @within crypto
--- Returns the SHA256 hash of the input.
--- If `raw` is `true`, returns the raw hash instead of hex-encoded string.
---
--- @param input The input data to calculate hash.
function crypto.sha256(input: Bytes | string): string
    return nil :: any
end
*/
fn sha256(lua: &Lua, (input, raw): (FlexBytes, Option<bool>)) -> Result<LuaString> {
    let hashsum = input
        .borrow_bytes(|b| openssl::hash::hash(MessageDigest::sha256(), b))
        .into_lua_err()?;
    if !raw.unwrap_or(false) {
        return lua.create_string(hex::encode(hashsum));
    }
    lua.create_string(hashsum.as_ref())
}

/*
--- @within crypto
--- Returns the BLAKE3 hash of the input.
---
--- @param input The input data to calculate hash.
function crypto.blake3(input: Bytes | string): string
    return nil :: any
end
*/
fn blake3(_: &Lua, input: FlexBytes) -> Result<String> {
    Ok(input.borrow_bytes(|b| hex::encode(blake3::hash(b).as_bytes())))
}

/*
--- @within crypto
--- Returns the BLAKE3 hash of the json data.
--- The hash is calculated from normalized json where all keys sorted.
---
--- #example
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
    let json_val: JsonValue = lua_try!(input.borrow_bytes(|b| json_from_slice(b)));
    let hash = blake3::hash(lua_try!(json_to_vec_pretty(&json_val)).as_ref());
    Ok(Ok(hex::encode(hash.as_bytes())))
}

//
// Encryption
//

// Construct a `Cipher` from a string.
fn ciper_from_str(cipher: &[u8]) -> StdResult<Cipher, &'static str> {
    match cipher {
        b"aes-128-cbc" => Ok(openssl::symm::Cipher::aes_128_cbc()),
        b"aes-256-cbc" => Ok(openssl::symm::Cipher::aes_256_cbc()),
        b"aes-128-ccm" => Ok(openssl::symm::Cipher::aes_128_ccm()),
        b"aes-256-ccm" => Ok(openssl::symm::Cipher::aes_256_ccm()),
        b"aes-128-ctr" => Ok(openssl::symm::Cipher::aes_128_ctr()),
        b"aes-256-ctr" => Ok(openssl::symm::Cipher::aes_256_ctr()),
        b"aes-128-ecb" => Ok(openssl::symm::Cipher::aes_128_ecb()),
        b"aes-256-ecb" => Ok(openssl::symm::Cipher::aes_256_ecb()),
        b"aes-128-gcm" => Ok(openssl::symm::Cipher::aes_128_gcm()),
        b"aes-256-gcm" => Ok(openssl::symm::Cipher::aes_256_gcm()),
        // b"camellia-128-cbc" => Ok(openssl::symm::Cipher::camellia_128_cbc()),
        // b"camellia-256-cbc" => Ok(openssl::symm::Cipher::camellia_256_cbc()),
        // b"camellia-128-ecb" => Ok(openssl::symm::Cipher::camellia_128_ecb()),
        // b"camellia-256-ecb" => Ok(openssl::symm::Cipher::camellia_256_ecb()),
        // b"chacha20" => Ok(openssl::symm::Cipher::chacha20()),
        b"des-cbc" => Ok(openssl::symm::Cipher::des_cbc()),
        b"des-ecb" => Ok(openssl::symm::Cipher::des_ecb()),
        b"des-ede3-cbc" => Ok(openssl::symm::Cipher::des_ede3_cbc()),
        b"des-ede3-ecb" => Ok(openssl::symm::Cipher::des_ede3_ecb()),
        _ => Err("unsupported cipher"),
    }
}

/*
--- @within crypto
--- Encrypts the input data using the specified cipher and key.
--- Returns the encrypted data or `nil` and an error message.
---
--- @param cipher The cipher to use for encryption (eg. "aes-256-cbc").
--- @param key The key to use for encryption.
--- @param iv The initialization vector to use for encryption.
--- @param data The input data to encrypt.
--- @param padding If `false`, disables padding (default: `true`).
function crypto.encrypt(cipher: string, key: string, iv: string?, data: Bytes | string, padding: boolean?): (string?, string?)
    return nil :: any
end
*/
async fn encrypt(
    _: Lua,
    (cipher, key, iv, data, padding): (
        LuaString,
        BString,
        Option<BString>,
        FlexBytes,
        Option<bool>,
    ),
) -> Result<StdResult<BString, String>> {
    let t = lua_try!(ciper_from_str(&cipher.as_bytes()));
    let data = data.into_bytes();
    let result = spawn_blocking(move || {
        let iv = iv.as_ref().map(|iv| iv.as_ref());
        run_crypter(t, Mode::Encrypt, &key, iv, &data, padding.unwrap_or(true))
    })
    .await
    .expect("failed to join thread");
    let output = BString::new(lua_try!(result));
    Ok(Ok(output))
}

/*
--- @within crypto
--- Decrypts the input data using the specified cipher and key.
--- Returns the decrypted data or `nil` and an error message.
---
--- @param cipher The cipher to use for decryption (eg. "aes-256-cbc").
--- @param key The key to use for decryption.
--- @param iv The initialization vector to use for decryption.
--- @param data The input data to decrypt.
--- @param padding If `false`, disables padding (default: `true`).
function crypto.decrypt(cipher: string, key: string, iv: string?, data: Bytes | string, padding: boolean?): (string?, string?)
    return nil :: any
end
*/
async fn decrypt(
    _: Lua,
    (cipher, key, iv, data, padding): (
        LuaString,
        BString,
        Option<BString>,
        FlexBytes,
        Option<bool>,
    ),
) -> Result<StdResult<BString, String>> {
    let t = lua_try!(ciper_from_str(&cipher.as_bytes()));
    let data = data.into_bytes();
    let result = spawn_blocking(move || {
        let iv = iv.as_ref().map(|iv| iv.as_ref());
        run_crypter(t, Mode::Decrypt, &key, iv, &data, padding.unwrap_or(true))
    })
    .await
    .expect("failed to join thread");
    let output = BString::new(lua_try!(result));
    Ok(Ok(output))
}

fn run_crypter(
    t: Cipher,
    mode: Mode,
    key: &[u8],
    iv: Option<&[u8]>,
    data: &[u8],
    padding: bool,
) -> StdResult<Vec<u8>, openssl::error::ErrorStack> {
    let mut c = Crypter::new(t, mode, key, iv)?;
    c.pad(padding);
    let mut out = vec![0; data.len() + t.block_size()];
    let count = c.update(data, &mut out)?;
    let rest = c.finalize(&mut out[count..])?;
    out.truncate(count + rest);
    Ok(out)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        // Hashing
        ("sha1", lua.create_function(sha1)?),
        ("sha256", lua.create_function(sha256)?),
        ("blake3", lua.create_function(blake3)?),
        ("json_digest", lua.create_function(json_digest)?),
        // Encryption
        ("encrypt", lua.create_async_function(encrypt)?),
        ("decrypt", lua.create_async_function(decrypt)?),
    ])
}

/*
return crypto
*/

#[cfg(test)]
mod tests {
    use bstr::BString;
    use mlua::{chunk, Function, Lua, Result};

    #[ntex::test]
    async fn test_hashing() -> Result<()> {
        let lua = Lua::new();

        let crypto = super::create_module(&lua)?;
        lua.globals().set(
            "hex_encode",
            Function::wrap(|x: BString| Ok(hex::encode(x))),
        )?;

        lua.load(chunk! {
            local sha1 = $crypto.sha1("hello")
            assert(sha1 == "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d", "sha1 hash mismatch")
            local sha1raw = $crypto.sha1("hello", true)
            assert(hex_encode(sha1raw) == "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d", "sha1 raw hash mismatch")

            local sha256 = $crypto.sha256("hello")
            assert(sha256 == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", "sha256 hash mismatch")
            local sha256raw = $crypto.sha256("hello", true)
            assert(hex_encode(sha256raw) == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", "sha256 raw hash mismatch")

            local blake3 = $crypto.blake3("hello")
            assert(blake3 == "ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f", "blake3 hash mismatch")
        })
        .exec_async()
        .await
    }

    #[ntex::test]
    async fn test_json_digest() -> Result<()> {
        let lua = Lua::new();

        let crypto = super::create_module(&lua)?;
        lua.load(chunk! {
            local digest1 = $crypto.json_digest("{\"foo\":\"bar\",\"a\":\"b\"}")
            local digest2 = $crypto.json_digest("{\"a\":\"b\",\"foo\":\"bar\"}")
            assert(digest1 == digest2, "json digest should be deterministic")
        })
        .exec_async()
        .await
    }

    #[ntex::test]
    async fn test_encrypt_decrypt() -> Result<()> {
        let lua = Lua::new();

        let crypto = super::create_module(&lua)?;
        lua.load(chunk! {
            local cipher = "aes-128-cbc"
            local data = "Some Crypto Text"
            local key = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F"
            local iv = "\x00\x01\x02\x03\x04\x05\x06\x07\x00\x01\x02\x03\x04\x05\x06\x07"
            local encrypted, err = $crypto.encrypt(cipher, key, iv, data)
            assert(encrypted, err)
            local decrypted, err = $crypto.decrypt(cipher, key, iv, encrypted)
            assert(decrypted, err)
            assert(decrypted == data, "decrypted data should match original data")
        })
        .exec_async()
        .await
    }
}

use std::result::Result as StdResult;

use base64::Engine as _;
use bstr::BString;
use mlua::{Lua, Result, String as LuaString, Table};
use rand::distributions::Standard;
use rand::{thread_rng, Rng as _};

/*
--- @class utils
--- @tag module
---
--- Built-in module with various utility functions.
local utils = {}
*/

/*
--- @within utils
--- Generates a random number in a range [0, 1).
function utils.random(): number
    return nil :: any
end
*/
fn random(_: &Lua, _: ()) -> Result<f64> {
    Ok(thread_rng().sample(Standard))
}

/*
--- @within utils
--- Generates a random integer in a range [low, high).
---
--- @param low Lower bound of the range.
--- @param high Upper bound of the range (non-inclusive).
function utils.random_range(low: number, high: number): number
    return nil :: any
end
*/
fn random_range(_: &Lua, (low, high): (i64, i64)) -> Result<i64> {
    // Ensure that range is not empty
    if low >= high {
        return Ok(low);
    }
    Ok(thread_rng().gen_range(low..high))
}

/*
--- @within utils
--- Generates a random string of a given length.
--- If no charset is provided, the string will be alphanumeric (a-z, A-Z and 0-9).
---
--- @param len Length of the string.
--- @param charset Optional charset of the string. Can be "hex" to generate a hexadecimal string.
---
--- @return string
function utils.random_string(len: number, charset: string?): string
    return nil :: any
end
*/
fn random_string(_: &Lua, (len, charset): (usize, Option<String>)) -> Result<String> {
    Ok(crate::utils::random_string(len, charset.as_deref()))
}

/*
--- @within utils
--- Encodes a string to base64 using the standard alphabet (with `+` and `/`).
---
--- @param data Input string.
--- @param padding Optional flag to enable padding. Default is `false`.
function utils.base64_encode(data: string, padding: boolean?): string
    return nil :: any
end
*/
fn base64_encode(_: &Lua, (data, padding): (LuaString, Option<bool>)) -> Result<String> {
    if padding.unwrap_or_default() {
        Ok(base64::engine::general_purpose::STANDARD.encode(data.as_bytes()))
    } else {
        Ok(base64::engine::general_purpose::STANDARD_NO_PAD.encode(data.as_bytes()))
    }
}

/*
--- @within utils
--- Decodes a base64 string using the standard alphabet (with `+` and `/`).
---
--- @param data Input string.
--- @param padding Optional flag to enable padding. Default is `false`.
function utils.base64_decode(data: string, padding: boolean?): string
    return nil :: any
end
*/
fn base64_decode(
    lua: &Lua,
    (data, padding): (LuaString, Option<bool>),
) -> Result<StdResult<LuaString, String>> {
    let data = if padding.unwrap_or_default() {
        lua_try!(base64::engine::general_purpose::STANDARD.decode(data.as_bytes()))
    } else {
        lua_try!(base64::engine::general_purpose::STANDARD_NO_PAD.decode(data.as_bytes()))
    };
    Ok(Ok(lua.create_string(&data)?))
}

/*
--- @within utils
--- Encodes a string to base64 using the URL-safe alphabet (with `-` and `_`).
---
--- @param data Input string.
--- @param padding Optional flag to enable padding. Default is `false`.
function utils.base64url_encode(data: string, padding: boolean?): string
    return nil :: any
end
*/
fn base64url_encode(_: &Lua, (data, padding): (LuaString, Option<bool>)) -> Result<String> {
    if padding.unwrap_or_default() {
        Ok(base64::engine::general_purpose::URL_SAFE.encode(data.as_bytes()))
    } else {
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data.as_bytes()))
    }
}

/*
--- @within utils
--- Decodes a base64 string using the URL-safe alphabet (with `-` and `_`).
---
--- @param data Input string.
--- @param padding Optional flag to enable padding. Default is `false`.
function utils.base64url_decode(data: string, padding: boolean?): string
    return nil :: any
end
*/
fn base64url_decode(
    lua: &Lua,
    (data, padding): (LuaString, Option<bool>),
) -> Result<StdResult<LuaString, String>> {
    let data = if padding.unwrap_or_default() {
        lua_try!(base64::engine::general_purpose::URL_SAFE.decode(data.as_bytes()))
    } else {
        lua_try!(base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(data.as_bytes()))
    };
    Ok(Ok(lua.create_string(&data)?))
}

/*
--- @within utils
--- Encodes a string as hex string using lowercase characters.
---
--- @param data Input string.
function utils.hex_encode(data: string): string
    return nil :: any
end
*/
fn hex_encode(_: &Lua, data: LuaString) -> Result<String> {
    Ok(hex::encode(data.as_bytes()))
}

/*
--- @within utils
--- Decodes a hex string to a byte string.
--- Returns `nil` and an error message if the input is invalid.
---
--- @param data Input string.
function utils.hex_decode(data: string): (string?, string?)
    return nil :: any
end
*/
fn hex_decode(_: &Lua, data: LuaString) -> Result<StdResult<BString, String>> {
    Ok(Ok(BString::new(lua_try!(hex::decode(data.as_bytes())))))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("random", lua.create_function(random)?),
        ("random_range", lua.create_function(random_range)?),
        ("random_string", lua.create_function(random_string)?),
        ("base64_encode", lua.create_function(base64_encode)?),
        ("base64_decode", lua.create_function(base64_decode)?),
        ("base64url_encode", lua.create_function(base64url_encode)?),
        ("base64url_decode", lua.create_function(base64url_decode)?),
        ("hex_encode", lua.create_function(hex_encode)?),
        ("hex_decode", lua.create_function(hex_decode)?),
    ])
}

/*
return utils
*/

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_random() -> Result<()> {
        let lua = Lua::new();

        let utils = super::create_module(&lua)?;
        lua.load(chunk! {
            local n = $utils.random()
            assert(type(n) == "number" and n >= 0 and n < 1, "invalid random number")

            local s = $utils.random_string(8)
            assert(type(s) == "string" and #s == 8, "invalid random alphanumeric string")
            assert(s:match("^[0-9a-zA-Z]+$"), "invalid random alphanumeric string")
            local s_hex = $utils.random_string(5, "hex")
            assert(s_hex:match("^[0-9a-f]+$"), "invalid random hex string")

            local r = $utils.random_range(10, 20)
            assert(type(r) == "number" and r >= 10 and r < 20, "invalid random empty range")
            local r2 = $utils.random_range(10, 10)
            assert(r2 == 10, "invalid random empty range")
        })
        .exec()
    }

    #[test]
    fn test_base64() -> Result<()> {
        let lua = Lua::new();

        let utils = super::create_module(&lua)?;
        lua.load(chunk! {
            // Encode (standard alphabet)
            local s = "hello internet~!"
            local b64 = $utils.base64_encode(s)
            assert(b64 == "aGVsbG8gaW50ZXJuZXR+IQ", "invalid base64 encoding")
            local b64pad = $utils.base64_encode(s, true)
            assert(b64pad == "aGVsbG8gaW50ZXJuZXR+IQ==", "invalid base64 encoding with padding")

            // Decode (standard alphabet)
            local s2 = $utils.base64_decode(b64)
            assert(s2 == s, "invalid base64 decoding")
            local s3 = $utils.base64_decode(b64pad, true)
            assert(s3 == s, "invalid base64 decoding with padding")

            // Encode (URL-safe alphabet)
            local b64url = $utils.base64url_encode(s)
            assert(b64url == "aGVsbG8gaW50ZXJuZXR-IQ", "invalid URL-safe base64 encoding")
            local b64urlpad = $utils.base64url_encode(s, true)
            assert(b64urlpad == "aGVsbG8gaW50ZXJuZXR-IQ==", "invalid URL-safe base64 encoding with padding")

            // Decode (URL-safe alphabet)
            local s4 = $utils.base64url_decode(b64url)
            assert(s4 == s, "invalid URL-safe base64 decoding")
            local s5 = $utils.base64url_decode(b64urlpad, true)
            assert(s5 == s, "invalid URL-safe base64 decoding with padding")

            // Invalid input
            local r, err = $utils.base64_decode("wrong base64")
            assert(r == nil and err ~= nil, "invalid base64 decoding result")
        })
        .exec()
    }

    #[test]
    fn test_hex() -> Result<()> {
        let lua = Lua::new();

        let utils = super::create_module(&lua)?;
        lua.load(chunk! {
            local s = "hello internet~!"
            local hex = $utils.hex_encode(s)
            assert(hex == "68656c6c6f20696e7465726e65747e21", "invalid hex encoding")

            local s2 = $utils.hex_decode(hex)
            assert(s2 == s, "invalid hex decoding")

            local r, err = $utils.hex_decode("invalid hex")
            assert(r == nil and err ~= nil, "invalid hex decoding result")
        })
        .exec()
    }
}

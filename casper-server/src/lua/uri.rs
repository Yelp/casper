use mlua::{ExternalResult, Lua, Result as LuaResult, String as LuaString, Table};
use ntex::http::uri::Uri;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};

const URI_COMPONENT_SET: AsciiSet = NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

fn percent_encode(_: &Lua, input: LuaString) -> LuaResult<String> {
    Ok(percent_encoding::percent_encode(input.as_bytes(), &URI_COMPONENT_SET).to_string())
}

fn percent_decode(_: &Lua, input: LuaString) -> LuaResult<String> {
    Ok(percent_encoding::percent_decode(input.as_bytes())
        .decode_utf8()?
        .to_string())
}

fn normalize_uri(_: &Lua, uri: LuaString) -> LuaResult<String> {
    let mut parts = Uri::try_from(uri.as_bytes()).into_lua_err()?.into_parts();

    if let Some(ref path_and_query) = parts.path_and_query {
        // TODO: Normalize using the haproxy rules
        // http://cbonte.github.io/haproxy-dconv/2.4/configuration.html#4.2-http-request%20normalize-uri

        let path = path_and_query.path();

        if let Some(query) = path_and_query.query() {
            // Split query to a list of (k, v) items (where `v` is optional)
            let mut query_pairs = query
                .split('&')
                .map(|it| it.splitn(2, '=').collect::<Box<_>>())
                .collect::<Box<_>>();

            // Sort the list
            query_pairs.sort_by_key(|x| x[0]);

            // Build query again from the sorted list
            let query = query_pairs
                .iter()
                .map(|kv| kv.join("="))
                .collect::<Box<_>>()
                .join("&");

            parts.path_and_query = Some(format!("{}?{}", path, query).parse().into_lua_err()?)
        }
    }

    Ok(Uri::from_parts(parts).into_lua_err()?.to_string())
}

pub fn create_module(lua: &Lua) -> LuaResult<Table> {
    lua.create_table_from([
        ("encode", lua.create_function(percent_encode)?),
        ("decode", lua.create_function(percent_decode)?),
        ("normalize", lua.create_function(normalize_uri)?),
    ])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_encode_decode() -> Result<()> {
        let lua = Lua::new();

        let uri = super::create_module(&lua)?;
        lua.load(chunk! {
            assert($uri.encode("foo <bar> - b@z") == "foo%20%3Cbar%3E%20-%20b%40z", "failed to encode")
            assert($uri.decode("foo%20%3Cbar%3E%20-%20b%40z") == "foo <bar> - b@z", "failed to decode")
        })
        .exec()
    }

    #[test]
    fn test_normalize_uri() -> Result<()> {
        let lua = Lua::new();

        let normalize_uri = lua.create_function(super::normalize_uri)?;
        let normalize_uri = |s| normalize_uri.call::<_, String>(s).unwrap();

        assert_eq!(normalize_uri("/a/b/c"), "/a/b/c");
        assert_eq!(normalize_uri("/?x=3&b=2&a=1"), "/?a=1&b=2&x=3");
        assert_eq!(normalize_uri("/?a=3&b=c&a=1"), "/?a=3&a=1&b=c");

        Ok(())
    }
}

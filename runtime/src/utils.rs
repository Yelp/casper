use std::convert::TryInto;
use std::error::Error as StdError;

use hyper::Uri;

pub fn normalize_uri<U, E>(uri: U) -> anyhow::Result<Uri>
where
    U: TryInto<Uri, Error = E>,
    E: StdError + Send + Sync + 'static,
{
    let mut parts = uri.try_into()?.into_parts();

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
                .into_iter()
                .map(|kv| kv.join("="))
                .collect::<Box<_>>()
                .join("&");

            parts.path_and_query = Some(format!("{}?{}", path, query).parse()?)
        }
    }

    Ok(Uri::from_parts(parts)?)
}

pub mod lua {
    use mlua::{ExternalResult, Lua, Result};

    pub fn normalize_uri(_: &Lua, uri: String) -> Result<String> {
        Ok(super::normalize_uri(uri).to_lua_err()?.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_uri;

    #[test]
    fn test_normalize_uri() {
        assert_eq!(normalize_uri("/a/b/c").unwrap(), "/a/b/c");
        assert_eq!(normalize_uri("/?a=3&b=c&a=1").unwrap(), "/?a=3&a=1&b=c");
    }
}

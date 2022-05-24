use std::convert::TryInto;
use std::error::Error as StdError;

use hyper::Uri;
use rand::{distributions::Alphanumeric, thread_rng, Rng, RngCore};

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
                .iter()
                .map(|kv| kv.join("="))
                .collect::<Box<_>>()
                .join("&");

            parts.path_and_query = Some(format!("{}?{}", path, query).parse()?)
        }
    }

    Ok(Uri::from_parts(parts)?)
}

/// Generates a random string with a given length and charset
///
/// Default charset is Alphanumeric
pub fn random_string(len: usize, charset: Option<&str>) -> String {
    let mut rng = thread_rng();
    match charset {
        None | Some("") => rng
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect(),
        Some("hex") => {
            let mut buf = vec![0u8; (len + 1) / 2];
            rng.fill_bytes(&mut buf);
            let mut s = hex::encode(&buf);
            if len % 2 != 0 {
                s.pop();
            }
            s
        }
        Some(charset) => {
            let charset = charset.chars().collect::<Vec<_>>();
            (0..len)
                .map(|_| charset[rng.gen_range(0..charset.len())])
                .collect()
        }
    }
}

pub mod zstd;

#[cfg(test)]
mod tests {
    use super::{normalize_uri, random_string};

    #[test]
    fn test_normalize_uri() {
        assert_eq!(normalize_uri("/a/b/c").unwrap(), "/a/b/c");
        assert_eq!(normalize_uri("/?x=3&b=2&a=1").unwrap(), "/?a=1&b=2&x=3");
        assert_eq!(normalize_uri("/?a=3&b=c&a=1").unwrap(), "/?a=3&a=1&b=c");
    }

    #[test]
    fn test_random_string() {
        assert!(random_string(0, None).len() == 0);
        assert!(random_string(8, Some("hex")).len() == 8);
        assert!(random_string(5, Some("hex")).len() == 5);

        // Custom charset
        for c in random_string(32, Some("qw!")).chars() {
            assert!(c == 'q' || c == 'w' || c == '!');
        }
    }
}

use std::time::Duration;

use http::uri::{InvalidUriParts, Scheme};
use hyper::header::{self, HeaderMap, HeaderName};
use hyper::{client::HttpConnector, Body, Client, Request, Response, Uri};
use mlua::{Function, Lua, RegistryKey, Result as LuaResult, Table};
use once_cell::sync::Lazy;

static HTTP_CLIENT: Lazy<Client<HttpConnector>> = Lazy::new(Client::new);

static HOP_BY_HOP_HEADERS: Lazy<[HeaderName; 8]> = Lazy::new(|| {
    [
        header::CONNECTION,
        HeaderName::from_static("keep-alive"),
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ]
});

#[derive(thiserror::Error, Debug)]
pub enum ProxyError {
    #[error("invalid destination: {0}")]
    Uri(#[from] InvalidUriParts),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error(transparent)]
    Http(#[from] hyper::Error),
}

pub fn filter_hop_headers(headers: &mut HeaderMap) {
    for header in &*HOP_BY_HOP_HEADERS {
        headers.remove(header);
    }
}

pub async fn proxy_to_downstream(
    mut req: Request<Body>,
    dst: Option<Uri>,
    timeout: Option<Duration>,
) -> Result<Response<Body>, ProxyError> {
    // Set destination to forward request
    let mut parts = req.uri().clone().into_parts();
    if let Some(dst_parts) = dst.map(|dst| dst.into_parts()) {
        if let Some(scheme) = dst_parts.scheme {
            parts.scheme = Some(scheme);
        }
        if let Some(authority) = dst_parts.authority {
            parts.authority = Some(authority);
        }
        if let Some(path_and_query) = dst_parts.path_and_query {
            parts.path_and_query = Some(path_and_query);
        }
    }
    // Set scheme to http if not set
    if parts.scheme.is_none() {
        parts.scheme = Some(Scheme::HTTP);
    }
    *req.uri_mut() = Uri::from_parts(parts)?;

    filter_hop_headers(req.headers_mut());

    // Proxy to a downstream service with timeout
    let mut res = match timeout {
        Some(timeout) => Ok(tokio::time::timeout(timeout, HTTP_CLIENT.request(req)).await??),
        None => Ok(HTTP_CLIENT.request(req).await?),
    };

    if let Ok(res) = res.as_mut() {
        filter_hop_headers(res.headers_mut());
    }

    res
}

pub(crate) fn set_headers_metatable(lua: &Lua, headers: Table) -> LuaResult<()> {
    struct MetatableHelperKey(RegistryKey);

    if let Some(key) = lua.app_data_ref::<MetatableHelperKey>() {
        return lua.registry_value::<Function>(&key.0)?.call(headers);
    }

    // Create new metatable helper and cache it
    let metatable_helper: Function = lua
        .load(
            r#"
            local headers = ...
            local metatable = {
                -- A mapping from a normalized (all lowercase) header name to its
                -- first-seen case, populated the first time a header is seen.
                normalized_to_original_case = {},
            }

            -- Add existing keys
            for key in pairs(headers) do
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                metatable.normalized_to_original_case[normalized_key] = key
            end

            -- When looking up a key that doesn't exist from the headers table, check
            -- if we've seen this header with a different casing, and return it if so.
            metatable.__index = function(tbl, key)
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                local original_key = metatable.normalized_to_original_case[normalized_key]
                if original_key ~= nil and original_key ~= key then
                    return tbl[original_key]
                end
                return nil
            end

            -- When adding a new key to the headers table, check if we've seen this
            -- header with a different casing, and set that key instead.
            metatable.__newindex = function(tbl, key, value)
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                local original_key = metatable.normalized_to_original_case[normalized_key]
                if original_key == nil then
                    metatable.normalized_to_original_case[normalized_key] = key
                    original_key = key
                end
                rawset(tbl, original_key, value)
            end

            setmetatable(headers, metatable)
        "#,
        )
        .into_function()?;

    // Store the helper in the Lua registry
    let registry_key = lua.create_registry_value(metatable_helper.clone())?;
    lua.set_app_data(MetatableHelperKey(registry_key));

    metatable_helper.call(headers)
}

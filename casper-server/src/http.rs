use http::header::{self, HeaderMap, HeaderName};
use http::uri::{InvalidUri, InvalidUriParts, Scheme};
use hyper::client::connect::Connect;
use hyper::{Body, Client, Response, Uri};
use mlua::{ExternalResult, Result as LuaResult};

use crate::lua::{LuaRequest, LuaResponse};

#[allow(clippy::declare_interior_mutable_const)]
const HOP_BY_HOP_HEADERS: [HeaderName; 8] = [
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

#[derive(thiserror::Error, Debug)]
pub enum ProxyError {
    #[error("invalid upstream: {0}")]
    Uri(#[from] InvalidUriParts),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error(transparent)]
    Http(#[from] hyper::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum UriError {
    #[error(transparent)]
    Uri(#[from] InvalidUri),
    #[error(transparent)]
    UriParts(#[from] InvalidUriParts),
}

pub fn filter_hop_headers(headers: &mut HeaderMap) {
    for header in HOP_BY_HOP_HEADERS {
        headers.remove(header);
    }
}

fn merge_uri(src: Uri, dst: &str) -> Result<Uri, UriError> {
    let mut parts = src.into_parts();
    let dst_uri = dst.parse::<Uri>()?;
    let dst_uri_parts = dst_uri.into_parts();

    // Use scheme from dst or set it to `http` is not set
    if let Some(scheme) = dst_uri_parts.scheme {
        parts.scheme = Some(scheme);
    }
    parts.scheme = parts.scheme.or(Some(Scheme::HTTP));

    if let Some(authority) = dst_uri_parts.authority {
        parts.authority = Some(authority);
    }

    if let Some(path_and_query) = dst_uri_parts.path_and_query {
        // Ignore path component is the dst uri does not has it
        if path_and_query.as_str() != "/" || dst.trim_end().ends_with("/") {
            parts.path_and_query = Some(path_and_query);
        }
    }

    Ok(Uri::from_parts(parts)?)
}

async fn send_to_upstream<C>(client: &Client<C>, req: LuaRequest) -> Result<LuaResponse, ProxyError>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    let timeout = req.timeout();
    let mut req = req.into_inner();

    filter_hop_headers(req.headers_mut());

    // Proxy to an upstream service with timeout
    let mut resp = match timeout {
        Some(timeout) => tokio::time::timeout(timeout, client.request(req)).await??,
        None => client.request(req).await?,
    };

    filter_hop_headers(resp.headers_mut());

    let mut resp = LuaResponse::from(resp);
    resp.is_proxied = true;

    Ok(resp)
}

pub async fn proxy_to_upstream<C>(
    client: &Client<C>,
    mut req: LuaRequest,
    upstream: Option<&str>,
) -> LuaResult<LuaResponse>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    // Merge request uri with the upstream uri
    if let Some(upstream) = upstream {
        let new_uri = merge_uri(req.uri().clone(), upstream).to_lua_err()?;
        *req.uri_mut() = new_uri;
    }

    match send_to_upstream(client, req).await {
        Ok(resp) => Ok(resp),
        err @ Err(ProxyError::Uri(..)) => err.to_lua_err(),
        err @ Err(ProxyError::Timeout(..)) => {
            let resp = Response::builder()
                .status(504)
                .header(header::CONTENT_TYPE, "text/plan")
                .body(Body::from(err.err().unwrap().to_string()))
                .to_lua_err()?;
            Ok(LuaResponse::from(resp))
        }
        Err(err) => {
            let resp = Response::builder()
                .status(502)
                .header(header::CONTENT_TYPE, "text/plan")
                .body(Body::from(err.to_string()))
                .to_lua_err()?;
            Ok(LuaResponse::from(resp))
        }
    }
}

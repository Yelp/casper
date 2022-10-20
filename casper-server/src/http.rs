use http::uri::{InvalidUriParts, Scheme};
use hyper::client::connect::Connect;
use hyper::header::{self, HeaderMap, HeaderName};
use hyper::{Client, Uri};

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

pub fn filter_hop_headers(headers: &mut HeaderMap) {
    for header in HOP_BY_HOP_HEADERS {
        headers.remove(header);
    }
}

pub async fn proxy_to_upstream<C>(
    client: Client<C>,
    req: LuaRequest,
) -> Result<LuaResponse, ProxyError>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    let timeout = req.timeout();
    let upstream = req.upstream();
    let mut req = req.into_inner();

    // Set upstream to forward request
    let mut parts = req.uri().clone().into_parts();
    if let Some(ups_parts) = upstream.map(|uri| uri.into_parts()) {
        if let Some(scheme) = ups_parts.scheme {
            parts.scheme = Some(scheme);
        }
        if let Some(authority) = ups_parts.authority {
            parts.authority = Some(authority);
        }
        if let Some(path_and_query) = ups_parts.path_and_query {
            parts.path_and_query = Some(path_and_query);
        }
    }
    // Set scheme to http if not set
    if parts.scheme.is_none() {
        parts.scheme = Some(Scheme::HTTP);
    }
    *req.uri_mut() = Uri::from_parts(parts)?;

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

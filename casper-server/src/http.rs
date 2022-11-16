use std::error::Error as StdError;
use std::mem;

use actix_http::header::{self, HeaderMap, HeaderName, HeaderValue};
use actix_http::uri::{InvalidUri, InvalidUriParts, Scheme, Uri};
use actix_http::{BoxedPayloadStream, Payload, StatusCode};
use awc::error::{ConnectError, SendRequestError};
use awc::Client;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use mlua::{ExternalResult, Result as LuaResult};

use crate::lua::{LuaBody, LuaRequest, LuaResponse};

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
        if path_and_query.as_str() != "/" || dst.trim_end().ends_with('/') {
            parts.path_and_query = Some(path_and_query);
        }
    }

    Ok(Uri::from_parts(parts)?)
}

async fn send_to_upstream(
    client: Client,
    mut req: LuaRequest,
) -> Result<LuaResponse, SendRequestError> {
    let mut client_req = client
        .request(req.method().clone(), req.uri().clone())
        .no_decompress();

    if let Some(timeout) = req.timeout() {
        client_req = client_req.timeout(timeout);
    }

    *client_req.headers_mut() = mem::replace(client_req.headers_mut(), HeaderMap::new());

    filter_hop_headers(req.headers_mut());

    // Proxy to an upstream service
    let mut resp: LuaResponse = client_req
        .send_body(LuaBody::from(req.take_body()))
        .await?
        .map_body(|_, b| Payload::Stream {
            payload: Box::pin(b) as BoxedPayloadStream,
        })
        .into();
    resp.is_proxied = true;

    filter_hop_headers(resp.headers_mut());

    Ok(resp)
}

pub async fn proxy_to_upstream(
    client: Client,
    mut req: LuaRequest,
    upstream: Option<&str>,
) -> LuaResult<LuaResponse> {
    // Merge request uri with the upstream uri
    if let Some(upstream) = upstream {
        let new_uri = merge_uri(req.uri().clone(), upstream).to_lua_err()?;
        *req.uri_mut() = new_uri;
    }

    match send_to_upstream(client, req).await {
        Ok(resp) => Ok(resp),
        err @ Err(SendRequestError::Url(..)) => err.map_err(|e| e.to_string()).to_lua_err(),
        err @ Err(SendRequestError::Timeout)
        | err @ Err(SendRequestError::Connect(ConnectError::Timeout)) => {
            let mut resp = LuaResponse::new(LuaBody::from(err.err().unwrap().to_string()));
            *resp.status_mut() = StatusCode::GATEWAY_TIMEOUT;
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plan"));
            Ok(resp)
        }
        Err(err) => {
            let mut resp = LuaResponse::new(LuaBody::from(err.to_string()).into());
            *resp.status_mut() = StatusCode::BAD_GATEWAY;
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plan"));
            Ok(resp)
        }
    }
}

pub async fn buffer_payload(payload: &mut Payload) -> Result<Bytes, Box<dyn StdError>> {
    let mut bytes = BytesMut::new();
    while let Some(item) = payload.next().await {
        bytes.extend_from_slice(&item?);
    }
    Ok(bytes.freeze())
}

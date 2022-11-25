use std::error::Error as StdError;
use std::io;
use std::mem;

use actix_http::error::PayloadError;
use actix_http::header::{self, HeaderMap, HeaderName, HeaderValue};
use actix_http::uri::{InvalidUri, InvalidUriParts, Scheme, Uri};
use actix_http::{Payload, StatusCode};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use isahc::config::Configurable;
use isahc::HttpClient;
use mlua::{ExternalError, ExternalResult, Result as LuaResult};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

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
    client: HttpClient,
    mut req: LuaRequest,
) -> Result<LuaResponse, isahc::error::Error> {
    let mut client_req_builder = http::Request::builder()
        .uri(req.uri().clone())
        .method(req.method().clone());

    if let Some(timeout) = req.timeout() {
        client_req_builder = client_req_builder.timeout(timeout);
    }

    // TODO: Rewrite this
    let body_bytes = req
        .take_body()
        .buffer()
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
    let body = match body_bytes {
        Some(bytes) => isahc::AsyncBody::from_bytes_static(bytes),
        None => isahc::AsyncBody::empty(),
    };

    let mut client_req = client_req_builder.body(body)?;

    let mut headers = mem::take(req.headers_mut());
    filter_hop_headers(&mut headers);
    for (key, value) in headers {
        client_req.headers_mut().append(key, value);
    }

    // Proxy to an upstream service
    let mut isahc_resp = client.send_async(client_req).await?;
    let mut resp = LuaResponse::new({
        let _body = mem::take(isahc_resp.body_mut());
        let length = isahc_resp
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        let reader = ReaderStream::new(_body.compat());

        LuaBody::Payload {
            payload: Payload::Stream {
                payload: reader.map_err(PayloadError::Io).boxed(),
            },
            length,
            timeout: None,
        }
    });

    resp.is_proxied = true;
    *resp.status_mut() = isahc_resp.status();
    resp.set_version(Some(isahc_resp.version()));

    // Copy headers
    let mut name = None;
    for (key, value) in isahc_resp.headers_mut().drain() {
        if key.is_some() {
            name = key;
        }
        resp.headers_mut().append(name.clone().unwrap(), value);
    }
    filter_hop_headers(resp.headers_mut());

    Ok(resp)
}

pub async fn proxy_to_upstream(
    client: HttpClient,
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
        Err(err) if err.is_timeout() => {
            let mut resp = LuaResponse::new(LuaBody::from(err.to_string()));
            *resp.status_mut() = StatusCode::GATEWAY_TIMEOUT;
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plan"));
            Ok(resp)
        }
        Err(err) if err.is_network() || err.is_server() => {
            let mut resp = LuaResponse::new(LuaBody::from(err.to_string()));
            *resp.status_mut() = StatusCode::BAD_GATEWAY;
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plan"));
            Ok(resp)
        }
        Err(err) => Err(err.to_lua_err()),
    }
}

pub async fn buffer_payload(payload: &mut Payload) -> Result<Bytes, Box<dyn StdError>> {
    let mut bytes = BytesMut::new();
    while let Some(item) = payload.next().await {
        bytes.extend_from_slice(&item?);
    }
    Ok(bytes.freeze())
}

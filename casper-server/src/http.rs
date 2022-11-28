use std::error::Error as StdError;
use std::io;
use std::mem;
use std::pin::Pin;

use actix_http::body::{BodySize, MessageBody};
use actix_http::error::PayloadError;
use actix_http::header::{self, HeaderMap, HeaderName, HeaderValue};
use actix_http::uri::{InvalidUri, InvalidUriParts, Scheme, Uri};
use actix_http::{Method, Payload, StatusCode};
use bytes::{Bytes, BytesMut};
use futures::future::poll_fn;
use futures::{StreamExt, TryStreamExt};
use mlua::{ExternalError, ExternalResult, Result as LuaResult};
use reqwest::Client as HttpClient;

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
) -> Result<LuaResponse, reqwest::Error> {
    let mut client_req = client.request(req.method().clone(), req.uri().to_string());

    if let Some(timeout) = req.timeout() {
        client_req = client_req.timeout(timeout);
    }

    // Add headers
    let mut headers = mem::take(req.headers_mut());
    let mut has_content_length = false;
    filter_hop_headers(&mut headers);
    for (key, value) in headers {
        if key == header::CONTENT_LENGTH {
            has_content_length = true;
        }
        client_req = client_req.header(key, value);
    }

    // Set body
    let mut content_length = None;
    match req.take_body().into() {
        LuaBody::None => {}
        LuaBody::Bytes(bytes) => {
            content_length = Some(bytes.len() as u64);
            client_req = client_req.body(bytes);
        }
        mut stream => {
            if let BodySize::Sized(length) = stream.size() {
                content_length = Some(length);
            }

            let (mut body_tx, body_rx) = futures::channel::mpsc::channel(2);
            tokio::task::spawn_local(async move {
                loop {
                    poll_fn(|cx| body_tx.poll_ready(cx)).await?;
                    let chunk = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx)).await;
                    match chunk {
                        None => {
                            body_tx.disconnect();
                            return anyhow::Ok(());
                        }
                        Some(Ok(bytes)) => body_tx.start_send(Ok(bytes))?,
                        Some(Err(err)) => {
                            body_tx.start_send(Err(err.to_string()))?;
                            body_tx.disconnect();
                            return anyhow::Ok(());
                        }
                    }
                }
            });
            client_req = client_req.body(reqwest::Body::wrap_stream(body_rx));
        }
    };

    // Add content-length header to request if it does not exists
    if req.method() != Method::GET && !has_content_length {
        if let Some(length) = content_length {
            client_req = client_req.header(header::CONTENT_LENGTH, length);
        }
    }

    // Proxy to an upstream service
    let mut upstream_resp = client_req.send().await?;

    let status = upstream_resp.status();
    let version = upstream_resp.version();

    // Take headers
    let mut headers = HeaderMap::with_capacity(upstream_resp.headers().len());
    let mut name = None;
    for (key, value) in upstream_resp.headers_mut().drain() {
        if key.is_some() {
            name = key;
        }
        headers.append(name.clone().unwrap(), value);
    }
    filter_hop_headers(&mut headers);

    // Make LuaResponse
    let mut resp = LuaResponse::new({
        let length = upstream_resp.content_length();
        LuaBody::Payload {
            payload: Payload::Stream {
                payload: upstream_resp
                    .bytes_stream()
                    .map_err(|err| match err {
                        _ if err.is_timeout() => {
                            PayloadError::Io(io::Error::new(io::ErrorKind::TimedOut, err))
                        }
                        _ => PayloadError::Io(io::Error::new(io::ErrorKind::Other, err)),
                    })
                    .boxed(),
            },
            length,
            timeout: None,
        }
    });

    *resp.status_mut() = status;
    resp.set_version(Some(version));
    *resp.headers_mut() = headers;
    resp.is_proxied = true;

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
        Err(err) => {
            let status = match err {
                _ if err.is_timeout() => StatusCode::GATEWAY_TIMEOUT,
                _ if err.is_connect() => StatusCode::SERVICE_UNAVAILABLE,
                _ if err.is_request() => StatusCode::BAD_GATEWAY,
                _ => return Err(err.to_lua_err()),
            };
            let mut resp = LuaResponse::new(LuaBody::from(err.to_string()));
            *resp.status_mut() = status;
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

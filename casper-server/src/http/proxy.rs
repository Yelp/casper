use std::mem;

use mlua::{ExternalError, ExternalResult, Result as LuaResult};
use ntex::http::client::error::SendRequestError;
use ntex::http::client::Client as HttpClient;
use ntex::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use ntex::http::uri::{InvalidUri, InvalidUriParts, Scheme, Uri};
use ntex::http::StatusCode;
use tracing::{debug, field::Empty, instrument, Span};

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

/// Filters out hop-by-hop headers from the request.
pub fn filter_hop_headers(headers: &mut HeaderMap) {
    for header in HOP_BY_HOP_HEADERS {
        headers.remove(header);
    }
}

/// Proxy request to upstream service.
#[instrument(
    skip_all,
    fields(req.method = %req.method(), req.uri = Empty, resp.status = 0)
)]
pub async fn proxy_to_upstream(
    client: HttpClient,
    mut req: LuaRequest,
    upstream: Option<&str>,
) -> LuaResult<LuaResponse> {
    let span = Span::current();

    // Merge request uri with the upstream uri
    if let Some(upstream) = upstream {
        let new_uri = merge_uri(req.uri().clone(), upstream).into_lua_err()?;
        *req.uri_mut() = new_uri;
    }

    span.record("req.uri", req.uri().to_string());

    // Special case to handle websocket upgrade requests
    if super::websocket::is_websocket_upgrade(&req) {
        return super::websocket::proxy_websocket_upgrade(&req).await;
    }

    match forward_to_upstream(client, req).await {
        Ok(resp) => {
            span.record("resp.status", resp.status().as_u16());
            Ok(resp)
        }
        Err(err) => {
            debug!(error = err.to_string(), "proxying error");
            let status = match err {
                SendRequestError::Connect(_) => StatusCode::SERVICE_UNAVAILABLE,
                SendRequestError::Timeout => StatusCode::GATEWAY_TIMEOUT,
                SendRequestError::Send(_)
                | SendRequestError::Response(_)
                | SendRequestError::Http(_)
                | SendRequestError::H2(_) => StatusCode::BAD_GATEWAY,
                _ => return Err(err.to_string().into_lua_err()),
            };
            span.record("resp.status", status.as_u16());

            let mut resp = LuaResponse::new(LuaBody::from(err.to_string()));
            *resp.status_mut() = status;
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plan"));
            Ok(resp)
        }
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

async fn forward_to_upstream(
    client: HttpClient,
    mut req: LuaRequest,
) -> Result<LuaResponse, SendRequestError> {
    let mut client_req = client.request(req.method().clone(), req.uri());

    if let Some(timeout) = req.timeout() {
        client_req = client_req.timeout(timeout);
    }

    // Do not decompress response
    client_req = client_req.no_decompress();

    // Add headers
    let mut headers = mem::take(req.headers_mut());
    filter_hop_headers(&mut headers);
    *client_req.headers_mut() = headers;

    // Proxy to an upstream service
    let body: LuaBody = req.take_body().into();
    let upstream_resp = client_req.send_body(body).await?;

    let mut resp = LuaResponse::from(upstream_resp);
    filter_hop_headers(resp.headers_mut());

    Ok(resp)
}

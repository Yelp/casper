use std::error::Error as StdError;
use std::mem;

use mlua::{ExternalError, ExternalResult, Result as LuaResult};
use ntex::http::body::MessageBody;
use ntex::http::client::error::SendRequestError;
use ntex::http::client::Client as HttpClient;
use ntex::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use ntex::http::uri::{InvalidUri, InvalidUriParts, Scheme, Uri};
use ntex::http::StatusCode;
use ntex::util::{Bytes, BytesMut};
use tracing::warn;

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
    filter_hop_headers(&mut resp.headers_mut());

    Ok(resp)
}

pub async fn proxy_to_upstream(
    client: HttpClient,
    mut req: LuaRequest,
    upstream: Option<&str>,
) -> LuaResult<LuaResponse> {
    // Merge request uri with the upstream uri
    if let Some(upstream) = upstream {
        let new_uri = merge_uri(req.uri().clone(), upstream).into_lua_err()?;
        *req.uri_mut() = new_uri;
    }

    match send_to_upstream(client, req).await {
        Ok(resp) => Ok(resp),
        Err(err) => {
            warn!("Failed to proxy request: {err:?}");
            let status = match err {
                SendRequestError::Connect(_) => StatusCode::SERVICE_UNAVAILABLE,
                SendRequestError::Timeout => StatusCode::GATEWAY_TIMEOUT,
                SendRequestError::Send(_)
                | SendRequestError::Response(_)
                | SendRequestError::Http(_)
                | SendRequestError::H2(_) => StatusCode::BAD_GATEWAY,
                _ => return Err(err.to_string().into_lua_err()),
            };
            let mut resp = LuaResponse::new(LuaBody::from(err.to_string()));
            *resp.status_mut() = status;
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plan"));
            Ok(resp)
        }
    }
}

pub async fn buffer_body(mut body: impl MessageBody) -> Result<Bytes, Box<dyn StdError>> {
    let mut bytes = BytesMut::new();
    while let Some(item) = futures::future::poll_fn(|cx| body.poll_next_chunk(cx)).await {
        bytes.extend_from_slice(&item?);
    }
    Ok(bytes.freeze())
}

pub(crate) mod connector {
    use std::task::{Context, Poll};

    use ntex::http::client::error::ConnectError;
    use ntex::http::client::Connect;
    use ntex::service::Service;
    use ntex::util::BoxFuture;

    pub(crate) struct RetryConnector<T>(pub(crate) T);

    impl<T> Service<Connect> for RetryConnector<T>
    where
        T: Service<Connect, Error = ConnectError>,
    {
        type Response = T::Response;
        type Error = ConnectError;
        type Future<'f> = BoxFuture<'f, Result<Self::Response, Self::Error>> where Self: 'f;

        fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.0.poll_ready(cx)
        }

        fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
            self.0.poll_shutdown(cx)
        }

        fn call(&self, req: Connect) -> Self::Future<'_> {
            Box::pin(async move {
                let mut n = 0;
                loop {
                    n += 1;
                    match self.0.call(req.clone()).await {
                        Err(ConnectError::Timeout | ConnectError::Disconnected(..)) if n < 3 => {
                            continue;
                        }
                        res => return res,
                    }
                }
            })
        }
    }
}

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use mlua::Result as LuaResult;
use ntex::connect::openssl::SslConnector as NtexSslConnector;
use ntex::connect::{ConnectError, Connector};
use ntex::http::body::BodySize;
use ntex::http::error::{DecodeError, EncodeError, PayloadError};
use ntex::http::header::{self, HeaderValue, CONTENT_LENGTH};
use ntex::http::{
    h1, ConnectionType, Payload, RequestHead, RequestHeadType, Response, ResponseHead, StatusCode,
    Uri,
};
use ntex::io::{Io, RecvError, Sealed};
use ntex::time::Millis;
use ntex::util::{ready, Bytes, Either, Stream};
use ntex::ws;
use openssl::ssl::{SslConnector as OpenSslConnector, SslMethod};
use tracing::{trace, warn};

use crate::lua::{LuaBody, LuaRequest, LuaResponse};

#[derive(thiserror::Error, Debug)]
pub enum WsError {
    /// Non-timeout connect error
    #[error(transparent)]
    Connect(#[from] ConnectError),
    /// Connection timeout
    #[error("Connection timeout")]
    ConnectTimeout,
    /// Connector has been disconnected
    #[error("Connector has been disconnected: {0:?}")]
    Disconnected(Option<io::Error>),
    /// Invalid request
    #[error("Invalid request")]
    Request(#[from] EncodeError),
    /// Invalid response
    #[error("Invalid response")]
    Response(#[from] DecodeError),
    /// Response took too long
    #[error("Timeout while waiting for response")]
    Timeout,
    /// Invalid websocket frame
    #[error("Invalid websocket frame")]
    Frame,
    /// Websocket protocol error
    #[error(transparent)]
    Protocol(#[from] ws::error::ProtocolError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl From<Either<io::Error, io::Error>> for WsError {
    fn from(err: Either<io::Error, io::Error>) -> Self {
        WsError::Disconnected(Some(err.into_inner()))
    }
}

impl From<Either<EncodeError, io::Error>> for WsError {
    fn from(err: Either<EncodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => WsError::Request(err),
            Either::Right(err) => WsError::Disconnected(Some(err)),
        }
    }
}

impl From<Either<DecodeError, io::Error>> for WsError {
    fn from(err: Either<DecodeError, io::Error>) -> Self {
        match err {
            Either::Left(err) => WsError::Response(err),
            Either::Right(err) => WsError::Disconnected(Some(err)),
        }
    }
}

/// Checks if the request is a websocket upgrade request
pub(super) fn is_websocket_upgrade(req: &LuaRequest) -> bool {
    match req.orig_req() {
        Some(req) => {
            req.head().upgrade() && ws::verify_handshake(req.head()).is_ok() && req.io().is_some()
        }
        None => false,
    }
}

pub(super) async fn proxy_websocket_upgrade(req: &LuaRequest) -> LuaResult<LuaResponse> {
    let resp = match req.timeout() {
        Some(timeout) => ntex::time::timeout(timeout, forward_websocket_upgrade(req))
            .await
            .map_err(|_| WsError::Timeout)
            .and_then(|res| res),
        None => forward_websocket_upgrade(req).await,
    };

    match resp {
        Ok(resp) => Ok(resp),
        Err(err) => {
            let status = match err {
                WsError::Connect(_) | WsError::Io(_) => StatusCode::SERVICE_UNAVAILABLE,
                WsError::ConnectTimeout | WsError::Timeout => StatusCode::GATEWAY_TIMEOUT,
                WsError::Disconnected(_)
                | WsError::Request(_)
                | WsError::Response(_)
                | WsError::Frame
                | WsError::Protocol(_) => StatusCode::BAD_GATEWAY,
            };
            let mut resp = LuaResponse::new(LuaBody::from(err.to_string()));
            *resp.status_mut() = status;
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plan"));
            Ok(resp)
        }
    }
}

async fn forward_websocket_upgrade(req: &LuaRequest) -> Result<LuaResponse, WsError> {
    // We assume that the request is a websocket upgrade request
    let uri = req.uri();
    let key = req.headers().get(&header::SEC_WEBSOCKET_KEY).unwrap();

    // Establish websocket connection with upstream (as a client)
    let io = connect(uri, None).await?;

    // Send upgrade request and read response
    let codec = h1::ClientCodec::default();
    let resp_head = {
        let mut head = RequestHead::default();
        head.uri = uri.clone();
        head.method = req.method().clone();
        head.headers = req.headers().clone();
        head.set_connection_type(ConnectionType::Upgrade);
        trace!("Ws handshake request: {head:?}");
        io.send(
            (RequestHeadType::Owned(head), BodySize::None).into(),
            &codec,
        )
        .await?;
        io.recv(&codec).await?.ok_or(WsError::Disconnected(None))?
    };
    trace!("Ws handshake response: {resp_head:?}");

    // Verify response
    if resp_head.status != StatusCode::SWITCHING_PROTOCOLS {
        trace!("Expected `101` status code, got: {}", resp_head.status);
        // Return response to the client
        return Ok(make_response(resp_head, io, codec));
    }

    // Check for "UPGRADE" to websocket header
    let has_upgrade_hdr = match resp_head.headers.get(&header::UPGRADE).map(|s| s.to_str()) {
        Some(Ok(s)) => s.to_ascii_lowercase().contains("websocket"),
        _ => false,
    };
    if !has_upgrade_hdr {
        trace!("Invalid upgrade header");
        // Return response to the client
        return Ok(make_response(resp_head, io, codec));
    }

    // Check for "CONNECTION" header
    let has_conn_hdr = match resp_head
        .headers
        .get(&header::CONNECTION)
        .map(|s| s.to_str())
    {
        Some(Ok(s)) => s.to_ascii_lowercase().contains("upgrade"),
        _ => false,
    };
    if !has_conn_hdr {
        // Return response to
        trace!("Invalid connection header");
        return Ok(make_response(resp_head, io, codec));
    }

    // Verify challenge response
    if let Some(hdr_key) = resp_head.headers.get(&header::SEC_WEBSOCKET_ACCEPT) {
        let encoded = ws::hash_key(key.as_ref());
        if hdr_key.as_bytes() != encoded.as_bytes() {
            trace!("Invalid challenge response: expected `{encoded}`, received `{key:?}`");
            // Return response to the client
            return Ok(make_response(resp_head, io, codec));
        }
    } else {
        trace!("Missing `SEC-WEBSOCKET-ACCEPT` header");
        return Ok(make_response(resp_head, io, codec));
    };

    // Prepare response to send back to the downstream client
    let (req_io, req_codec) = req
        .orig_req()
        .unwrap()
        .head()
        .take_io()
        .expect("IO does not attached to the request");
    let resp = {
        let mut resp = Response::new(resp_head.status);
        *resp.headers_mut() = resp_head.headers().clone();
        resp.head_mut().set_connection_type(ConnectionType::Upgrade);
        resp.into_parts().0
    };
    req_io
        .send(h1::Message::Item((resp, BodySize::Empty)), &req_codec)
        .await?;

    // Spawn task to copy frames between upstream and downstream
    let ws_codec = ws::Codec::new();
    let ws_client_codec = ws::Codec::new().client_mode();
    ntex::rt::spawn(async move {
        let res = async {
            loop {
                tokio::select! {
                    // Receive frame from upstream (connected to client) and send to downstream
                    Ok(Some(frame)) = io.recv(&ws_client_codec) => {
                        trace!("received websocket frame from upstream: {frame:?}");
                        let close = match frame {
                            ws::Frame::Close(_) => ws_client_codec.is_closed(),
                            _ => false,
                        };
                        let msg = ws_frame2message(frame)?;
                        req_io.encode(msg, &ws_codec)?;
                        if close {
                            req_io.flush(true).await?;
                            break;
                        }
                    }
                    // Receive frame from downstream and send to upstream
                    Ok(Some(frame)) = req_io.recv(&ws_codec) => {
                        trace!("received websocket frame from downstream: {frame:?}");
                        let close = match frame {
                            ws::Frame::Close(_) => ws_codec.is_closed(),
                            _ => false,
                        };
                        let msg = ws_frame2message(frame)?;
                        io.encode(msg, &ws_client_codec)?;
                        if close {
                            io.flush(true).await?;
                            break;
                        }
                    }
                    else => break,
                }
            }
            Ok::<_, WsError>(())
        }
        .await;
        if let Err(err) = res {
            warn!("Error copying websocket frames: {err:?}");
        }
        io.close();
        req_io.close();
    });

    let mut resp = LuaResponse::from(resp_head);
    resp.set_proxied(true);
    Ok(resp)
}

async fn connect(uri: &Uri, timeout: Option<Millis>) -> Result<Io<Sealed>, WsError> {
    let scheme = uri.scheme_str();
    let fut = async {
        if scheme == Some("wss") || scheme == Some("https") {
            let ssl_connector = OpenSslConnector::builder(SslMethod::tls_client())
                .expect("Failed to create SSL connector")
                .build();
            let io = NtexSslConnector::new(ssl_connector)
                .connect(uri.clone())
                .await?
                .seal();
            Ok::<_, WsError>(io)
        } else {
            let io = Connector::new().connect(uri.clone()).await?.seal();
            Ok::<_, WsError>(io)
        }
    };
    match timeout {
        Some(to) => ntex::time::timeout(to, fut)
            .await
            .map_err(|_| WsError::ConnectTimeout)?,
        None => fut.await,
    }
}

fn make_response(resp_head: ResponseHead, io: Io<Sealed>, codec: h1::ClientCodec) -> LuaResponse {
    let mut resp = LuaResponse::from(resp_head);
    resp.set_proxied(true);

    if codec.message_type() != h1::MessageType::None {
        let content_length = resp
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|len| len.to_str().ok())
            .and_then(|len| len.parse::<u64>().ok());

        let payload = Payload::from_stream(IoPayloadStream::new(io, codec));
        *resp.body_mut() = LuaBody::from((payload, content_length)).into();
    }

    resp
}

struct IoPayloadStream {
    io: Io<Sealed>,
    codec: h1::ClientPayloadCodec,
}

impl IoPayloadStream {
    fn new(io: Io<Sealed>, codec: h1::ClientCodec) -> Self {
        Self {
            io,
            codec: codec.into_payload_codec(),
        }
    }
}

impl Stream for IoPayloadStream {
    type Item = Result<Bytes, PayloadError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut();
        loop {
            return Poll::Ready(Some(match ready!(this.io.poll_recv(&this.codec, cx)) {
                Ok(Some(chunk)) => Ok(chunk),
                Ok(None) => return Poll::Ready(None),
                Err(RecvError::KeepAlive) => {
                    Err(io::Error::other("Keep-alive").into())
                }
                Err(RecvError::Stop) => {
                    Err(io::Error::other("Dispatcher stopped").into())
                }
                Err(RecvError::WriteBackpressure) => {
                    ready!(this.io.poll_flush(cx, false))?;
                    continue;
                }
                Err(RecvError::Decoder(err)) => Err(err),
                Err(RecvError::PeerGone(Some(err))) => Err(err.into()),
                Err(RecvError::PeerGone(None)) => return Poll::Ready(None),
            }));
        }
    }
}

// Converts received websocket frame to a message
fn ws_frame2message(frame: ws::Frame) -> Result<ws::Message, WsError> {
    match frame {
        ws::Frame::Binary(b) => Ok(ws::Message::Binary(b)),
        ws::Frame::Close(c) => Ok(ws::Message::Close(c)),
        ws::Frame::Continuation(it) => Ok(ws::Message::Continuation(it)),
        ws::Frame::Ping(b) => Ok(ws::Message::Ping(b)),
        ws::Frame::Pong(b) => Ok(ws::Message::Pong(b)),
        ws::Frame::Text(b) => Ok(ws::Message::Text(b.try_into().map_err(|_| WsError::Frame)?)),
    }
}

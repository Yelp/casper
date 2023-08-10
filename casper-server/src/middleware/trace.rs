use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ntex::http::body::{Body, BodySize, MessageBody, ResponseBody};
use ntex::service::{
    forward_poll_ready, forward_poll_shutdown, Middleware, Service, ServiceCall, ServiceCtx,
};
use ntex::util::Bytes;
use ntex::web::{ErrorRenderer, WebRequest, WebResponse};

use pin_project_lite::pin_project;
use tracing::field::Empty;
use tracing::{span, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::http::trace::RequestHeaderCarrier;
use crate::trace::RootSpan;

/// `RequestTracing` is a middleware to capture structured diagnostic when processing an HTTP request.
#[derive(Default, Debug)]
pub struct RequestTracing;

impl RequestTracing {
    /// Create a middleware to trace each request.
    pub fn new() -> RequestTracing {
        RequestTracing
    }
}

impl<S> Middleware<S> for RequestTracing {
    type Service = RequestTracingService<S>;

    fn create(&self, service: S) -> Self::Service {
        RequestTracingService { inner: service }
    }
}

#[derive(Debug, Clone)]
pub struct RequestTracingService<S> {
    inner: S,
}

impl<S, E> Service<WebRequest<E>> for RequestTracingService<S>
where
    S: Service<WebRequest<E>, Response = WebResponse>,
    E: ErrorRenderer,
{
    type Response = WebResponse;
    type Error = S::Error;
    type Future<'f> = TracingResponse<'f, S, E> where S: 'f, E: 'f;

    forward_poll_ready!(inner);
    forward_poll_shutdown!(inner);

    #[inline]
    fn call<'a>(&'a self, req: WebRequest<E>, ctx: ServiceCtx<'a, Self>) -> Self::Future<'a> {
        // Create a root span
        let root_span = {
            let connection_info = req.connection_info();
            span!(
                Level::INFO,
                "HTTP request",
                req.method = %req.method(),
                req.uri = %req.uri(),
                req.host = %connection_info.host(),
                req.peer_addr = %req.peer_addr().map(|addr| addr.to_string()).unwrap_or_default(),
                resp.status_code = Empty,
                otel.name = %format!("{} {}", req.method(), req.uri().path()),
                otel.kind = "server",
                otel.status_code = Empty,
            )
        };

        // Get parent context from request headers
        let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&RequestHeaderCarrier::new(req.headers()))
        });

        root_span.set_parent(parent_context);

        // Store root span in request extensions
        let root_span_wrapper = RootSpan::new(root_span.clone());
        req.extensions_mut().insert(root_span_wrapper);

        let fut = root_span.in_scope(|| ctx.call(&self.inner, req));

        TracingResponse {
            fut,
            span: root_span,
        }
    }
}

//
// Response handling
//

pin_project! {
    pub struct TracingResponse<'f, S: Service<WebRequest<E>>, E>
    where S: 'f, E: 'f
    {
        #[pin]
        fut: ServiceCall<'f, S, WebRequest<E>>,
        span: Span,
    }
}

impl<'f, S, E> Future for TracingResponse<'f, S, E>
where
    S: Service<WebRequest<E>, Response = WebResponse>,
{
    type Output = Result<WebResponse, S::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let span = this.span.clone();
        this.span.in_scope(move || {
            let res = futures::ready!(this.fut.poll(cx));

            // Request is done
            match res {
                Ok(ref response) => {
                    let status_code = response.status().as_u16();
                    span.record("resp.status_code", status_code);
                    if response.status().is_server_error() {
                        span.record("otel.status_code", "ERROR");
                    } else {
                        span.record("otel.status_code", "OK");
                    }
                }
                Err(_) => {
                    span.record("resp.status_code", 500);
                    span.record("otel.status_code", "ERROR");
                }
            }

            Poll::Ready(res.map(|response| {
                response.map_body(move |_, body| {
                    ResponseBody::Other(Body::from_message(StreamSpan { body, span }))
                })
            }))
        })
    }
}

struct StreamSpan {
    body: ResponseBody<Body>,
    span: Span,
}

impl MessageBody for StreamSpan {
    #[inline]
    fn size(&self) -> BodySize {
        self.body.size()
    }

    #[inline]
    fn poll_next_chunk(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Box<dyn Error>>>> {
        self.span.in_scope(|| self.body.poll_next_chunk(cx))
    }
}

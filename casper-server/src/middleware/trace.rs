use std::error::Error;
use std::fmt::Display;
use std::task::{Context, Poll};

use ntex::http::body::{Body, BodySize, MessageBody, ResponseBody};
use ntex::service::{forward_ready, forward_shutdown, Middleware, Service, ServiceCtx};
use ntex::util::Bytes;
use ntex::web::{ErrorRenderer, WebRequest, WebResponse};
use opentelemetry::trace::{self, FutureExt, TraceContextExt, Tracer, TracerProvider as _};
use opentelemetry::{global, Context as OtelContext, KeyValue};

use crate::config::TracingConfig;
use crate::http::trace::{ParentSamplingDecision, RequestHeaderCarrier};

/// `RequestTracing` is a middleware to capture structured diagnostic when processing an HTTP request.
#[derive(Default, Debug)]
pub struct RequestTracing {
    config: TracingConfig,
}

impl RequestTracing {
    /// Create a middleware to trace each request.
    pub fn new(config: Option<TracingConfig>) -> RequestTracing {
        RequestTracing {
            config: config.unwrap_or_default(),
        }
    }
}

impl<S> Middleware<S> for RequestTracing {
    type Service = RequestTracingService<S>;

    fn create(&self, service: S) -> Self::Service {
        let tracer = global::tracer_provider()
            .tracer_builder("casper-opentelemetry")
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(opentelemetry_semantic_conventions::SCHEMA_URL)
            .build();

        RequestTracingService {
            config: self.config.clone(),
            tracer,
            service,
        }
    }
}

#[derive(Debug)]
pub struct RequestTracingService<S> {
    config: TracingConfig,
    tracer: global::BoxedTracer,
    service: S,
}

impl<S, E> Service<WebRequest<E>> for RequestTracingService<S>
where
    S: Service<WebRequest<E>, Response = WebResponse>,
    S::Error: Display,
    E: ErrorRenderer,
{
    type Response = WebResponse;
    type Error = S::Error;

    forward_ready!(service);
    forward_shutdown!(service);

    #[inline]
    async fn call(
        &self,
        req: WebRequest<E>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        // Get parent context from request headers
        let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&RequestHeaderCarrier::new(req.headers()))
        });

        let mut otel_cx = OtelContext::new();

        if self.config.enabled
            && (parent_context.span().span_context().is_valid()
                || self.config.start_new_traces.unwrap_or(true))
        {
            let connection_info = req.connection_info();
            let span_builder = self
                .tracer
                .span_builder(format!("{}", req.method()))
                .with_kind(trace::SpanKind::Server)
                .with_attributes([
                    KeyValue::new("request.method", req.method().to_string()),
                    KeyValue::new("request.uri", req.uri().to_string()),
                    KeyValue::new("request.host", connection_info.host().to_string()),
                    KeyValue::new(
                        "request.peer_addr",
                        req.peer_addr()
                            .map(|addr| addr.to_string())
                            .unwrap_or_default(),
                    ),
                ]);
            let span = self
                .tracer
                .build_with_context(span_builder, &parent_context);
            otel_cx = parent_context.with_span(span);

            // In the firehose mode we need to propagate the sampling decision
            // but ignore it in the app (e.g. always sample).
            if self.config.mode.as_deref() == Some("firehose") {
                if let Some(sampled) = req.headers().get("X-B3-Sampled") {
                    otel_cx = otel_cx.with_value(ParentSamplingDecision(sampled.clone()));
                }
            }
        }

        let res = ctx
            .call(&self.service, req)
            .with_context(otel_cx.clone())
            .await;
        res.map(|response| {
            let span = otel_cx.span();
            span.add_event("received response headers", vec![]);

            let status = response.status();
            span.set_attribute(KeyValue::new(
                "response.status_code",
                status.as_u16() as i64,
            ));
            if status.is_server_error() {
                span.set_status(trace::Status::error(status.to_string()));
            } else if status.is_success() {
                span.set_status(trace::Status::Ok);
            }

            let otel_cx = otel_cx.clone();
            response.map_body(move |_, body| {
                ResponseBody::Other(Body::from_message(StreamSpan { body, otel_cx }))
            })
        })
        .map_err(|err| {
            let span = otel_cx.span();
            span.set_status(trace::Status::error(err.to_string()));
            err
        })
    }
}

struct StreamSpan {
    body: ResponseBody<Body>,
    otel_cx: OtelContext,
}

impl Drop for StreamSpan {
    fn drop(&mut self) {
        self.otel_cx.span().end();
    }
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
        let _cx_guard = self.otel_cx.clone().attach();
        self.body.poll_next_chunk(cx)
    }
}

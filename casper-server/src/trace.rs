use std::convert::Infallible;
use std::time::Duration;

use futures::future::{self, Ready};
use ntex::http::Payload;
use ntex::web::{FromRequest, HttpRequest};
use opentelemetry::sdk::trace::{self, RandomIdGenerator, Sampler};
use opentelemetry_http::hyper::HyperClient;
use tracing::Span;
use tracing_log::LogTracer;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

use crate::config::Config;

pub fn init(config: &Config) {
    let enabled = config.tracing.as_ref().map(|c| c.enabled).unwrap_or(false);

    opentelemetry::global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());

    let mut pipeline_builder = opentelemetry_zipkin::new_pipeline()
        .with_http_client(HyperClient::new_with_timeout(
            hyper::client::Client::new(),
            Duration::from_secs(5),
        ))
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default()),
        );

    if let Some(service_name) = &config.main.service_name {
        pipeline_builder = pipeline_builder.with_service_name(service_name);
    }
    if let Some(tracing_conf) = &config.tracing {
        if let Some(collector_endpoint) = &tracing_conf.collector_endpoint {
            pipeline_builder = pipeline_builder.with_collector_endpoint(collector_endpoint);
        }
    }

    let tracer = pipeline_builder
        .install_batch(opentelemetry::runtime::TokioCurrentThread)
        .expect("failed to create zipkin tracer");

    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer();

    let otel_layer = OpenTelemetryLayer::new(tracer).with_location(false);

    // Convert log records to tracing events
    LogTracer::init().expect("failed to init log tracer");

    if enabled {
        let subscriber = Registry::default()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer);
        tracing::subscriber::set_global_default(subscriber)
    } else {
        let subscriber = Registry::default().with(env_filter).with(fmt_layer);
        tracing::subscriber::set_global_default(subscriber)
    }
    .expect("failed to set default global tracer");
}

/// The root span associated to the in-flight current request.
///
/// It can be used to populate additional properties using values computed or retrieved in the request.
#[derive(Clone)]
pub struct RootSpan(Span);

impl RootSpan {
    pub(crate) fn new(span: Span) -> Self {
        Self(span)
    }
}

impl std::ops::Deref for RootSpan {
    type Target = Span;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RootSpan> for Span {
    fn from(r: RootSpan) -> Self {
        r.0
    }
}

impl<Err> FromRequest<Err> for RootSpan {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        future::ready(Ok(req.extensions().get::<Self>().cloned().unwrap()))
    }
}

use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::LocalBoxFuture;
use hyper::{header::CONTENT_TYPE, service::Service, Body, Request, Response};
use once_cell::sync::Lazy;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, ValueObserver, ValueRecorder};
use opentelemetry_prometheus::PrometheusExporter;
use prometheus::{Encoder, TextEncoder};
use tower::Layer;

pub static OT_STATS: Lazy<OpenTelemetryState> = Lazy::new(OpenTelemetryState::new);

pub struct OpenTelemetryState {
    pub exporter: PrometheusExporter,

    pub connections_counter: Counter<u64>,
    pub active_connections_counter: ActiveCounter,
    pub active_connections_observer: ValueObserver<u64>,

    pub requests_counter: Counter<u64>,
    pub active_requests_counter: ActiveCounter,
    pub active_requests_observer: ValueObserver<u64>,

    pub storage_counter: Counter<u64>,
    pub storage_histogram: ValueRecorder<f64>,
}

impl OpenTelemetryState {
    pub fn new() -> Self {
        let boundaries = vec![
            0.001, 0.002, 0.003, 0.005, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0,
            3.0, 4.0, 5.0, 10.0,
        ];
        let exporter = opentelemetry_prometheus::exporter()
            .with_default_histogram_boundaries(boundaries)
            .init();

        let meter = global::meter("casper");

        let active_connections_counter = ActiveCounter::new(0);
        let active_connections_counter2 = active_connections_counter.clone();
        let active_requests_counter = ActiveCounter::new(0);
        let active_requests_counter2 = active_requests_counter.clone();

        OpenTelemetryState {
            exporter,

            connections_counter: meter
                .u64_counter("http_connections_total")
                .with_description("Total number of HTTP connections processed by the application.")
                .init(),
            active_connections_counter,
            active_connections_observer: meter
                .u64_value_observer("http_connections_current", move |observer| {
                    observer.observe(active_connections_counter2.get(), &[]);
                })
                .with_description(
                    "Current number of HTTP connections being processed by the application.",
                )
                .init(),

            requests_counter: meter
                .u64_counter("http_requests_total")
                .with_description("Total number of HTTP requests processed by the application.")
                .init(),
            active_requests_counter,
            active_requests_observer: meter
                .u64_value_observer("http_requests_current", move |observer| {
                    observer.observe(active_requests_counter2.get(), &[]);
                })
                .with_description(
                    "Current number of HTTP requests being processed by the application.",
                )
                .init(),

            storage_counter: meter
                .u64_counter("storage_requests_total")
                .with_description(
                    "Total number of requests being processed by the storage backend.",
                )
                .init(),
            storage_histogram: meter
                .f64_value_recorder("storage_request_duration_seconds")
                .with_description("The storage backend request latency in seconds.")
                .init(),
        }
    }
}

macro_rules! connections_counter_add {
    ($increment:expr) => {{
        crate::stats::OT_STATS
            .connections_counter
            .add($increment, &[]);

        crate::stats::OT_STATS
            .active_connections_counter
            .inc($increment)
    }};
}

macro_rules! requests_counter_add {
    ($increment:expr) => {{
        crate::stats::OT_STATS.requests_counter.add($increment, &[]);

        crate::stats::OT_STATS
            .active_requests_counter
            .inc($increment)
    }};
}

macro_rules! storage_counter {
    ($increment:expr, $($key:expr => $val:expr),*) => {
        crate::stats::OT_STATS.storage_counter.add(
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    };
}

macro_rules! storage_histogram {
    ($value:expr, $($key:expr => $val:expr),*) => {
        crate::stats::OT_STATS.storage_histogram.record(
            $value,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    };
}

#[derive(Clone, Debug)]
pub struct Instrumentation<S> {
    endpoint: Rc<String>,
    inner: S,
}

impl<S> Service<Request<Body>> for Instrumentation<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + 'static,
    S::Future: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // best practice is to clone the inner service like this
        // see https://github.com/tower-rs/tower/issues/547 for details
        let clone = self.inner.clone();
        let mut inner = mem::replace(&mut self.inner, clone);

        let endpoint = self.endpoint.clone();
        Box::pin(async move {
            let _req_counter_handler = requests_counter_add!(1);

            if req.uri().path() == *endpoint {
                return Ok(Self::metrics_handler(req).await.unwrap_or_else(|_| {
                    Response::builder()
                        .status(500)
                        .body(Body::from("Error encoding metrics"))
                        .expect("Cannot build Response")
                }));
            }
            inner.call(req).await
        })
    }
}

impl<S> Instrumentation<S> {
    async fn metrics_handler(_: Request<Body>) -> Result<Response<Body>, anyhow::Error> {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = OT_STATS.exporter.registry().gather();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(Response::builder()
            .status(200)
            .header(CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))?)
    }
}

pub struct InstrumentationLayer {
    endpoint: Rc<String>,
}

impl InstrumentationLayer {
    pub fn new(endpoint: String) -> Self {
        InstrumentationLayer {
            endpoint: Rc::new(endpoint),
        }
    }
}

impl<S> Layer<S> for InstrumentationLayer {
    type Service = Instrumentation<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Instrumentation {
            endpoint: self.endpoint.clone(),
            inner,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ActiveCounter(Arc<AtomicU64>);

#[derive(Debug)]
pub struct ActiveCounterHandler(Arc<AtomicU64>, u64);

impl ActiveCounter {
    pub fn new(v: u64) -> Self {
        ActiveCounter(Arc::new(AtomicU64::new(v)))
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn inc(&self, n: u64) -> ActiveCounterHandler {
        self.0.fetch_add(n, Ordering::Relaxed);
        ActiveCounterHandler(Arc::clone(&self.0), n)
    }
}

impl Drop for ActiveCounterHandler {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::Relaxed);
    }
}

use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::LocalBoxFuture;
use hyper::{header::CONTENT_TYPE, service::Service, Body, Request, Response};
use once_cell::sync::Lazy;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, ValueRecorder};
use opentelemetry_prometheus::PrometheusExporter;
use prometheus::{Encoder, TextEncoder};
use tower::Layer;

#[derive(Debug, Default)]
pub struct Stats {
    pub total_conns: AtomicUsize,
    pub total_requests: AtomicUsize,
    pub active_conns: ActiveCounter,
    pub active_requests: ActiveCounter,
}

pub static GLOBAL_STATS: Lazy<Stats> = Lazy::new(|| Stats::new());

impl Stats {
    pub fn new() -> Self {
        Stats::default()
    }

    // pub fn total_conns(&self) -> usize {
    //     self.total_conns.load(Ordering::Relaxed)
    // }

    pub fn inc_total_conns(&self, n: usize) {
        self.total_conns.fetch_add(n, Ordering::Relaxed);
    }

    // pub fn total_requests(&self) -> usize {
    //     self.total_requests.load(Ordering::Relaxed)
    // }

    pub fn inc_total_requests(&self, n: usize) {
        self.total_requests.fetch_add(n, Ordering::Relaxed);
    }

    pub fn active_conns(&self) -> usize {
        self.active_conns.get()
    }

    pub fn inc_active_conns(&self, n: usize) -> ActiveCounterHandler {
        self.active_conns.inc(n)
    }

    pub fn active_requests(&self) -> usize {
        self.active_requests.get()
    }

    pub fn inc_active_requests(&self, n: usize) -> ActiveCounterHandler {
        self.active_requests.inc(n)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ActiveCounter(Arc<AtomicUsize>);

#[derive(Debug)]
pub struct ActiveCounterHandler(Arc<AtomicUsize>, usize);

impl ActiveCounter {
    pub fn new(v: usize) -> Self {
        ActiveCounter(Arc::new(AtomicUsize::new(v)))
    }

    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    pub fn inc(&self, n: usize) -> ActiveCounterHandler {
        self.0.fetch_add(n, Ordering::Relaxed);
        ActiveCounterHandler(Arc::clone(&self.0), n)
    }
}

impl Drop for ActiveCounterHandler {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::Relaxed);
    }
}

pub static OT_STATS: Lazy<OpenTelemetryState> = Lazy::new(|| OpenTelemetryState::new());

pub struct OpenTelemetryState {
    pub exporter: PrometheusExporter,
    pub storage_counter: Counter<u64>,
    pub storage_histogram: ValueRecorder<f64>,
}

impl OpenTelemetryState {
    pub fn new() -> Self {
        let exporter = opentelemetry_prometheus::exporter().init();
        let meter = global::meter("casper");
        OpenTelemetryState {
            exporter,
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

#[macro_export]
macro_rules! storage_counter {
    ($increment:expr, $($key:expr => $val:expr),*) => {
        OT_STATS.storage_counter.add(
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    };
}

#[macro_export]
macro_rules! storage_histogram {
    ($value:expr, $($key:expr => $val:expr),*) => {
        OT_STATS.storage_histogram.record(
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

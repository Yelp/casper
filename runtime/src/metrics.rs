use std::collections::HashMap;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use futures::future::LocalBoxFuture;
use hyper::{header::CONTENT_TYPE, service::Service, Body, Request, Response};
use once_cell::sync::Lazy;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, ValueObserver, ValueRecorder};
use opentelemetry_prometheus::PrometheusExporter;
use parking_lot::Mutex;
use prometheus::{Encoder, TextEncoder};
use tokio::sync::RwLock;
use tower::Layer;

use crate::config::MetricsConfig;

static PROMETHEUS_EXPORTER: Lazy<PrometheusExporter> = Lazy::new(|| {
    let boundaries = vec![
        0.001, 0.002, 0.003, 0.005, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0,
        4.0, 5.0, 10.0,
    ];
    opentelemetry_prometheus::exporter()
        .with_default_histogram_boundaries(boundaries)
        .init()
});

pub fn init() {
    let _exporter = Lazy::force(&PROMETHEUS_EXPORTER);
    let _metrics = Lazy::force(&METRICS);
}

pub static METRICS: Lazy<OpenTelemetryMetrics> = Lazy::new(OpenTelemetryMetrics::new);

pub struct OpenTelemetryMetrics {
    pub connections_counter: Counter<u64>,
    pub active_connections_counter: ActiveCounter,
    pub active_connections_observer: ValueObserver<u64>,

    pub requests_counter: Counter<u64>,
    pub requests_histogram: ValueRecorder<f64>,
    pub active_requests_counter: ActiveCounter,
    pub active_requests_observer: ValueObserver<u64>,

    pub storage_counter: Counter<u64>,
    pub storage_histogram: ValueRecorder<f64>,

    pub middleware_histogram: ValueRecorder<f64>,

    pub lua_used_memory: Arc<RwLock<Vec<AtomicU64>>>,
    pub lua_used_memory_observer: ValueObserver<u64>,

    pub num_threads_observer: ValueObserver<u64>,

    //
    // User-defined metrics
    //
    pub counters: Mutex<HashMap<String, Counter<u64>>>,
}

impl OpenTelemetryMetrics {
    pub fn new() -> Self {
        let meter = global::meter("casper");

        let active_connections_counter = ActiveCounter::new(0);
        let active_requests_counter = ActiveCounter::new(0);

        let lua_used_memory = Arc::new(RwLock::default());

        OpenTelemetryMetrics {
            connections_counter: meter
                .u64_counter("http_connections_total")
                .with_description("Total number of HTTP connections processed by the application.")
                .init(),
            active_connections_counter: active_connections_counter.clone(),
            active_connections_observer: meter
                .u64_value_observer("http_connections_current", move |observer| {
                    observer.observe(active_connections_counter.get(), &[]);
                })
                .with_description(
                    "Current number of HTTP connections being processed by the application.",
                )
                .init(),

            requests_counter: meter
                .u64_counter("http_requests_total")
                .with_description("Total number of HTTP requests processed by the application.")
                .init(),
            requests_histogram: meter
                .f64_value_recorder("http_request_duration_seconds")
                .with_description("HTTP request latency in seconds.")
                .init(),
            active_requests_counter: active_requests_counter.clone(),
            active_requests_observer: meter
                .u64_value_observer("http_requests_current", move |observer| {
                    observer.observe(active_requests_counter.get(), &[]);
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

            middleware_histogram: meter
                .f64_value_recorder("middleware_request_duration_seconds")
                .with_description("Middleware only request latency in seconds.")
                .init(),

            lua_used_memory: lua_used_memory.clone(),
            lua_used_memory_observer: meter
                .u64_value_observer("lua_used_memory_bytes", move |observer| {
                    // Almost all the time it's locked for read only
                    if let Ok(lua_used_memory) = lua_used_memory.try_read() {
                        let lua_used_memory_total = lua_used_memory
                            .iter()
                            .map(|v| v.load(Ordering::Relaxed))
                            .sum();
                        observer.observe(lua_used_memory_total, &[]);
                    }
                })
                .with_description("Total memory used by Lua workers.")
                .init(),

            num_threads_observer: meter
                .u64_value_observer("process_threads_count", move |observer| {
                    if let Some(n) = num_threads::num_threads() {
                        observer.observe(n.get() as u64, &[]);
                    }
                })
                .with_description("Current number of active threads.")
                .init(),

            counters: Mutex::default(),
        }
    }
}

pub fn register_custom_metrics(config: MetricsConfig) {
    let meter = global::meter("casper");

    let mut counters = METRICS.counters.lock();
    for (key, config) in config.counters {
        let mut counter = meter.u64_counter(config.name.unwrap_or_else(|| key.clone()));
        if let Some(description) = config.description {
            counter = counter.with_description(description);
        }
        counters.insert(key, counter.init());
    }
}

macro_rules! connections_counter_add {
    ($increment:expr) => {{
        crate::metrics::METRICS
            .connections_counter
            .add($increment, &[]);

        crate::metrics::METRICS
            .active_connections_counter
            .inc($increment)
    }};
}

macro_rules! requests_counter_add {
    ($increment:expr) => {{
        crate::metrics::METRICS
            .requests_counter
            .add($increment, &[]);

        crate::metrics::METRICS
            .active_requests_counter
            .inc($increment)
    }};
}

macro_rules! requests_histogram_rec {
    ($start:expr, $($key:expr => $val:expr),*) => {
        crate::metrics::METRICS.requests_histogram.record(
            $start.elapsed().as_secs_f64(),
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    };
}

macro_rules! storage_counter_add {
    ($increment:expr, $($key:expr => $val:expr),*) => {
        crate::metrics::METRICS.storage_counter.add(
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    };
}

macro_rules! storage_histogram_rec {
    ($start:expr, $($key:expr => $val:expr),*) => {
        crate::metrics::METRICS.storage_histogram.record(
            $start.elapsed().as_secs_f64(),
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    };
}

macro_rules! middleware_histogram_rec {
    ($start:expr, $($key:expr => $val:expr),*) => {
        crate::metrics::METRICS.middleware_histogram.record(
            $start.elapsed().as_secs_f64(),
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    };
}

macro_rules! lua_used_memory_update {
    ($id:expr, $value:expr) => {{
        let lua_used_memory = crate::metrics::METRICS.lua_used_memory.read().await;
        if $id < lua_used_memory.len() {
            lua_used_memory[$id].store($value as u64, ::std::sync::atomic::Ordering::Relaxed);
        } else {
            drop(lua_used_memory);
            let mut lua_used_memory = crate::metrics::METRICS.lua_used_memory.write().await;
            // Double check (situation can be changed after acquiring lock) and grow vector
            if $id >= lua_used_memory.len() {
                lua_used_memory.resize_with($id + 1, Default::default);
            }
            lua_used_memory[$id].store($value as u64, ::std::sync::atomic::Ordering::Relaxed);
        }
    }};
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

            let start = Instant::now();
            let result = inner.call(req).await;
            let mut status = 0i64;
            if let Ok(response) = result.as_ref() {
                status = response.status().as_u16() as i64;
            }
            requests_histogram_rec!(start, "status" => status);

            result
        })
    }
}

impl<S> Instrumentation<S> {
    async fn metrics_handler(_: Request<Body>) -> Result<Response<Body>, anyhow::Error> {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = PROMETHEUS_EXPORTER.registry().gather();
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

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use once_cell::sync::Lazy;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};
use opentelemetry::sdk::export::metrics::aggregation;
use opentelemetry::sdk::metrics::{controllers, processors, selectors};
use opentelemetry::sdk::Resource;
use opentelemetry::{global, KeyValue};
use opentelemetry_prometheus::PrometheusExporter;
use parking_lot::Mutex;
use tokio::sync::RwLock;

use crate::config::{MainConfig, MetricsConfig};

pub(crate) static PROMETHEUS_EXPORTER: Lazy<PrometheusExporter> = Lazy::new(|| {
    let boundaries = vec![
        0.001, 0.003, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5,
        2.0, 3.0, 4.0, 5.0, 7.5, 10.0,
    ];

    let mut attrs = Vec::new();
    if let Ok(service_name) = env::var("SERVICE_NAME") {
        attrs.push(KeyValue::new("service.name", service_name));
    }
    if let Ok(service_instance) = env::var("SERVICE_INSTANCE") {
        attrs.push(KeyValue::new("service.instance", service_instance));
    }
    let controller = controllers::basic(processors::factory(
        selectors::simple::histogram(boundaries),
        aggregation::cumulative_temporality_selector(),
    ))
    .with_resource(Resource::new(attrs))
    .build();

    opentelemetry_prometheus::exporter(controller).init()
});

pub static METRICS: Lazy<OpenTelemetryMetrics> = Lazy::new(OpenTelemetryMetrics::new);

pub fn init(config: &MainConfig) {
    // Set env variable with service name if missing
    if env::var("SERVICE_NAME").is_err() {
        if let Some(service_name) = &config.service_name {
            env::set_var("SERVICE_NAME", service_name);
        }
    }

    // Init metrics
    let _exporter = Lazy::force(&PROMETHEUS_EXPORTER);
    let _metrics = Lazy::force(&METRICS);
}

pub struct OpenTelemetryMetrics {
    pub connections_counter: Counter<u64>,
    pub active_connections_counter: ActiveCounter,
    pub active_connections_gauge: ObservableGauge<u64>,

    pub requests_counter: Counter<u64>,
    pub requests_histogram: Histogram<f64>,
    pub active_requests_counter: ActiveCounter,
    pub active_requests_gauge: ObservableGauge<u64>,

    pub storage_counter: Counter<u64>,
    pub storage_histogram: Histogram<f64>,

    pub filter_histogram: Histogram<f64>,
    pub filter_error_counter: Counter<u64>,

    pub handler_error_counter: Counter<u64>,

    pub active_tasks_counter: ActiveCounter,
    pub active_tasks_gauge: ObservableGauge<u64>,
    pub task_histogram: Histogram<f64>,
    pub task_error_counter: Counter<u64>,

    pub lua_used_memory: Arc<RwLock<Vec<AtomicU64>>>,
    pub lua_used_memory_gauge: ObservableGauge<u64>,

    pub num_threads_gauge: ObservableGauge<u64>,

    //
    // User-defined metrics
    //
    pub counters: Mutex<HashMap<String, Counter<u64>>>,
}

impl OpenTelemetryMetrics {
    pub fn new() -> Self {
        let meter = global::meter("casper");

        meter
            .register_callback(|cx| {
                let metrics = &*METRICS;

                let active_connections = metrics.active_connections_counter.get();
                metrics
                    .active_connections_gauge
                    .observe(cx, active_connections, &[]);

                let active_requests = metrics.active_requests_counter.get();
                metrics
                    .active_requests_gauge
                    .observe(cx, active_requests, &[]);

                if let Ok(lua_used_memory) = metrics.lua_used_memory.try_read() {
                    let lua_used_memory_total = lua_used_memory
                        .iter()
                        .map(|v| v.load(Ordering::Relaxed))
                        .sum();
                    metrics
                        .lua_used_memory_gauge
                        .observe(cx, lua_used_memory_total, &[]);
                }

                let active_tasks = metrics.active_tasks_counter.get();
                metrics.active_tasks_gauge.observe(cx, active_tasks, &[]);

                if let Some(n) = num_threads::num_threads() {
                    metrics.num_threads_gauge.observe(cx, n.get() as u64, &[]);
                }
            })
            .expect("Failed to register callback");

        OpenTelemetryMetrics {
            connections_counter: meter
                .u64_counter("http_connections")
                .with_description("Total number of HTTP connections processed by the application.")
                .init(),
            active_connections_counter: ActiveCounter::new(0),
            active_connections_gauge: meter
                .u64_observable_gauge("http_connections_current")
                .with_description(
                    "Current number of HTTP connections being processed by the application.",
                )
                .init(),

            requests_counter: meter
                .u64_counter("http_requests")
                .with_description("Total number of HTTP requests processed by the application.")
                .init(),
            requests_histogram: meter
                .f64_histogram("http_request_duration_seconds")
                .with_description("HTTP request latency in seconds.")
                .init(),
            active_requests_counter: ActiveCounter::new(0),
            active_requests_gauge: meter
                .u64_observable_gauge("http_requests_current")
                .with_description(
                    "Current number of HTTP requests being processed by the application.",
                )
                .init(),

            storage_counter: meter
                .u64_counter("storage_requests")
                .with_description(
                    "Total number of requests being processed by the storage backend.",
                )
                .init(),
            storage_histogram: meter
                .f64_histogram("storage_request_duration_seconds")
                .with_description("The storage backend request latency in seconds.")
                .init(),

            filter_histogram: meter
                .f64_histogram("filter_request_duration_seconds")
                .with_description("Filter only request latency in seconds.")
                .init(),
            filter_error_counter: meter
                .u64_counter("filter_errors")
                .with_description("Total number of errors thrown by filter.")
                .init(),

            handler_error_counter: meter
                .u64_counter("handler_errors")
                .with_description("Total number of errors thrown by handler.")
                .init(),

            active_tasks_counter: ActiveCounter::new(0),
            active_tasks_gauge: meter
                .u64_observable_gauge("tasks_current")
                .with_description("Current number of Lua tasks running by the application.")
                .init(),
            task_histogram: meter
                .f64_histogram("task_duration_seconds")
                .with_description("Task running duration in seconds.")
                .init(),
            task_error_counter: meter
                .u64_counter("task_errors")
                .with_description("Total number of errors thrown by task.")
                .init(),

            lua_used_memory: Arc::new(RwLock::default()),
            lua_used_memory_gauge: meter
                .u64_observable_gauge("lua_used_memory_bytes")
                .with_description("Total memory used by Lua workers.")
                .init(),

            num_threads_gauge: meter
                .u64_observable_gauge("process_threads_count")
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

macro_rules! connections_counter_inc {
    () => {{
        let cx = ::opentelemetry::Context::current();
        crate::metrics::METRICS.connections_counter.add(&cx, 1, &[]);
        crate::metrics::METRICS.active_connections_counter.inc()
    }};
}

macro_rules! active_request_guard {
    () => {
        crate::metrics::METRICS.active_requests_counter.inc()
    };
}

macro_rules! requests_counter_inc {
    ($attrs_map:expr) => {{
        let cx = ::opentelemetry::Context::current();
        let attrs = $attrs_map
            .iter()
            .map(|(key, value)| ::opentelemetry::KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        crate::metrics::METRICS.requests_counter.add(&cx, 1, &attrs);
    }};
}

macro_rules! requests_histogram_rec {
    ($start:expr, $attrs_map:expr) => {{
        let cx = ::opentelemetry::Context::current();
        let attrs = $attrs_map
            .iter()
            .map(|(key, value)| ::opentelemetry::KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        crate::metrics::METRICS.requests_histogram.record(
            &cx,
            $start.elapsed().as_secs_f64(),
            &attrs,
        );
    }};
}

macro_rules! storage_counter_add {
    ($increment:expr, $($key:expr => $val:expr),*) => {{
        let cx = ::opentelemetry::Context::current();
        crate::metrics::METRICS.storage_counter.add(&cx,
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! storage_histogram_rec {
    ($start:expr, $($key:expr => $val:expr),*) => {{
        let cx = ::opentelemetry::Context::current();
        crate::metrics::METRICS.storage_histogram.record(&cx,
            $start.elapsed().as_secs_f64(),
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! filter_histogram_rec {
    ($start:expr, $($key:expr => $val:expr),*) => {{
        let cx = ::opentelemetry::Context::current();
        crate::metrics::METRICS.filter_histogram.record(&cx,
            $start.elapsed().as_secs_f64(),
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! filter_error_counter_add {
    ($increment:expr, $($key:expr => $val:expr),*) => {{
        let cx = ::opentelemetry::Context::current();
        crate::metrics::METRICS.filter_error_counter.add(&cx,
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! handler_error_counter_add {
    ($increment:expr) => {
        handler_error_counter_add!($increment,)
    };
    ($increment:expr, $($key:expr => $val:expr),*) => {{
        let cx = ::opentelemetry::Context::current();
        crate::metrics::METRICS.handler_error_counter.add(&cx,
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! tasks_counter_inc {
    () => {
        crate::metrics::METRICS.active_tasks_counter.inc()
    };
}

macro_rules! task_histogram_rec {
    ($start:expr) => {
        task_histogram_rec!($start,)
    };
    ($start:expr, $($key:expr => $val:expr),*) => {{
        let cx = ::opentelemetry::Context::current();
        crate::metrics::METRICS.task_histogram.record(&cx,
            $start.elapsed().as_secs_f64(),
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! task_error_counter_add {
    ($increment:expr) => {
        task_error_counter_add!($increment,)
    };
    ($increment:expr, $($key:expr => $val:expr),*) => {{
        let cx = ::opentelemetry::Context::current();
        crate::metrics::METRICS.task_error_counter.add(&cx,
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
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

#[derive(Debug, Default, Clone)]
pub struct ActiveCounter(Arc<AtomicU64>);

#[derive(Debug)]
pub struct ActiveCounterGuard(Arc<AtomicU64>, u64);

impl ActiveCounter {
    pub fn new(v: u64) -> Self {
        ActiveCounter(Arc::new(AtomicU64::new(v)))
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn inc(&self) -> ActiveCounterGuard {
        self.0.fetch_add(1, Ordering::Relaxed);
        ActiveCounterGuard(Arc::clone(&self.0), 1)
    }
}

impl Drop for ActiveCounterGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::Relaxed);
    }
}

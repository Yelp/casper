use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, MeterProvider as _};
use opentelemetry_sdk::metrics::{self, SdkMeterProvider};
use tokio::sync::RwLock;

use crate::config::Config;

static METRICS: OnceLock<OpenTelemetryMetrics> = OnceLock::new();

// Histogram boundaries
static BOUNDARIES: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

pub fn init(config: &Config) {
    METRICS
        .set(OpenTelemetryMetrics::new(config))
        .expect("failed to init metrics");
}

#[inline]
pub fn global() -> &'static OpenTelemetryMetrics {
    #[cfg(test)]
    if METRICS.get().is_none() {
        init(&Config::default());
    }

    METRICS.get().unwrap()
}

#[derive(Debug)]
pub struct OpenTelemetryMetrics {
    pub connections_counter: Counter<u64>,
    pub active_connections_counter: ActiveCounter,

    pub requests_counter: Counter<u64>,
    pub requests_histogram: Histogram<f64>,
    pub active_requests_counter: ActiveCounter,

    pub storage_counter: Counter<u64>,
    pub storage_histogram: Histogram<f64>,

    pub filter_histogram: Histogram<f64>,
    pub filter_error_counter: Counter<u64>,

    pub handler_error_counter: Counter<u64>,

    pub active_tasks_counter: ActiveCounter,
    pub task_histogram: Histogram<f64>,
    pub task_error_counter: Counter<u64>,

    pub lua_used_memory: Arc<RwLock<Vec<AtomicU64>>>,

    // Common labels
    pub extra_labels: HashMap<String, String>,

    // User-defined metrics
    pub counters: HashMap<String, Counter<u64>>,
}

impl OpenTelemetryMetrics {
    fn new(config: &Config) -> Self {
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(prometheus::default_registry().clone())
            .without_target_info()
            .without_scope_info()
            .build()
            .expect("failed to create prometheus exporter");

        let provider = SdkMeterProvider::builder()
            .with_reader(exporter)
            .with_view(
                metrics::new_view(
                    {
                        let mut instrument = metrics::Instrument::new();
                        instrument.kind = Some(metrics::InstrumentKind::Histogram);
                        instrument
                    },
                    metrics::Stream::new().aggregation(
                        metrics::Aggregation::ExplicitBucketHistogram {
                            boundaries: BOUNDARIES.to_vec(),
                            record_min_max: true,
                        },
                    ),
                )
                .expect("failed to create histogram view"),
            )
            .build();

        let meter = provider.meter("casper");
        global::set_meter_provider(provider);

        let active_connections_counter = {
            let counter = ActiveCounter::new(0);
            let counter2 = counter.clone();
            meter
                .u64_observable_gauge("http_connections_current")
                .with_description(
                    "Current number of HTTP connections being processed by the application.",
                )
                .with_callback(move |instr| {
                    instr.observe(counter2.get(), &[]);
                })
                .init();
            counter
        };

        let active_requests_counter = {
            let counter = ActiveCounter::new(0);
            let counter2 = counter.clone();
            meter
                .u64_observable_gauge("http_requests_current")
                .with_description(
                    "Current number of HTTP requests being processed by the application.",
                )
                .with_callback(move |instr| {
                    instr.observe(counter2.get(), &[]);
                })
                .init();
            counter
        };

        let active_tasks_counter = {
            let counter = ActiveCounter::new(0);
            let counter2 = counter.clone();
            meter
                .u64_observable_gauge("tasks_current")
                .with_description("Current number of Lua tasks running by the application.")
                .with_callback(move |instr| {
                    instr.observe(counter2.get(), &[]);
                })
                .init();
            counter
        };

        let lua_used_memory = {
            let used_memory: Arc<RwLock<Vec<AtomicU64>>> = Arc::new(RwLock::default());
            let used_memory2 = used_memory.clone();
            meter
                .u64_observable_gauge("lua_used_memory_bytes")
                .with_description("Total memory used by Lua workers.")
                .with_callback(move |instr| {
                    if let Ok(used_memory) = used_memory2.try_read() {
                        let used_memory_total =
                            used_memory.iter().map(|v| v.load(Ordering::Relaxed)).sum();
                        instr.observe(used_memory_total, &[]);
                    }
                })
                .init();
            used_memory
        };

        let _num_threads_gauge = meter
            .u64_observable_gauge("process_threads_count")
            .with_description("Current number of active threads.")
            .with_callback(|instr| {
                if let Some(n) = num_threads::num_threads() {
                    instr.observe(n.get() as u64, &[]);
                }
            })
            .init();

        // Common (prometheus) labels for all metrics
        let mut extra_labels = config
            .metrics
            .as_ref()
            .and_then(|conf| conf.extra_labels.as_ref())
            .cloned()
            .unwrap_or_default();
        if let Some(service_name) = &config.main.service_name {
            extra_labels.insert("service_name".to_string(), service_name.clone());
        }

        // Init user-defined metrics
        let mut counters = HashMap::new();
        let counters_conf = config
            .metrics
            .as_ref()
            .and_then(|conf| conf.counters.as_ref());
        if let Some(counters_conf) = counters_conf {
            for (key, conf) in counters_conf.clone() {
                // If name already ends with `_total`, strip it out as the suffix is added automatically
                let mut name = conf.name.unwrap_or_else(|| key.clone());
                if name.ends_with("_total") {
                    name = name[..name.len() - 6].to_string();
                }
                let mut counter = meter.u64_counter(name);
                if let Some(description) = conf.description {
                    counter = counter.with_description(description);
                }
                counters.insert(key, counter.init());
            }
        }

        OpenTelemetryMetrics {
            connections_counter: meter
                .u64_counter("http_connections")
                .with_description("Total number of HTTP connections processed by the application.")
                .init(),
            active_connections_counter,

            requests_counter: meter
                .u64_counter("http_requests")
                .with_description("Total number of HTTP requests processed by the application.")
                .init(),
            requests_histogram: meter
                .f64_histogram("http_request_duration_seconds")
                .with_description("HTTP request latency in seconds.")
                .init(),
            active_requests_counter,

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

            active_tasks_counter,
            task_histogram: meter
                .f64_histogram("task_duration_seconds")
                .with_description("Task running duration in seconds.")
                .init(),
            task_error_counter: meter
                .u64_counter("task_errors")
                .with_description("Total number of errors thrown by task.")
                .init(),

            lua_used_memory,

            // Extra labels will be attached to every metric
            extra_labels,

            counters,
        }
    }
}

macro_rules! connections_counter_inc {
    () => {{
        crate::metrics::global().connections_counter.add(1, &[]);
        crate::metrics::global().active_connections_counter.inc()
    }};
}

macro_rules! active_request_guard {
    () => {
        crate::metrics::global().active_requests_counter.inc()
    };
}

macro_rules! requests_counter_inc {
    ($attrs_map:expr) => {{
        let attrs = $attrs_map
            .iter()
            .map(|(key, value)| ::opentelemetry::KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        crate::metrics::global().requests_counter.add(1, &attrs);
    }};
}

macro_rules! requests_histogram_rec {
    ($start:expr, $attrs_map:expr) => {{
        let attrs = $attrs_map
            .iter()
            .map(|(key, value)| ::opentelemetry::KeyValue {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        crate::metrics::global()
            .requests_histogram
            .record($start.elapsed().as_secs_f64(), &attrs);
    }};
}

macro_rules! storage_counter_add {
    ($increment:expr, $($key:expr => $val:expr),*) => {{
        crate::metrics::global().storage_counter.add(
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! storage_histogram_rec {
    ($start:expr, $($key:expr => $val:expr),*) => {{
        crate::metrics::global().storage_histogram.record(
            $start.elapsed().as_secs_f64(),
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! filter_histogram_rec {
    ($start:expr, $($key:expr => $val:expr),*) => {{
        crate::metrics::global().filter_histogram.record(
            $start.elapsed().as_secs_f64(),
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! filter_error_counter_add {
    ($increment:expr, $($key:expr => $val:expr),*) => {{
        crate::metrics::global().filter_error_counter.add(
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
        crate::metrics::global().handler_error_counter.add(
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! tasks_counter_inc {
    () => {
        crate::metrics::global().active_tasks_counter.inc()
    };
}

macro_rules! tasks_counter_get {
    () => {
        crate::metrics::global().active_tasks_counter.get()
    };
}

macro_rules! task_histogram_rec {
    ($start:expr) => {
        task_histogram_rec!($start,)
    };
    ($start:expr, $($key:expr => $val:expr),*) => {{
        crate::metrics::global().task_histogram.record(
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
        crate::metrics::global().task_error_counter.add(
            $increment,
            &[
                $(::opentelemetry::KeyValue::new($key, $val),)*
            ],
        )
    }};
}

macro_rules! lua_used_memory_update {
    ($id:expr, $value:expr) => {{
        let lua_used_memory = crate::metrics::global().lua_used_memory.read().await;
        if $id < lua_used_memory.len() {
            lua_used_memory[$id].store($value as u64, ::std::sync::atomic::Ordering::Relaxed);
        } else {
            drop(lua_used_memory);
            let mut lua_used_memory = crate::metrics::global().lua_used_memory.write().await;
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

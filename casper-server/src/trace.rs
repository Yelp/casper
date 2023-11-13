use std::time::Duration;

use opentelemetry_http::hyper::HyperClient;
use opentelemetry_sdk::trace::{self, RandomIdGenerator, Sampler};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::EnvFilter;

use crate::config::Config;

pub fn init(config: &Config) {
    init_opentelemetry(config);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

fn init_opentelemetry(config: &Config) {
    let tracing_conf = match config.tracing {
        Some(ref tracing_conf) if tracing_conf.enabled => tracing_conf,
        _ => return,
    };

    opentelemetry::global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());

    let sampler = if tracing_conf.mode.as_deref() == Some("firehose") {
        // In "firehose" mode we always sample but propagate the original sampling decision.
        Sampler::AlwaysOn
    } else {
        Sampler::ParentBased(Box::new(Sampler::AlwaysOn))
    };

    let mut pipeline_builder = opentelemetry_zipkin::new_pipeline()
        .with_http_client(HyperClient::new_with_timeout(
            hyper::client::Client::new(),
            Duration::from_secs(5),
        ))
        .with_trace_config(
            trace::config()
                .with_sampler(sampler)
                .with_id_generator(RandomIdGenerator::default()),
        );

    if let Some(service_name) = &config.main.service_name {
        pipeline_builder = pipeline_builder.with_service_name(service_name);
    }

    if let Some(collector_endpoint) = &tracing_conf.collector_endpoint {
        pipeline_builder = pipeline_builder.with_collector_endpoint(collector_endpoint);
    }

    pipeline_builder
        .install_batch(opentelemetry_sdk::runtime::TokioCurrentThread)
        .expect("failed to create zipkin tracer");
}

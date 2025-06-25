use std::fmt;
use std::str::FromStr;

use bytes::Bytes;
use ntex::rt::Arbiter;
use ntex::time::Millis;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use opentelemetry_http::{HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::runtime::TokioCurrentThread;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;

pub fn init(config: &Config) -> Option<SdkTracerProvider> {
    let tracing_conf = match config.tracing {
        Some(ref tracing_conf) if tracing_conf.enabled => tracing_conf,
        _ => return None,
    };

    // We support multiple trace context propagators
    let mut propagators: Vec<Box<dyn TextMapPropagator + Send + Sync>> = Vec::new();
    for prop in tracing_conf.propagators.as_deref().unwrap_or_default() {
        match prop.as_str() {
            "zipkin" => propagators.push(Box::new(opentelemetry_zipkin::Propagator::new())),
            "w3c" => propagators.push(Box::new(TraceContextPropagator::new())),
            _ => tracing::warn!("Unknown propagator: {prop}"),
        }
    }
    opentelemetry::global::set_text_map_propagator(TextMapCompositePropagator::new(propagators));

    let mut exporter_builder = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_http_client(spawn_http_client());

    if let Some(collector_endpoint) = &tracing_conf.collector_endpoint {
        exporter_builder = exporter_builder.with_endpoint(collector_endpoint);
    }

    let exporter = exporter_builder
        .build()
        .expect("Failed to create opentelemetry exporter");

    let span_processor = BatchSpanProcessor::builder(exporter, TokioCurrentThread).build();

    let sampler = match tracing_conf.sampler.as_deref() {
        Some("AlwaysOn") => Sampler::AlwaysOn,
        Some("AlwaysOff") => Sampler::AlwaysOff,
        // This is a trick to enable sampling, but propagate the "sampling disabled" flag
        Some("SilentOn") => Sampler::AlwaysOn,
        _ => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
    };

    let mut provider_builder = SdkTracerProvider::builder()
        .with_span_processor(span_processor)
        .with_id_generator(RandomIdGenerator::default())
        .with_sampler(sampler);

    if let Some(service_name) = &config.main.service_name {
        provider_builder = provider_builder.with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_service_name(service_name.clone())
                .build(),
        );
    }

    let provider = provider_builder.build();
    opentelemetry::global::set_tracer_provider(provider.clone());

    Some(provider)
}

type BatchHttpClientRequest = (
    Request<Bytes>,
    oneshot::Sender<Result<Response<Bytes>, HttpError>>,
);

// Http client to send batches using ntex client running on a dedicated thread
struct BatchHttpClient(mpsc::Sender<BatchHttpClientRequest>);

impl fmt::Debug for BatchHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchHttpClient").finish()
    }
}

#[async_trait::async_trait]
impl HttpClient for BatchHttpClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (tx, rx) = oneshot::channel();
        self.0.send((request, tx)).await?;
        rx.await?
    }
}

fn spawn_http_client() -> BatchHttpClient {
    let (tx, mut rx) = mpsc::channel::<BatchHttpClientRequest>(100);

    Arbiter::new().exec_fn(move || {
        ntex::rt::spawn(async move {
            let client = ntex::http::client::Client::build()
                .disable_redirects()
                .timeout(Millis(5000)) // TODO: configure
                .finish();

            while let Some((inner_req, sender)) = rx.recv().await {
                let method = inner_req.method().as_str();
                let url = inner_req.uri().to_string();
                let mut req = client.request(ntex::http::Method::from_str(method).unwrap(), url);
                // Copy headers
                for (key, value) in inner_req.headers() {
                    req.headers_mut().append(
                        key.as_str().try_into().unwrap(),
                        value.as_bytes().try_into().unwrap(),
                    );
                }
                // Send
                match req.send_body(inner_req.into_body().to_vec()).await {
                    Ok(mut inner_resp) if inner_resp.status().is_success() => {
                        let status = inner_resp.status().as_u16();
                        let body = inner_resp.body().await.unwrap_or_default().to_vec();

                        let mut response = Response::<Bytes>::new(body.into());
                        *response.status_mut() = status.try_into().unwrap();
                        // Copy headers
                        for (name, value) in inner_resp.headers() {
                            let name = http::HeaderName::from_str(name.as_str()).unwrap();
                            let value = value.as_bytes().try_into().unwrap();
                            response.headers_mut().append(name, value);
                        }
                        let _ = sender.send(Ok(response));
                    }
                    Ok(inner_resp) => {
                        let err = format!("request failed with status: {}", inner_resp.status());
                        let _ = sender.send(Err(err.into()));
                    }
                    Err(err) => {
                        let _ = sender.send(Err(err.to_string().into()));
                    }
                }
            }
        });
    });

    BatchHttpClient(tx)
}

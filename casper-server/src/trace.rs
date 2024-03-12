use std::fmt;
use std::str::FromStr;

use bytes::Bytes;
use ntex::time::Millis;
use opentelemetry_http::{HttpClient, HttpError, Request, Response};
use opentelemetry_sdk::trace::{self, RandomIdGenerator, Sampler};
use tokio::runtime;
use tokio::sync::{mpsc, oneshot};
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
        .with_http_client(spawn_http_client())
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

type BatchHttpClientRequest = (
    Request<Vec<u8>>,
    oneshot::Sender<Result<Response<Bytes>, HttpError>>,
);

// Http client to send batches based on ntex with usage of dedicated thread
struct BatchHttpClient(mpsc::Sender<BatchHttpClientRequest>);

impl fmt::Debug for BatchHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtelHttpClient").finish()
    }
}

#[async_trait::async_trait]
impl HttpClient for BatchHttpClient {
    async fn send(&self, request: Request<Vec<u8>>) -> Result<Response<Bytes>, HttpError> {
        let (tx, rx) = oneshot::channel();
        self.0.send((request, tx)).await?;
        rx.await?
    }
}

fn spawn_http_client() -> BatchHttpClient {
    let (tx, mut rx) = mpsc::channel::<BatchHttpClientRequest>(100);

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");

    // Spawn dedicated thread to send tracing batches
    std::thread::spawn(move || {
        runtime.block_on(async move {
            let client = ntex::http::client::Client::build()
                .disable_redirects()
                .timeout(Millis(5000))
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
                match req.send_body(inner_req.into_body()).await {
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

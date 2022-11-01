use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use clap::Parser;
use futures::{Stream, TryStreamExt};
use hyper::client::Client as HttpClient;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream, Http};
use hyper_tls::HttpsConnector;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::ServiceBuilderExt;
use tracing::{error, info};
use tracing_log::LogTracer;

use crate::error::ErrorLayer;
use crate::log::LogLayer;
use crate::metrics::MetricsLayer;
use crate::service::Svc;
use crate::storage::Storage;
use crate::types::{RemoteAddr, SimpleHttpClient};
use crate::worker::{WorkerContext, WorkerPoolHandle};

#[macro_use]
mod metrics;

struct AddrIncomingStream(AddrIncoming);

impl Stream for AddrIncomingStream {
    type Item = std::io::Result<AddrStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_accept(cx)
    }
}

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value = "casper.lua", env = "CASPER_CONFIG")]
    config: String,
}

async fn main_inner() -> anyhow::Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Read application configuration
    let config = Arc::new(config::read_config(&args.config)?);

    // Init Metrics subsystem
    crate::metrics::init(&config.main);

    // Register metrics defined in the config
    if let Some(metrics) = config.metrics.clone() {
        metrics::register_custom_metrics(metrics);
    }

    // Construct HTTP client shared between workers
    let http_client = SimpleHttpClient::from({
        let connector = HttpsConnector::new();
        HttpClient::builder()
            .http1_preserve_header_case(true)
            .build(connector)
    });

    // Construct storage backends defined in the config
    let mut storage_backends = Vec::new();
    for (name, conf) in config.storage.clone() {
        let backend = storage::Backend::new(name.clone(), conf)?;
        if let Err(err) = backend.connect().await {
            // Not critical error
            error!("Failed to establish connection with storage '{name}': {err:?}");
        }
        storage_backends.push(backend);
    }

    let config2 = Arc::clone(&config);
    let worker_pool = WorkerPoolHandle::build()
        .pool_size(config.main.workers)
        .on_worker_init(move |handle| {
            let id = handle.id();
            let config = Arc::clone(&config2);
            let http_client = http_client.clone();
            let storage_backends = storage_backends.clone();

            #[cfg(target_os = "linux")]
            if config.main.pin_workers {
                let cores = affinity::get_core_num();
                if let Err(err) = affinity::set_thread_affinity([id % cores]) {
                    error!("Failed to set worker thread affinity: {}", err);
                }
            }

            async move {
                let context = WorkerContext::builder()
                    .with_config(config)
                    .with_http_client(http_client.clone())
                    .with_storage_backends(storage_backends)
                    .build(handle)?;

                // Attach SimpleHttpClient to Lua
                context.lua.set_app_data::<SimpleHttpClient>(http_client);

                // Track Lua used memory every 10 seconds
                let lua = Rc::clone(&context.lua);
                tokio::task::spawn_local(async move {
                    loop {
                        lua_used_memory_update!(id, lua.used_memory());
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                });

                Ok(context)
            }
        })
        .build()?;
    info!("Created {} workers", config.main.workers);

    let addr = &config.main.listen;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to listen {addr}"))?;
    info!("Listening on http://{}", addr);

    let mut incoming = AddrIncomingStream(AddrIncoming::from_listener(listener)?);
    incoming.0.set_nodelay(true);

    while let Some(stream) = incoming.try_next().await? {
        let accept_time = Instant::now();
        worker_pool.spawn_pinned(move |worker_ctx| async move {
            // Time spent in a queue before processing
            let _queue_dur = accept_time.elapsed();
            let _conn_count_guard = connections_counter_inc!();

            let remote_addr = stream.remote_addr();
            let svc = Svc {
                worker_ctx: worker_ctx.clone(),
                remote_addr,
            };

            let service = ServiceBuilder::new()
                .add_extension(RemoteAddr(remote_addr))
                .layer(MetricsLayer::new("/metrics".to_string()))
                .layer(ErrorLayer)
                .layer(LogLayer::new(worker_ctx.clone()))
                .service(svc);

            let server = Http::new()
                .with_executor(worker_ctx)
                .http1_keep_alive(true)
                .serve_connection(stream, service);

            if let Err(err) = server.await {
                error!("{err:?}");
            }
        });
    }

    Ok(())
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .thread_name("casper-main")
        .enable_all()
        .build()
        .expect("Failed to build a tokio runtime");

    // Install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    // Convert log records to tracing Events
    let _ = LogTracer::init();

    if let Err(err) = runtime.block_on(main_inner()) {
        error!("{err:?}");
    }
}

mod config;
mod config_loader;
mod error;
mod handler;
mod http;
mod log;
mod lua;
mod service;
mod storage;
mod types;
mod utils;
mod worker;

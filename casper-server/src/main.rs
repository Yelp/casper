use std::net::TcpListener;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use actix_web::rt::System;
use actix_web::web;
use actix_web::{App, HttpServer};
use anyhow::Context as _;
use clap::Parser;
use isahc::config::{Configurable as _, DnsCache, RedirectPolicy};
use isahc::HttpClient;
use tracing::{error, info};
use tracing_log::LogTracer;

use crate::context::AppContext;
use crate::metrics::ActiveCounterGuard;
use crate::storage::Storage;

#[macro_use]
mod metrics;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value = "casper.lua", env = "CASPER_CONFIG")]
    config: String,
}

async fn main_inner(args: Args) -> anyhow::Result<()> {
    // Read application configuration
    let config = Arc::new(config::read_config(&args.config)?);

    // Init Metrics subsystem
    crate::metrics::init(&config.main);

    // Register metrics defined in the config
    if let Some(metrics) = config.metrics.clone() {
        metrics::register_custom_metrics(metrics);
    }

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

    // Construct default HTTP client
    let http_client = HttpClient::builder()
        .connection_cache_size(1000)
        .connection_cache_ttl(Duration::from_secs(60))
        .dns_cache(DnsCache::Disable)
        .automatic_decompression(false)
        .redirect_policy(RedirectPolicy::None)
        .tcp_nodelay()
        .build()?;

    // Try to initialize application context on the listening thread to check for errors
    let context = AppContext::builder()
        .with_config(config.clone())
        .with_storage_backends(storage_backends.clone())
        .build()?;
    // Drop it
    drop(context);

    let addr = &config.main.listen;
    let listener = TcpListener::bind(addr).with_context(|| format!("Failed to listen {addr}"))?;
    info!("Listening on http://{}", addr);

    let config2 = config.clone();
    HttpServer::new(move || {
        // Initialize per-worker thread application context
        let context = AppContext::builder()
            .with_config(config.clone())
            .with_storage_backends(storage_backends.clone())
            .build()
            .unwrap();
        let id = context.id;

        #[cfg(target_os = "linux")]
        if config.main.pin_workers {
            let cores = affinity::get_core_num();
            if let Err(err) = affinity::set_thread_affinity([id % cores]) {
                error!("Failed to set worker thread affinity: {}", err);
            }
        }

        // Attach default HTTP client to Lua
        context.lua.set_app_data(http_client.clone());

        // Track Lua used memory every 10 seconds
        let lua = Rc::clone(&context.lua);

        tokio::task::spawn_local(async move {
            loop {
                lua_used_memory_update!(id, lua.used_memory());
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });

        App::new()
            .app_data(web::Data::new(context))
            .wrap(middleware::Metrics::new("/metrics".to_string()))
            .wrap(middleware::Logger::new())
            // .wrap(actix_web::middleware::Logger::default())
            .default_service(web::to(handler::handler))
    })
    .on_connect(|_, ext| {
        // Count number of active connections
        struct ConnectionCountGuard(ActiveCounterGuard);
        ext.insert(ConnectionCountGuard(connections_counter_inc!()));
    })
    .listen(listener)?
    .keep_alive(Duration::from_secs(60))
    .workers(config2.main.workers)
    .run()
    .await?;

    Ok(())
}

fn main() {
    // Install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    // Convert log records to tracing Events
    let _ = LogTracer::init();

    // Parse command line arguments
    let args = Args::parse();

    let system = System::new();
    if let Err(err) = system.block_on(main_inner(args)) {
        error!("{err:#}");
    }
}

mod config;
mod config_loader;
mod context;
mod handler;
mod http;
mod lua;
mod middleware;
mod storage;
mod types;
mod utils;

use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use ntex::http::client::Client as HttpClient;
use ntex::http::HttpService;
use ntex::io::Io;
use ntex::rt::System;
use ntex::server::Server;
use ntex::service::{apply_fn_factory, Service};
use ntex::time::Seconds;
use ntex::util::PoolId;
use ntex::web::{self, App};
use tracing::error;
use tracing_log::LogTracer;

use crate::context::AppContext;
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

    // Try to initialize application context on the listening thread to check for errors
    let context = AppContext::builder()
        .with_config(config.clone())
        .with_storage_backends(storage_backends.clone())
        .build()?;
    // Drop it
    drop(context);

    let addr = config.main.listen.clone();
    let workers = config.main.workers;

    // Get available CPU cores
    let core_ids = Arc::new(Mutex::new(
        core_affinity::get_core_ids().unwrap_or_default(),
    ));

    Server::build()
        .bind("casper", &addr, move |conf| {
            conf.memory_pool(PoolId::P0);

            // Initialize per-worker thread application context
            let context = AppContext::builder()
                .with_config(config.clone())
                .with_storage_backends(storage_backends.clone())
                .build()
                .unwrap();
            let id = context.id;

            if config.main.pin_workers {
                if let Some(id) = core_ids.lock().unwrap().pop() {
                    core_affinity::set_for_current(id);
                }
            }

            // Construct default HTTP client and attach it to Lua
            let http_client = HttpClient::build()
                .disable_redirects()
                .disable_timeout()
                .finish();
            context.lua.set_app_data(http_client);

            // Track Lua used memory every 10 seconds
            let lua = Rc::clone(&context.lua);

            tokio::task::spawn_local(async move {
                loop {
                    lua_used_memory_update!(id, lua.used_memory());
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            });

            let app = App::new()
                .state(context)
                .wrap(middleware::Metrics::new("/metrics".to_string()))
                .wrap(middleware::Logger::new())
                // .wrap(ntex::web::middleware::Logger::default())
                .default_service(web::to(handler::handler));

            // TODO: AppConfig

            let service = HttpService::build()
                .keep_alive(30)
                .client_timeout(Seconds::new(5))
                .disconnect_timeout(Seconds::new(5))
                .finish(app);

            apply_fn_factory(service, |io: Io, handler| {
                // Count number of active connections
                let _conn_guard = connections_counter_inc!();
                handler.call(io)
            })
        })?
        .backlog(2048)
        .workers(workers)
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

    let system = System::new("casper");
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

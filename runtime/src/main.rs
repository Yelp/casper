use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use clap::Parser;
use futures::{Stream, TryStreamExt};
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::worker::LocalWorker;

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

    // Register storage backends defined in the config
    backends::register_backends(config.storage.clone()).await?;

    let main_config = &config.main;

    let mut workers = Vec::new();
    let num_worker_threads = main_config.worker_threads;
    for id in 0..num_worker_threads {
        let worker =
            LocalWorker::new(id, config.clone()).with_context(|| "Failed to initialize worker")?;
        workers.push(worker);
        info!("Worker {id} created");
    }

    let addr = &main_config.listen;
    let listener = TcpListener::bind(addr).await?;

    info!("Listening on http://{}", addr);

    let mut incoming = AddrIncomingStream(AddrIncoming::from_listener(listener)?);
    let mut accept_count = 0;
    while let Some(stream) = incoming.try_next().await? {
        workers[accept_count % num_worker_threads].spawn(stream);
        accept_count += 1;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    if let Err(err) = main_inner().await {
        error!("{:?}", err);
    }
}

mod backends;
mod config;
mod config_loader;
mod core;
mod handler;
mod regex;
mod request;
mod response;
mod service;
mod stats;
mod storage;
mod udp;
mod utils;
mod worker;

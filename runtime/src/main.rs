use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use futures::{Stream, TryStreamExt};
use hyper::{
    client::HttpConnector,
    server::conn::AddrIncoming,
    server::{accept::Accept, conn::AddrStream},
    Client,
};
use once_cell::sync::Lazy;
use tokio::net::TcpListener;

use crate::worker::LocalWorker;

pub static CLIENT: Lazy<Client<HttpConnector>> = Lazy::new(Client::new);

struct AddrIncomingStream(AddrIncoming);

impl Stream for AddrIncomingStream {
    type Item = std::io::Result<AddrStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_accept(cx)
    }
}

async fn main_inner() -> anyhow::Result<()> {
    pretty_env_logger::init();

    let config = Arc::new(config::read_config("./casper.toml")?);

    // Register storage backends defined in the config
    backends::register_backends(config.storage.clone())
        .context("cannot register storage backends")?;

    let main_config = &config.main;

    let mut workers = Vec::new();
    let num_threads = main_config.num_threads;
    for id in 0..num_threads {
        workers.push(LocalWorker::new(id, config.clone()));
    }

    let addr = &main_config.listen;
    let listener = TcpListener::bind(addr).await?;

    println!("Listening on http://{}", addr);

    let mut incoming = AddrIncomingStream(AddrIncoming::from_listener(listener)?);
    let mut accept_count = 0;
    while let Some(stream) = incoming.try_next().await? {
        accept_count += 1;
        workers[accept_count % num_threads].spawn(stream);
        accept_count += 1;
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(err) = main_inner().await {
        eprintln!("{:?}", err);
    }
}

mod backends;
mod config;
mod core;
mod handler;
mod request;
mod response;
mod service;
mod storage;
mod worker;

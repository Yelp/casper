use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let config = Arc::new(config::read_config("./casper.toml")?);

    // println!("{:?}", config);

    let mut workers = Vec::new();
    let num_threads = config.main.as_ref().map(|cfg| cfg.num_threads).unwrap_or(4);
    for _ in 0..num_threads {
        workers.push(LocalWorker::new(config.clone()));
    }

    let addr: SocketAddr = ([0, 0, 0, 0], 8888).into();
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = AddrIncomingStream(AddrIncoming::from_listener(listener)?);
    let mut accept_count = 0;
    while let Some(stream) = incoming.try_next().await? {
        accept_count += 1;
        workers[accept_count % num_threads].spawn(stream);
        accept_count += 1;
    }

    Ok(())
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

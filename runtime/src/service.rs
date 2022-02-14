use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper::{service::Service, Body, Request, Response};
use mlua::{Function, Lua, LuaSerdeExt, RegistryKey, Table};
use serde::Serialize;
use tracing::{error, instrument};

use crate::handler;
use crate::metrics::METRICS;
use crate::worker::WorkerData;

#[derive(Clone)]
pub struct Svc {
    pub lua: Rc<Lua>,
    pub worker_data: Rc<WorkerData>,
    pub remote_addr: SocketAddr,
}

#[derive(Default, Debug, Serialize)]
struct LogData {
    uri: String,
    method: String,
    remote_addr: String,
    elapsed: Duration,
    status: u16,
    active_conns: u64,
    active_requests: u64,
    worker_active_requests: u64,
    // TODO: accept date
}

impl Service<Request<Body>> for Svc {
    type Response = Response<Body>;
    type Error = Infallible;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        Box::pin(Self::handler(self.clone(), req))
    }
}

impl Svc {
    #[instrument(skip(self), fields(http.uri = %req.uri(), http.method = %req.method()))]
    async fn handler(self, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let start = Instant::now();

        let _req_cnt_worker = self.worker_data.active_requests.inc(1);

        // Create Lua context table
        let ctx = self
            .lua
            .create_table()
            .expect("Failed to create Lua context table");
        let ctx_key = self
            .lua
            .create_registry_value(ctx)
            .expect("Failed to store Lua context table in the registry");
        let ctx_key = Rc::new(ctx_key);

        // Save essential parts for logging
        let mut log_data = LogData {
            uri: req.uri().to_string(),
            method: req.method().to_string(),
            remote_addr: self.remote_addr.to_string(),
            ..Default::default()
        };

        let lua = self.lua.clone();
        let worker_data = self.worker_data.clone();
        let response = handler::handler(lua, worker_data, req, ctx_key.clone()).await;

        log_data.elapsed = start.elapsed();
        log_data.active_conns = METRICS.active_connections_counter.get();
        log_data.active_requests = METRICS.active_requests_counter.get();
        log_data.worker_active_requests = self.worker_data.active_requests.get();

        match response {
            Ok(res) => {
                log_data.status = res.status().as_u16();

                self.spawn_access_log(log_data, ctx_key);
                Ok(res)
            }
            Err(_err) => {
                // // Execute user-defined error log function
                // if data.error_log.is_some() {
                //     tokio::task::spawn_local(async move {
                //         let ctx = get_registry::<Table>(&lua, &ctx_key);
                //         let error_log_key = data.error_log.as_ref().unwrap();
                //         let error_logger = get_registry::<Function>(&lua, error_log_key);
                //         let err = format!("{:#}", err);
                //         if let Err(err) = error_logger.call_async::<_, Value>((err, ctx)).await {
                //             error!("{:#}", err);
                //         }
                //     });
                // } else {
                //     error!("{:?}", err);
                // }

                Ok(Response::builder()
                    .status(500)
                    .body(Body::from("Internal Server Error"))
                    .expect("Cannot build Response"))
            }
        }
    }

    /// Executes user-defined access log function
    fn spawn_access_log(&self, log_data: LogData, ctx_key: Rc<RegistryKey>) {
        if self.worker_data.access_log.is_some() {
            let lua = self.lua.clone();
            let worker_data = self.worker_data.clone();
            let log = async move {
                let access_log_key = worker_data.access_log.as_ref().unwrap(); // never fails
                let ctx = lua.registry_value::<Table>(&ctx_key)?;
                let log_data = lua.to_value(&log_data)?;
                let access_logger = lua.registry_value::<Function>(access_log_key)?;
                access_logger.call_async::<_, ()>((log_data, ctx)).await
            };
            tokio::task::spawn_local(async move {
                if let Err(err) = log.await {
                    error!("{:#}", err);
                }
            });
        }
    }
}

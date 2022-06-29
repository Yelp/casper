use std::fmt::Debug;
use std::mem;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::future::LocalBoxFuture;
use hyper::{Body, Request, Response};
use mlua::{Function, LuaSerdeExt, Table};
use serde::Serialize;
use tower::{Layer, Service};
use tracing::error;

use crate::metrics::METRICS;
use crate::types::{LuaContext, RemoteAddr};
use crate::worker::WorkerContext;

pub struct LogLayer {
    worker_context: WorkerContext,
}

impl LogLayer {
    pub fn new(worker_context: WorkerContext) -> Self {
        LogLayer { worker_context }
    }
}

impl<S> Layer<S> for LogLayer {
    type Service = LogService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LogService {
            service,
            worker_context: self.worker_context.clone(),
        }
    }
}

#[derive(Default, Debug, Serialize)]
struct LogContext {
    uri: String,
    method: String,
    remote_addr: String,
    elapsed: Duration,
    status: u16,
    active_conns: u64,
    active_requests: u64,
    // TODO: accept date
}

// This service implements the Log behavior
#[derive(Clone, Debug)]
pub struct LogService<S> {
    service: S,
    worker_context: WorkerContext,
}

impl<S> LogService<S> {
    /// Executes user-defined access log function
    fn spawn_access_log(worker_ctx: WorkerContext, log_ctx: LogContext, lua_ctx: LuaContext) {
        let worker_ctx2 = worker_ctx.clone();
        if worker_ctx.access_log.is_some() {
            let log = async move {
                let lua = &worker_ctx.lua;
                let access_log_key = worker_ctx.access_log.as_ref().unwrap(); // never fails
                let lua_ctx = lua.registry_value::<Table>(&lua_ctx.0)?;
                let log_ctx = lua.to_value(&log_ctx)?;
                let access_logger = lua.registry_value::<Function>(access_log_key)?;

                access_logger.call_async::<_, ()>((log_ctx, lua_ctx)).await
            };
            worker_ctx2.spawn_local(async move {
                if let Err(err) = log.await {
                    error!("{:#}", err);
                }
            });
        }
    }
}

impl<S> Service<Request<Body>> for LogService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + 'static,
    S::Error: Debug,
    S::Future: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let start = Instant::now();

        let remote_addr = req.extensions().get::<RemoteAddr>().copied();
        let remote_addr = remote_addr.unwrap_or_default();

        let mut log_context = LogContext {
            uri: req.uri().to_string(),
            method: req.method().to_string(),
            remote_addr: remote_addr.0.to_string(),
            ..Default::default()
        };
        let worker_context = self.worker_context.clone();

        let clone = self.service.clone();
        let mut service = mem::replace(&mut self.service, clone);

        Box::pin(async move {
            let mut resp = service.call(req).await;

            log_context.elapsed = start.elapsed();
            log_context.active_conns = METRICS.active_connections_counter.get();
            log_context.active_requests = METRICS.active_requests_counter.get();

            match resp.as_mut() {
                Ok(resp) => {
                    log_context.status = resp.status().as_u16();

                    let lua_context = resp
                        .extensions()
                        .get::<LuaContext>()
                        .cloned()
                        .expect("Cannot find response context");

                    Self::spawn_access_log(worker_context, log_context, lua_context);
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
                    //     error!("{err:?}");
                    // }
                }
            }

            resp
        })
    }
}

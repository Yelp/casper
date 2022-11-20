use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use actix_web::body::{BodySize, MessageBody};
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use bytes::Bytes;
use futures::future::Ready;
use mlua::{Function, LuaSerdeExt};
use pin_project_lite::pin_project;
use serde::Serialize;
use tracing::{debug, error};

use crate::context::AppContext;
use crate::metrics::METRICS;
use crate::types::LuaContext;

#[derive(Debug)]
pub struct Logger {
    app_context: AppContext,
}

impl Logger {
    pub fn new(app_context: AppContext) -> Self {
        Logger { app_context }
    }
}

impl<S, B> Transform<S, ServiceRequest> for Logger
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    B: MessageBody,
{
    type Response = ServiceResponse<StreamLog<B>>;
    type Error = Error;
    type Transform = LoggerMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        futures::future::ready(Ok(LoggerMiddleware {
            service,
            app_context: self.app_context.clone(),
        }))
    }
}

#[derive(Default, Debug, Serialize)]
struct LogData {
    // TODO: start time
    uri: String,
    method: String,
    remote_addr: Option<String>,
    elapsed: Duration,
    status: u16,
    active_conns: u64,
    active_requests: u64,
}

/// Logger middleware service.
#[derive(Clone, Debug)]
pub struct LoggerMiddleware<S> {
    app_context: AppContext,
    service: S,
}

impl LoggerMiddleware<()> {
    /// Executes user-defined access log function
    fn spawn_access_log(app_ctx: AppContext, log_data: LogData, lua_ctx: LuaContext) {
        let log = async move {
            let lua = &app_ctx.lua;
            let log_data = lua.to_value(&log_data);
            let access_log_key = app_ctx.access_log.as_ref().unwrap(); // never fails
            let access_logger = lua.registry_value::<Function>(access_log_key)?;
            let lua_ctx = lua_ctx.get(lua);

            access_logger
                .call_async::<_, ()>((log_data?, lua_ctx))
                .await
        };

        actix_web::rt::spawn(async move {
            if let Err(err) = log.await {
                error!("{err:#}");
            }
        });
    }
}

impl<S, B> Service<ServiceRequest> for LoggerMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    B: MessageBody,
{
    type Response = ServiceResponse<StreamLog<B>>;
    type Error = Error;
    type Future = LoggerResponse<S, B>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let start = Instant::now();

        let log_data = LogData {
            uri: req.uri().to_string(),
            method: req.method().to_string(),
            remote_addr: req.peer_addr().map(|addr| addr.to_string()),
            ..Default::default()
        };

        let fut = self.service.call(req);

        LoggerResponse {
            fut,
            start,
            app_context: self.app_context.clone(),
            lua_context: None,
            log_data: Some(log_data),
            _phantom: PhantomData,
        }

        // Box::pin(async move {
        //     let mut resp = service.call(req).await;

        //     log_context.elapsed = start.elapsed();
        //     log_context.active_conns = METRICS.active_connections_counter.get();
        //     log_context.active_requests = METRICS.active_requests_counter.get();

        //     match resp.as_mut() {
        //         Ok(resp) => {
        //             log_context.status = resp.status().as_u16();

        //             let lua_context = resp
        //                 .extensions()
        //                 .get::<LuaContext>()
        //                 .cloned()
        //                 .expect("Cannot find response context");

        //             Self::spawn_access_log(worker_context, log_context, lua_context);
        //         }
        //         Err(_err) => {
        //             // // Execute user-defined error log function
        //             // if data.error_log.is_some() {
        //             //     tokio::task::spawn_local(async move {
        //             //         let ctx = get_registry::<Table>(&lua, &ctx_key);
        //             //         let error_log_key = data.error_log.as_ref().unwrap();
        //             //         let error_logger = get_registry::<Function>(&lua, error_log_key);
        //             //         let err = format!("{:#}", err);
        //             //         if let Err(err) = error_logger.call_async::<_, Value>((err, ctx)).await {
        //             //             error!("{:#}", err);
        //             //         }
        //             //     });
        //             // } else {
        //             //     error!("{err:?}");
        //             // }
        //         }
        //     }

        //     resp
        // })
    }
}

pin_project! {
    pub struct LoggerResponse<S, B>
    where
        S: Service<ServiceRequest>,
        B: MessageBody,
    {
        #[pin]
        fut: S::Future,
        start: Instant,
        app_context: AppContext,
        lua_context: Option<LuaContext>,
        log_data: Option<LogData>,
        _phantom: PhantomData<B>,
    }
}

impl<S, B> Future for LoggerResponse<S, B>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    B: MessageBody,
{
    type Output = Result<ServiceResponse<StreamLog<B>>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // TODO: Execute error log

        let res = match futures::ready!(this.fut.poll(cx)) {
            Ok(res) => res,
            Err(e) => return Poll::Ready(Err(e)),
        };

        if let Some(log_data) = this.log_data.as_mut() {
            log_data.elapsed = this.start.elapsed();
            log_data.active_conns = METRICS.active_connections_counter.get();
            log_data.active_requests = METRICS.active_requests_counter.get();

            // Collect response fields
            log_data.status = res.status().as_u16();

            *this.lua_context = res.response().extensions().get::<LuaContext>().cloned();
        }

        if let Some(error) = res.response().error() {
            debug!("Error in response: {:?}", error);
        }

        Poll::Ready(Ok(res.map_body(move |_, body| StreamLog {
            body,
            body_size: 0,
            start: *this.start,
            app_context: this.app_context.clone(),
            lua_context: this.lua_context.take(),
            log_data: this.log_data.take(),
        })))
    }
}

pin_project! {
    /// Used to calculate final body size and spawn logging coroutine
    pub struct StreamLog<B> {
        #[pin]
        body: B,
        body_size: usize,
        start: Instant,
        app_context: AppContext,
        lua_context: Option<LuaContext>,
        log_data: Option<LogData>,
    }

    // This is where we execute log action, after streaming body
    impl<B> PinnedDrop for StreamLog<B> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let (Some(log_data), Some(lua_ctx)) = (this.log_data.take(), this.lua_context.take()) {
                LoggerMiddleware::spawn_access_log(this.app_context.clone(), log_data, lua_ctx)
            }
        }
    }
}

impl<B: MessageBody> MessageBody for StreamLog<B> {
    type Error = B::Error;

    #[inline]
    fn size(&self) -> BodySize {
        self.body.size()
    }

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        let this = self.project();

        match futures::ready!(this.body.poll_next(cx)) {
            Some(Ok(chunk)) => {
                *this.body_size += chunk.len();
                Poll::Ready(Some(Ok(chunk)))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}

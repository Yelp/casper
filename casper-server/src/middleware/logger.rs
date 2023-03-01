use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use mlua::{Function, LuaSerdeExt};
use ntex::http::body::{Body, BodySize, MessageBody, ResponseBody};
use ntex::service::{forward_poll_ready, forward_poll_shutdown, Middleware, Service};
use ntex::util::Bytes;
use ntex::web::{WebRequest, WebResponse};
use pin_project_lite::pin_project;
use serde::Serialize;
use tracing::error;

use crate::context::AppContext;
use crate::metrics::METRICS;
use crate::types::LuaContext;

#[derive(Debug)]
pub struct Logger;

impl Logger {
    pub fn new() -> Self {
        Logger
    }
}

impl<S> Middleware<S> for Logger {
    type Service = LoggerMiddleware<S>;

    fn create(&self, service: S) -> Self::Service {
        LoggerMiddleware { service }
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
    response_size: u64,
    error: Option<bool>,
}

/// Logger middleware
pub struct LoggerMiddleware<S> {
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

        ntex::rt::spawn(async move {
            if let Err(err) = log.await {
                error!("{err:#}");
            }
        });
    }
}

impl<S, E> Service<WebRequest<E>> for LoggerMiddleware<S>
where
    S: Service<WebRequest<E>, Response = WebResponse>,
{
    type Response = WebResponse;
    type Error = S::Error;
    type Future<'f> = LoggerResponse<'f, S, E> where S: 'f, E: 'f;

    forward_poll_ready!(service);
    forward_poll_shutdown!(service);

    #[inline]
    fn call(&self, req: WebRequest<E>) -> Self::Future<'_> {
        let start = Instant::now();

        let app_context: AppContext = req.app_state::<AppContext>().unwrap().clone();

        let log_data = app_context.access_log.as_ref().map(|_| LogData {
            uri: req.uri().to_string(),
            method: req.method().to_string(),
            remote_addr: req.peer_addr().map(|addr| addr.to_string()),
            ..Default::default()
        });

        LoggerResponse {
            fut: self.service.call(req),
            start,
            app_context,
            lua_context: None,
            log_data,
            _phantom: PhantomData,
        }
    }
}

pin_project! {
    pub struct LoggerResponse<'f, S: Service<WebRequest<E>>, E>
    where S: 'f, E: 'f
    {
        #[pin]
        fut: S::Future<'f>,
        start: Instant,
        app_context: AppContext,
        lua_context: Option<LuaContext>,
        log_data: Option<LogData>,
        _phantom: PhantomData<E>,
    }
}

impl<'f, S, E> Future for LoggerResponse<'f, S, E>
where
    S: Service<WebRequest<E>, Response = WebResponse>,
{
    type Output = Result<WebResponse, S::Error>;

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

        Poll::Ready(Ok(res.map_body(move |_, body| {
            ResponseBody::Other(Body::from_message(StreamLog {
                body,
                body_size: 0,
                app_context: this.app_context.clone(),
                lua_context: this.lua_context.take(),
                log_data: this.log_data.take(),
            }))
        })))
    }
}

/// Used to calculate final body size and spawn logging coroutine
pub struct StreamLog {
    body: ResponseBody<Body>,
    body_size: u64,
    app_context: AppContext,
    lua_context: Option<LuaContext>,
    log_data: Option<LogData>,
}

// This is where we execute log action, after streaming body
impl Drop for StreamLog {
    fn drop(&mut self) {
        if let (Some(mut log_data), Some(lua_ctx)) = (self.log_data.take(), self.lua_context.take())
        {
            log_data.response_size = self.body_size;
            LoggerMiddleware::spawn_access_log(self.app_context.clone(), log_data, lua_ctx)
        }
    }
}

impl MessageBody for StreamLog {
    #[inline]
    fn size(&self) -> BodySize {
        self.body.size()
    }

    fn poll_next_chunk(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Box<dyn Error>>>> {
        match futures::ready!(self.body.poll_next_chunk(cx)) {
            Some(Ok(chunk)) => {
                self.body_size += chunk.len() as u64;
                Poll::Ready(Some(Ok(chunk)))
            }
            Some(Err(err)) => {
                if let Some(log_data) = self.log_data.as_mut() {
                    log_data.error = Some(true);
                }
                Poll::Ready(Some(Err(err)))
            }
            val => Poll::Ready(val),
        }
    }
}

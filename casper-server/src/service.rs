use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use hyper::{service::Service, Body, Request, Response};

use crate::handler;
use crate::lua::LuaRequest;
use crate::types::LuaContext;
use crate::worker::WorkerContext;

#[derive(Clone)]
pub struct Svc {
    pub worker_ctx: WorkerContext,
    pub remote_addr: SocketAddr,
}

impl Service<Request<Body>> for Svc {
    type Response = Response<Body>;
    type Error = anyhow::Error;
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
    #[tracing::instrument(skip(self, req), fields(method = %req.method(), uri = %req.uri()))]
    async fn handler(self, req: Request<Body>) -> Result<Response<Body>, anyhow::Error> {
        let start = Instant::now();
        let _req_count_guard = requests_counter_inc!();
        let lua = &self.worker_ctx.lua;

        let worker_ctx = self.worker_ctx.clone();
        let lua_ctx = LuaContext::new(lua); // Create Lua context table
        let mut lua_req = LuaRequest::from(req);
        lua_req.set_remote_addr(self.remote_addr);

        let mut resp = handler::handler(worker_ctx, lua_req, lua_ctx.clone()).await?;

        requests_histogram_rec!(start, "status" => resp.status().as_u16() as i64);
        resp.extensions_mut().insert(lua_ctx);

        Ok(resp.into_inner())
    }
}

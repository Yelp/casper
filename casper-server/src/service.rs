use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use hyper::{service::Service, Body, Request, Response};
use opentelemetry::{Key as OTKey, Value as OTValue};

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
        let _req_guard = active_request_guard!();
        let lua = &self.worker_ctx.lua;

        let worker_ctx = self.worker_ctx.clone();
        let lua_ctx = LuaContext::new(lua); // Create Lua context table
        let mut lua_req = LuaRequest::from(req);
        lua_req.set_remote_addr(self.remote_addr);

        let method = lua_req.method().to_string();
        let mut resp_result = handler::handler(worker_ctx, lua_req, lua_ctx.clone()).await;

        // Collect response labels for metric attributes
        let mut attrs_map: HashMap<OTKey, OTValue> = HashMap::new();
        attrs_map.insert("method".into(), method.into());
        match resp_result {
            Ok(ref mut resp) => {
                attrs_map.insert("status".into(), (resp.status().as_u16() as i64).into());
                // Read labels set by Lua and attach them
                if let Some(lua_labels) = resp.take_labels() {
                    for (k, v) in lua_labels {
                        attrs_map.insert(k, v);
                    }
                }
            }
            Err(_) => {
                attrs_map.insert("status".into(), 0.into());
            }
        }
        requests_counter_inc!(attrs_map);
        requests_histogram_rec!(start, attrs_map);

        let mut resp = resp_result?;
        resp.extensions_mut().insert(lua_ctx);

        Ok(resp.into_inner())
    }
}

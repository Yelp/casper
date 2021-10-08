use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::FutureExt;
use hyper::service::Service;
use hyper::{Body, Request, Response};
use mlua::Lua;

use crate::handler;
use crate::worker::WorkerData;

pub struct Svc {
    pub lua: Rc<Lua>,
    pub worker_data: Rc<WorkerData>,
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
        // If handler returns an error, then generate 5xx response
        let handler = handler::handler(self.lua.clone(), self.worker_data.clone(), req);
        Box::pin(handler.map(|result| match result {
            Ok(res) => Ok(res),
            Err(err) => {
                println!("handler error: {}", err);
                Response::builder()
                    .status(500)
                    .body(Body::from("Internal Server Error"))
                    .map_err(Into::into)
            }
        }))
    }
}

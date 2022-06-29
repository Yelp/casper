use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::ready;
use hyper::{header, Body, Request, Response};
use pin_project_lite::pin_project;
use tower::{Layer, Service};
use tracing::error;

#[derive(Clone, Debug)]
pub struct ErrorLayer;

impl<S> Layer<S> for ErrorLayer {
    type Service = ErrorService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ErrorService { inner }
    }
}

#[derive(Clone, Debug)]
pub struct ErrorService<S> {
    inner: S,
}

impl<S> Service<Request<Body>> for ErrorService<S>
where
    S: Service<Request<Body>, Response = Response<Body>>,
    S::Error: Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ErrorResponseFuture<S::Future>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        ErrorResponseFuture::new(self.inner.call(request))
    }
}

pin_project! {
    /// [`ErrorService`] response future
    #[derive(Debug)]
    pub struct ErrorResponseFuture<T> {
        #[pin]
        response: T,
    }
}

impl<T> ErrorResponseFuture<T> {
    pub(crate) fn new(response: T) -> Self {
        ErrorResponseFuture { response }
    }
}

impl<F, E> Future for ErrorResponseFuture<F>
where
    F: Future<Output = Result<Response<Body>, E>>,
    E: Debug,
{
    type Output = Result<Response<Body>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.project().response.poll(cx)) {
            Ok(res) => Poll::Ready(Ok(res)),
            Err(err) => {
                error!("{err:?}");
                let resp = Response::builder()
                    .status(500)
                    .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Body::from("Internal Server Error"))
                    .expect("Failed to build response");
                Poll::Ready(Ok(resp))
            }
        }
    }
}

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::Either;
use hyper::{header::CONTENT_TYPE, service::Service, Body, Request, Response};
use pin_project_lite::pin_project;
use prometheus::{Encoder, TextEncoder};
use tower::Layer;

pub struct MetricsLayer {
    endpoint: Arc<String>,
}

impl MetricsLayer {
    pub fn new(endpoint: String) -> Self {
        MetricsLayer {
            endpoint: Arc::new(endpoint),
        }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = Metrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Metrics {
            endpoint: self.endpoint.clone(),
            inner,
        }
    }
}

#[derive(Debug)]
pub struct Metrics<S> {
    endpoint: Arc<String>,
    inner: S,
}

type MetricsRenderFuture<E> = Pin<Box<dyn Future<Output = Result<Response<Body>, E>>>>;

impl<S> Service<Request<Body>> for Metrics<S>
where
    S: Service<Request<Body>, Response = Response<Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<Either<S::Future, MetricsRenderFuture<S::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        if req.uri().path() == *self.endpoint {
            let render = Box::pin(async move { Ok(Self::metrics_handler(req).await) });
            return ResponseFuture::new(Either::Right(render));
        }

        ResponseFuture::new(Either::Left(self.inner.call(req)))
    }
}

impl<S> Metrics<S> {
    async fn metrics_handler(_: Request<Body>) -> Response<Body> {
        tokio::task::spawn_blocking(move || {
            let mut buffer = Vec::<u8>::new();
            let encoder = TextEncoder::new();
            let metric_families = super::PROMETHEUS_EXPORTER.registry().gather();
            encoder
                .encode(&metric_families, &mut buffer)
                .expect("Failed to encode metrics");

            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(Body::from(buffer))
                .expect("Failed to build response")
        })
        .await
        .expect("Failed to render metrics")
    }
}

pin_project! {
    /// [`Metrics`] response future
    #[derive(Debug)]
    pub struct ResponseFuture<T> {
        #[pin]
        response: T,
    }
}

impl<T> ResponseFuture<T> {
    pub(crate) fn new(response: T) -> Self {
        ResponseFuture { response }
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().response.poll(cx)
    }
}

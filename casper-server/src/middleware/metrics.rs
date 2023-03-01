use std::rc::Rc;

use ntex::http::header::{HeaderValue, CONTENT_TYPE};
use ntex::http::Response;
use ntex::service::{forward_poll_ready, forward_poll_shutdown, Middleware, Service};
use ntex::util::Either;
use ntex::web::{ErrorRenderer, WebRequest, WebResponse};

use futures::future::{FutureExt, LocalBoxFuture};
use prometheus::{Encoder, TextEncoder, TEXT_FORMAT};

use crate::metrics::PROMETHEUS_EXPORTER;

#[derive(Debug, Clone)]
pub struct Metrics {
    endpoint: Rc<String>,
}

impl Metrics {
    pub fn new(endpoint: String) -> Self {
        Metrics {
            endpoint: Rc::new(endpoint),
        }
    }
}

impl<S> Middleware<S> for Metrics {
    type Service = MetricsService<S>;

    fn create(&self, service: S) -> Self::Service {
        MetricsService {
            inner: service,
            endpoint: self.endpoint.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsService<S> {
    inner: S,
    endpoint: Rc<String>,
}

impl<S> MetricsService<S> {
    async fn metrics_handler<E>(request: WebRequest<E>) -> WebResponse {
        let data = tokio::task::spawn_blocking(move || {
            let mut buffer = Vec::<u8>::with_capacity(16384);
            let metric_families = PROMETHEUS_EXPORTER.registry().gather();
            TextEncoder::new()
                .encode(&metric_families, &mut buffer)
                .expect("Failed to encode metrics");
            buffer
        })
        .await
        .expect("Failed to render metrics");

        let response = Response::Ok()
            .header(CONTENT_TYPE, HeaderValue::from_static(TEXT_FORMAT))
            .body(data);

        request.into_response(response)
    }
}

impl<S, E> Service<WebRequest<E>> for MetricsService<S>
where
    S: Service<WebRequest<E>, Response = WebResponse>,
    E: ErrorRenderer,
{
    type Response = WebResponse;
    type Error = S::Error;
    type Future<'f> = Either<LocalBoxFuture<'f, Result<WebResponse, S::Error>>, S::Future<'f>> where S: 'f;

    forward_poll_ready!(inner);
    forward_poll_shutdown!(inner);

    #[inline]
    fn call(&self, req: WebRequest<E>) -> Self::Future<'_> {
        if req.uri().path() == *self.endpoint {
            return Either::Left(Box::pin(Self::metrics_handler(req).map(Ok)));
        }

        Either::Right(self.inner.call(req))
    }
}

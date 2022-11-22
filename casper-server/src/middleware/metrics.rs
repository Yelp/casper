use std::future::{self, Future, Ready};
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use actix_web::body::EitherBody;
use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::header::{HeaderValue, CONTENT_TYPE};
use actix_web::{Error, HttpResponse};
use bytes::Bytes;
use futures::future::LocalBoxFuture;
use pin_project_lite::pin_project;
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

impl<S, B> Transform<S, ServiceRequest> for Metrics
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Transform = MetricsService<S, B>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, inner: S) -> Self::Future {
        future::ready(Ok(MetricsService {
            inner,
            endpoint: self.endpoint.clone(),
            _phantom: PhantomData,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct MetricsService<S, B> {
    inner: S,
    endpoint: Rc<String>,
    _phantom: PhantomData<B>,
}

impl<S, B> MetricsService<S, B> {
    async fn metrics_handler(request: ServiceRequest) -> ServiceResponse<Bytes> {
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

        let response = HttpResponse::Ok()
            .append_header((CONTENT_TYPE, HeaderValue::from_static(TEXT_FORMAT)))
            .message_body(Bytes::from(data))
            .unwrap();

        request.into_response(response)
    }
}

impl<S, B> Service<ServiceRequest> for MetricsService<S, B>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = ResponseFuture<S::Future, B>;

    forward_ready!(inner);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        if req.uri().path() == *self.endpoint {
            let fut = Box::pin(async move {
                Ok(Self::metrics_handler(req)
                    .await
                    .map_into_boxed_body()
                    .map_into_right_body())
            });
            return ResponseFuture::MetricsFuture { fut };
        }

        let fut = self.inner.call(req);
        ResponseFuture::ServiceFuture { fut }
    }
}

pin_project! {
    /// [`Metrics`] response future
    #[project = ResponseFutureProj]
    pub enum ResponseFuture<Fut, B> {
        ServiceFuture {
            #[pin]
            fut: Fut,
        },
        MetricsFuture {
            fut: LocalBoxFuture<'static, Result<ServiceResponse<EitherBody<B>>, Error>>,
        }
    }
}

impl<Fut, B> Future for ResponseFuture<Fut, B>
where
    Fut: Future<Output = Result<ServiceResponse<B>, Error>>,
{
    type Output = Result<ServiceResponse<EitherBody<B>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_mut().project() {
            ResponseFutureProj::ServiceFuture { fut } => {
                let res = futures::ready!(fut.poll(cx))?;
                Poll::Ready(Ok(res.map_into_left_body()))
            }
            ResponseFutureProj::MetricsFuture { fut } => fut.as_mut().poll(cx),
        }
    }
}

use std::str::FromStr;

use ntex::http::header::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::propagation::{Extractor, Injector};

pub(crate) struct RequestHeaderCarrier<'a> {
    headers: &'a HeaderMap,
}

impl<'a> RequestHeaderCarrier<'a> {
    #[inline]
    pub(crate) fn new(headers: &'a HeaderMap) -> Self {
        RequestHeaderCarrier { headers }
    }
}

impl Extractor for RequestHeaderCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(|header| header.as_str()).collect()
    }
}

pub(crate) struct RequestHeaderCarrierMut<'a> {
    headers: &'a mut HeaderMap,
}

impl<'a> RequestHeaderCarrierMut<'a> {
    #[inline]
    pub(crate) fn new(headers: &'a mut HeaderMap) -> Self {
        RequestHeaderCarrierMut { headers }
    }
}

impl Injector for RequestHeaderCarrierMut<'_> {
    fn set(&mut self, key: &str, value: String) {
        let header_name = HeaderName::from_str(key).expect("invalid tracing header name");
        let header_value = HeaderValue::from_str(&value).expect("invalid tracing header value");
        self.headers.insert(header_name, header_value);
    }
}

/// A flag when attached to Context, indicates that the injected sampling decision should be "off",
/// but (our) spans should still be exported.
pub struct SilentOnSamplingDecision;

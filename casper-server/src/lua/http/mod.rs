use http::method::InvalidMethod;
use http::uri::InvalidUri;

pub use headers::{LuaHttpHeaders, LuaHttpHeadersExt};
pub use request::LuaRequest;
pub use response::LuaResponse;

#[derive(thiserror::Error, Debug)]
pub enum HttpError {
    #[error("invalid uri: {0}")]
    Uri(#[from] InvalidUri),
    #[error(transparent)]
    Method(#[from] InvalidMethod),
    #[error(transparent)]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error(transparent)]
    Http(#[from] hyper::Error),
    #[error(transparent)]
    Lua(#[from] mlua::Error),
}

mod headers;
mod request;
mod response;

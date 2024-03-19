use std::error::Error as StdError;

use ntex::http::body::MessageBody;
use ntex::util::{Bytes, BytesMut};

pub use proxy::{filter_hop_headers, proxy_to_upstream};

pub async fn buffer_body(mut body: impl MessageBody) -> Result<Bytes, Box<dyn StdError>> {
    let mut bytes = BytesMut::new();
    while let Some(item) = futures::future::poll_fn(|cx| body.poll_next_chunk(cx)).await {
        bytes.extend_from_slice(&item?);
    }
    Ok(bytes.freeze())
}

pub(crate) mod proxy;
pub(crate) mod trace;
pub(crate) mod websocket;
pub(crate) mod serde;

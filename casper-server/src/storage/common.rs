use ntex::http::header::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::http::serde as http_serde;

pub fn encode_headers(
    headers: &HeaderMap,
    v2: bool,
) -> Result<Vec<u8>, flexbuffers::SerializationError> {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    if v2 {
        headers.serialize(&mut serializer)?;
    } else {
        http_serde::header_map::serialize(headers, &mut serializer)?;
    }
    Ok(serializer.take_buffer())
}

pub fn decode_headers(
    data: &[u8],
    v2: bool,
) -> Result<HeaderMap, flexbuffers::DeserializationError> {
    let deserializer = flexbuffers::Reader::get_root(data)?;
    if v2 {
        HeaderMap::deserialize(deserializer)
    } else {
        http_serde::header_map::deserialize(deserializer)
    }
}

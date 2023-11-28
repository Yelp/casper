use ntex::http::header::HeaderMap;
use serde::{Deserialize, Serialize};

pub fn encode_headers(headers: &HeaderMap) -> Result<Vec<u8>, flexbuffers::SerializationError> {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    headers.serialize(&mut serializer)?;
    Ok(serializer.take_buffer())
}

pub fn decode_headers(data: &[u8]) -> Result<HeaderMap, flexbuffers::DeserializationError> {
    let deserializer = flexbuffers::Reader::get_root(data)?;
    HeaderMap::deserialize(deserializer)
}

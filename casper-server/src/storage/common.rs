use ntex::http::header::HeaderMap;

use crate::http::serde as http_serde;

pub fn encode_headers(headers: &HeaderMap) -> Result<Vec<u8>, flexbuffers::SerializationError> {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    http_serde::header_map::serialize(headers, &mut serializer)?;
    Ok(serializer.take_buffer())
}

pub fn decode_headers(data: &[u8]) -> Result<HeaderMap, flexbuffers::DeserializationError> {
    let deserializer = flexbuffers::Reader::get_root(data)?;
    http_serde::header_map::deserialize(deserializer)
}

use bytes::Bytes;
use http::HeaderMap;

pub fn encode_headers(headers: &HeaderMap) -> Result<Vec<u8>, flexbuffers::SerializationError> {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    http_serde::header_map::serialize(headers, &mut serializer)?;
    Ok(serializer.take_buffer())
}

pub fn decode_headers(data: &[u8]) -> Result<HeaderMap, flexbuffers::DeserializationError> {
    let deserializer = flexbuffers::Reader::get_root(data)?;
    http_serde::header_map::deserialize(deserializer)
}

pub async fn compress_with_zstd(data: Bytes, level: i32) -> Result<Vec<u8>, anyhow::Error> {
    let result =
        tokio::task::spawn_blocking(move || zstd::stream::encode_all(data.as_ref(), level)).await;
    Ok(result??)
}

use ntex::http::header::HeaderMap;

pub fn encode_headers(headers: &HeaderMap) -> Result<Vec<u8>, flexbuffers::SerializationError> {
    // Covert ntex headers to http headers
    // TODO: Avoid conversion and serialize directly from ntex's HeaderMap
    let mut http_headers = http::HeaderMap::with_capacity(headers.len());
    for (name, val) in headers {
        http_headers.append(
            name.clone(),
            http::HeaderValue::from_bytes(val.as_bytes()).unwrap(),
        );
    }
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    http_serde::header_map::serialize(&http_headers, &mut serializer)?;
    Ok(serializer.take_buffer())
}

pub fn decode_headers(data: &[u8]) -> Result<HeaderMap, flexbuffers::DeserializationError> {
    let deserializer = flexbuffers::Reader::get_root(data)?;
    (http_serde::header_map::deserialize(deserializer) as Result<http::HeaderMap, _>)
        .map(Into::into)
}

pub async fn compress_with_zstd<B>(data: B, level: i32) -> Result<Vec<u8>, anyhow::Error>
where
    B: AsRef<[u8]> + Send + 'static,
{
    let result =
        tokio::task::spawn_blocking(move || zstd::stream::encode_all(data.as_ref(), level)).await;
    Ok(result??)
}

pub async fn decompress_with_zstd<B>(data: B) -> Result<Vec<u8>, anyhow::Error>
where
    B: AsRef<[u8]> + Send + 'static,
{
    let result = tokio::task::spawn_blocking(move || zstd::stream::decode_all(data.as_ref())).await;
    Ok(result??)
}

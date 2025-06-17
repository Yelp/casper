use std::future::Future;
use std::io::Error as IoError;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, stream::Stream};
use ntex::util::{Buf, Bytes, BytesMut};
use pin_project_lite::pin_project;
use tokio::task::{spawn_blocking, JoinHandle};
use zstd::stream::raw::{Decoder, Operation, OutBuffer, Status as ZstdStatus};

const ENCODE_INPLACE_THRESHOLD: usize = 1024;
const DECODE_INPLACE_THRESHOLD: usize = 4096;

pub async fn compress_with_zstd<B>(data: B, level: i32) -> Result<Bytes, IoError>
where
    B: AsRef<[u8]> + Send + 'static,
{
    if data.as_ref().len() <= ENCODE_INPLACE_THRESHOLD {
        return zstd::stream::encode_all(data.as_ref(), level).map(Bytes::from);
    }
    spawn_blocking(move || zstd::stream::encode_all(data.as_ref(), level))
        .await?
        .map(Bytes::from)
}

#[inline]
pub async fn decompress_with_zstd<B>(data: B) -> Result<Bytes, IoError>
where
    B: AsRef<[u8]> + Send + 'static,
{
    if data.as_ref().len() <= DECODE_INPLACE_THRESHOLD {
        return zstd::stream::decode_all(data.as_ref()).map(Bytes::from);
    }
    spawn_blocking(move || zstd::stream::decode_all(data.as_ref()))
        .await?
        .map(Bytes::from)
}

pin_project! {
    pub struct ZstdDecoder<S> {
        #[pin]
        stream: S,
        decoder: Option<Decoder<'static>>,
        state: State,
        input: Option<Bytes>,
        buffer: Option<BytesMut>,
    }
}

type InterimResult = Result<(Decoder<'static>, Bytes, BytesMut, ZstdStatus), IoError>;

enum State {
    Reading,
    Decoding(Option<JoinHandle<InterimResult>>),
    Flushing,
    Done,
}

// This size equals to ZSTD_BLOCKSIZE_MAX
const OUTPUT_BUFFER_SIZE: usize = 131072; // 128KB

impl<S> ZstdDecoder<S> {
    pub fn new(stream: S) -> Self {
        let mut buffer = BytesMut::with_capacity(OUTPUT_BUFFER_SIZE);
        buffer.resize(OUTPUT_BUFFER_SIZE, 0);
        Self {
            stream,
            decoder: Some(Decoder::new().expect("Unable to create zstd decoder")),
            state: State::Reading,
            input: Some(Bytes::new()),
            buffer: Some(buffer),
        }
    }
}

impl<S> Stream for ZstdDecoder<S>
where
    S: Stream<Item = Result<Bytes, IoError>>,
{
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.state {
                State::Reading => {
                    if let Some(chunk) = ready!(this.stream.as_mut().poll_next(cx)) {
                        *this.input = Some(chunk?);
                        *this.state = State::Decoding(None);
                    } else {
                        *this.state = State::Flushing;
                    }
                }

                State::Decoding(join_handle @ None) => {
                    if this.input.is_none() || this.input.as_ref().unwrap().is_empty() {
                        *this.state = State::Reading;
                        continue;
                    }

                    let input = this.input.as_mut().unwrap();
                    if input.len() <= DECODE_INPLACE_THRESHOLD {
                        let decoder = this.decoder.as_mut().unwrap();
                        let buffer = this.buffer.as_mut().unwrap();
                        let status = decoder.run_on_buffers(input, buffer)?;
                        input.advance(status.bytes_read);
                        if status.bytes_written > 0 {
                            let chunk = Bytes::copy_from_slice(&buffer[..status.bytes_written]);
                            break Poll::Ready(Some(Ok(chunk)));
                        }
                        continue;
                    }

                    // Temporary move buffers to a dedicated threads
                    let mut decoder = this.decoder.take().unwrap();
                    let input = this.input.take().unwrap();
                    let mut buffer = this.buffer.take().unwrap();
                    *join_handle = Some(spawn_blocking(move || {
                        decoder
                            .run_on_buffers(&input, &mut buffer)
                            .map(|status| (decoder, input, buffer, status))
                    }));
                }

                State::Decoding(Some(join_handle)) => {
                    let (decoder, mut input, buffer, status) =
                        ready!(Pin::new(join_handle).poll(cx))??;
                    *this.decoder = Some(decoder);
                    input.advance(status.bytes_read);
                    *this.input = Some(input);
                    *this.buffer = Some(buffer);
                    *this.state = State::Decoding(None);
                    if status.bytes_written > 0 {
                        let buffer = this.buffer.as_ref().unwrap();
                        let chunk = Bytes::copy_from_slice(&buffer[..status.bytes_written]);
                        break Poll::Ready(Some(Ok(chunk)));
                    }
                }

                State::Flushing => {
                    let buffer = this.buffer.as_mut().unwrap();
                    let mut temp_buffer = OutBuffer::around(&mut buffer[..]);
                    let decoder = this.decoder.as_mut().unwrap();
                    let bytes_left = decoder.flush(&mut temp_buffer)?;
                    if bytes_left > 0 {
                        // Continue flushing until all bytes are written
                        *this.state = State::Flushing;
                    } else {
                        *this.state = State::Done;
                    }
                    if temp_buffer.pos() > 0 {
                        let chunk = Bytes::copy_from_slice(temp_buffer.as_slice());
                        break Poll::Ready(Some(Ok(chunk)));
                    }
                }

                State::Done => {
                    break Poll::Ready(None);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::{self, TryStreamExt};
    use rand::distr::{Alphanumeric, SampleString};

    #[ntex::test]
    async fn test_compress_decompress() {
        let data = Bytes::from_static(b"Hello, world!");
        let compressed = compress_with_zstd(data.clone(), 0).await.unwrap();
        assert_ne!(data, compressed);
        let decompressed = decompress_with_zstd(compressed).await.unwrap();
        assert_eq!(data, decompressed);
    }

    #[ntex::test]
    async fn test_decompress_stream() {
        let raw_data = Alphanumeric.sample_string(&mut rand::rng(), OUTPUT_BUFFER_SIZE * 3);
        let data = Bytes::from(raw_data);
        let compressed = compress_with_zstd(data.clone(), 0).await.unwrap();

        // Try bigger chunks (to decode in a separate thread)
        let stream = stream::iter(
            compressed
                .chunks(16 * 1024)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
        );
        let decoder = ZstdDecoder::new(stream);
        let decoded_chunks = decoder.try_collect::<Vec<Bytes>>().await.unwrap();
        assert!(decoded_chunks.len() > 1);
        assert_eq!(decoded_chunks.concat(), data);

        // Try smaller chunks (to decode in-place)
        let stream = stream::iter(
            compressed
                .chunks(1024)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
        );
        let decoder = ZstdDecoder::new(stream);
        let decoded_chunks = decoder.try_collect::<Vec<Bytes>>().await.unwrap();
        assert!(decoded_chunks.len() > 1);
        assert_eq!(decoded_chunks.concat(), data);
    }
}

use std::io::Error as IoError;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, stream::Stream};
use ntex::util::{Buf, Bytes, BytesMut};
use pin_project_lite::pin_project;
use zstd::stream::raw::{Decoder, Operation, OutBuffer};

pub async fn compress_with_zstd<B>(data: B, level: i32) -> Result<Bytes, IoError>
where
    B: AsRef<[u8]> + Send + 'static,
{
    tokio::task::spawn_blocking(move || zstd::stream::encode_all(data.as_ref(), level))
        .await?
        .map(Bytes::from)
}

#[inline]
pub fn decompress_with_zstd(data: &[u8]) -> Result<Bytes, IoError> {
    zstd::stream::decode_all(data).map(Bytes::from)
}

pin_project! {
    pub struct ZstdDecoder<S> {
        #[pin]
        stream: S,
        decoder: Decoder<'static>,
        state: State,
        input: Bytes,
        output: BytesMut,
    }
}

enum State {
    Reading,
    Decoding,
    Flushing,
    Done,
}

// This size equals to ZSTD_BLOCKSIZE_MAX
const OUTPUT_BUFFER_SIZE: usize = 131072; // 128KB

impl<S> ZstdDecoder<S> {
    pub fn new(stream: S) -> Self {
        let mut output = BytesMut::with_capacity(OUTPUT_BUFFER_SIZE);
        output.resize(OUTPUT_BUFFER_SIZE, 0);
        Self {
            stream,
            decoder: Decoder::new().expect("Unable to create zstd decoder"),
            state: State::Reading,
            input: Bytes::new(),
            output,
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
            match *this.state {
                State::Reading => {
                    if let Some(chunk) = ready!(this.stream.as_mut().poll_next(cx)) {
                        *this.input = chunk?;
                        *this.state = State::Decoding;
                    } else {
                        *this.state = State::Flushing;
                    }
                }

                State::Decoding => {
                    if this.input.is_empty() {
                        *this.state = State::Reading;
                        continue;
                    }

                    let status = this.decoder.run_on_buffers(this.input, this.output)?;
                    this.input.advance(status.bytes_read);
                    if status.bytes_written > 0 {
                        let chunk = Bytes::copy_from_slice(&this.output[..status.bytes_written]);
                        break Poll::Ready(Some(Ok(chunk)));
                    }
                }

                State::Flushing => {
                    let mut temp_buffer = OutBuffer::around(this.output.as_mut());
                    let bytes_left = this.decoder.flush(&mut temp_buffer)?;
                    if bytes_left > 0 {
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
    use rand::distributions::{Alphanumeric, DistString};

    #[ntex::test]
    async fn test_compress_decompress() {
        let data = Bytes::from_static(b"Hello, world!");
        let compressed = compress_with_zstd(data.clone(), 0).await.unwrap();
        assert_ne!(data, compressed);
        let decompressed = decompress_with_zstd(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[ntex::test]
    async fn test_decompress_stream() {
        let raw_data = Alphanumeric.sample_string(&mut rand::thread_rng(), OUTPUT_BUFFER_SIZE * 3);
        let data = Bytes::from(raw_data);
        let compressed = compress_with_zstd(data.clone(), 0).await.unwrap();
        let stream = stream::iter(
            compressed
                .chunks(16 * 1024)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
        );
        let decoder = ZstdDecoder::new(stream);
        let decoded_chunks = decoder.try_collect::<Vec<Bytes>>().await.unwrap();
        assert!(decoded_chunks.len() > 1);
        assert_eq!(decoded_chunks.concat(), data);
    }
}

use std::io::Error as IoError;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, stream::Stream};
use ntex::util::{Buf, Bytes, BytesMut};
use pin_project_lite::pin_project;
use zstd::stream::raw::Operation;

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

enum State {
    Reading,
    Writing,
    Flushing,
    Done,
}

pin_project! {
    pub struct ZstdDecoder<S> {
        #[pin]
        stream: S,
        decoder: zstd::stream::raw::Decoder<'static>,
        state: State,
        input: Bytes,
        output: BytesMut,
    }
}

const OUTPUT_BUFFER_SIZE: usize = 8192; // 8KB

impl<S> ZstdDecoder<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            decoder: zstd::stream::raw::Decoder::new().expect("Unable to create zstd decoder"),
            state: State::Reading,
            input: Bytes::new(),
            output: BytesMut::new(),
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

        let result = (|| loop {
            let mut temp_buffer = [0u8; OUTPUT_BUFFER_SIZE];

            *this.state = match this.state {
                State::Reading => {
                    if let Some(chunk) = ready!(this.stream.as_mut().poll_next(cx)) {
                        *this.input = chunk?;
                        State::Writing
                    } else {
                        State::Flushing
                    }
                }

                State::Writing => {
                    if this.input.is_empty() {
                        State::Reading
                    } else {
                        let status = this
                            .decoder
                            .run_on_buffers(&*this.input, &mut temp_buffer)?;
                        this.input.advance(status.bytes_read);
                        this.output
                            .extend_from_slice(&temp_buffer[0..status.bytes_written]);
                        if status.remaining == 0 {
                            State::Flushing
                        } else {
                            State::Writing
                        }
                    }
                }

                State::Flushing => {
                    let mut temp_output_buffer =
                        zstd::stream::raw::OutBuffer::around(&mut temp_buffer);
                    let bytes_left = this.decoder.flush(&mut temp_output_buffer)?;
                    this.output.extend_from_slice(temp_output_buffer.as_slice());
                    if bytes_left > 0 {
                        State::Flushing
                    } else {
                        State::Done
                    }
                }

                State::Done => {
                    return Poll::Ready(None);
                }
            };
        })();

        match result {
            Poll::Ready(Some(Ok(_))) => unreachable!(),
            Poll::Ready(Some(Err(_))) => {
                *this.state = State::Done;
                result
            }
            Poll::Ready(None) | Poll::Pending => {
                if this.output.is_empty() {
                    result
                } else {
                    let result = Poll::Ready(Some(Ok(this.output.split().freeze())));
                    *this.output = BytesMut::new();
                    result
                }
            }
        }
    }
}

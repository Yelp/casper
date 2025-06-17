use std::borrow::Cow;
use std::future::Future;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, stream::Stream};
use ntex::util::{Bytes, BytesMut};
use openssl::symm::{decrypt_aead, encrypt_aead, Cipher, Crypter, Mode};
use pin_project_lite::pin_project;
use rand::{rng as thread_rng, RngCore};
use tokio::task::{spawn_blocking, JoinHandle};

/// Some constants used by AES256-GCM.
const IV_SIZE: usize = 12;
const TAG_SIZE: usize = 16;

/// Encrypts data with AES256-GCM using the key.
///
/// It does not block when performing the encryption.
pub async fn aes256_encrypt<B>(data: B, key: Bytes) -> Result<Bytes, IoError>
where
    B: AsRef<[u8]> + Send + 'static,
{
    spawn_blocking(move || {
        let data = data.as_ref();
        let cipher = Cipher::aes_256_gcm();

        let mut iv = vec![0; IV_SIZE];
        thread_rng().fill_bytes(&mut iv);
        let mut tag = [0; TAG_SIZE];

        let key = normalize_key(&key, cipher.key_len());
        encrypt_aead(cipher, &key, Some(&iv), &[], data, &mut tag)
            .map(|mut data| {
                // Prepend iv and tag to the beginning of the encrypted data:
                // <iv><tag><encrypted data>
                data.reverse();
                tag.reverse();
                data.extend_from_slice(&tag);
                iv.reverse();
                data.extend_from_slice(&iv);
                data.reverse();
                Bytes::from(data)
            })
            .map_err(|_| IoError::new(IoErrorKind::Other, "failed to encrypt data"))
    })
    .await?
}

/// Decrypts data with AES256-GCM using the key.
///
/// The data must be encrypted with `aes256_encrypt` and contain the iv and tag.
pub async fn aes256_decrypt<B>(data: B, key: Bytes) -> Result<Bytes, IoError>
where
    B: AsRef<[u8]> + Send + 'static,
{
    spawn_blocking(move || {
        let cipher = Cipher::aes_256_gcm();
        let data = data.as_ref();

        let (iv_tag, data) = data.split_at(IV_SIZE + TAG_SIZE);
        let (iv, tag) = iv_tag.split_at(IV_SIZE);

        let key = normalize_key(&key, cipher.key_len());
        decrypt_aead(cipher, &key, Some(iv), &[], data, tag)
            .map(Into::into)
            .map_err(|_| IoError::new(IoErrorKind::Other, "failed to decrypt data"))
    })
    .await?
}

/// Normalizes the key to the required length.
fn normalize_key(key: &[u8], required_len: usize) -> Cow<[u8]> {
    match key.len() {
        len if len > required_len => Cow::Borrowed(&key[..required_len]),
        len if len < required_len => {
            let mut new_key = vec![0; required_len];
            new_key[..len].copy_from_slice(key);
            Cow::Owned(new_key)
        }
        _ => Cow::Borrowed(key),
    }
}

pin_project! {
    pub struct AESDecoder<S> {
        #[pin]
        stream: S,
        key: Bytes,
        decrypter: Option<Crypter>,
        state: State,
        input: Option<Bytes>,
        buffer: Option<BytesMut>,
    }
}

type InterimResult = Result<(Crypter, BytesMut, usize), IoError>;

enum State {
    Init,
    Reading,
    Decoding(Option<JoinHandle<InterimResult>>),
    Flushing,
    Done,
}

impl<S> AESDecoder<S> {
    pub fn new(stream: S, key: Bytes) -> Self {
        Self {
            stream,
            key,
            decrypter: None,
            state: State::Init,
            input: None,
            buffer: Some(BytesMut::new()),
        }
    }
}

impl<S> Stream for AESDecoder<S>
where
    S: Stream<Item = Result<Bytes, IoError>>,
{
    type Item = <S as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.state {
                State::Init => {
                    // Fetch iv and tag to init decrypter
                    if let Some(chunk) = ready!(this.stream.as_mut().poll_next(cx)) {
                        let buffer = this.buffer.as_mut().unwrap();
                        buffer.extend_from_slice(&chunk?);
                        if buffer.len() < TAG_SIZE + IV_SIZE {
                            continue; // Not enough data
                        }

                        // Init decrypter
                        let iv = &buffer[..IV_SIZE];
                        let tag = &buffer[IV_SIZE..IV_SIZE + TAG_SIZE];
                        let cipher = Cipher::aes_256_gcm();
                        let key = normalize_key(this.key, cipher.key_len());
                        let decrypter = Crypter::new(cipher, Mode::Decrypt, &key, Some(iv))
                            .and_then(|mut decr| decr.set_tag(tag).map(|_| decr))
                            .map_err(|_| {
                                IoError::new(IoErrorKind::Other, "failed to init decrypter")
                            })?;
                        *this.input = Some(Bytes::copy_from_slice(&buffer[IV_SIZE + TAG_SIZE..]));
                        *this.decrypter = Some(decrypter);
                        *this.state = State::Decoding(None);
                    } else {
                        *this.state = State::Done;
                    }
                }

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

                    let mut decrypter = this.decrypter.take().unwrap();
                    let input = this.input.take().unwrap();
                    let mut buffer = this.buffer.take().unwrap();
                    buffer.resize(input.len(), 0);
                    *join_handle = Some(spawn_blocking(move || {
                        decrypter
                            .update(&input, &mut buffer)
                            .map(|count| (decrypter, buffer, count))
                            .map_err(|_| {
                                IoError::new(IoErrorKind::InvalidData, "failed to decrypt chunk")
                            })
                    }));
                }

                State::Decoding(Some(join_handle)) => {
                    let (decrypter, buffer, count) = ready!(Pin::new(join_handle).poll(cx))??;
                    *this.decrypter = Some(decrypter);
                    *this.buffer = Some(buffer);
                    *this.state = State::Reading;
                    if count > 0 {
                        let buffer = this.buffer.as_ref().unwrap();
                        break Poll::Ready(Some(Ok(Bytes::copy_from_slice(&buffer[..count]))));
                    }
                }

                State::Flushing => {
                    let decrypter = this.decrypter.as_mut().unwrap();
                    let buffer = this.buffer.as_mut().unwrap();
                    let count = decrypter.finalize(buffer).map_err(|_| {
                        IoError::new(IoErrorKind::InvalidData, "failed to finalize decryption")
                    })?;
                    *this.state = State::Done;
                    if count > 0 {
                        break Poll::Ready(Some(Ok(buffer.split_to(count).freeze())));
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

    #[ntex::test]
    async fn test_encrypt_decrypt() {
        let key = Bytes::from_static(b"some key");

        let data = Bytes::from_static(b"hello");
        let encrypted = aes256_encrypt(data.clone(), key.clone()).await.unwrap();

        let decrypted = aes256_decrypt(encrypted, key).await.unwrap();
        assert_eq!(decrypted, data);
    }

    #[ntex::test]
    async fn test_decrypt_stream() {
        let key = Bytes::from_static(b"some key");

        let data =
            b"hello world, this is a long string that will be encrypted and decrypted.".repeat(100);
        let encrypted = aes256_encrypt(data.clone(), key.clone()).await.unwrap();

        let stream = stream::iter(
            encrypted
                .chunks(256)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
        );
        let decoder = AESDecoder::new(stream, key.clone());
        let decoded_chunks = decoder.try_collect::<Vec<Bytes>>().await.unwrap();
        assert_eq!(decoded_chunks.len(), 29);
        assert_eq!(decoded_chunks.concat(), data);
    }
}

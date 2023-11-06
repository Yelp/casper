use std::borrow::Cow;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, stream::Stream};
use ntex::util::{Bytes, BytesMut};
use openssl::symm::{decrypt_aead, encrypt_aead, Cipher, Crypter, Mode};
use pin_project_lite::pin_project;
use rand::{thread_rng, RngCore};

/// Size of the tag used by AES256-GCM.
const TAG_SIZE: usize = 16;

/// Encrypts data with AES256-GCM using the key.
///
/// It does not block when performing the encryption.
pub async fn aes256_encrypt<B>(data: B, key: Bytes) -> Result<Bytes, IoError>
where
    B: AsRef<[u8]> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let data = data.as_ref();
        let cipher = Cipher::aes_256_gcm();

        let mut iv = vec![0; cipher.iv_len().unwrap()];
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
pub fn aes256_decrypt(data: &[u8], key: &[u8]) -> Result<Bytes, IoError> {
    let cipher = Cipher::aes_256_gcm();

    let (iv_tag, data) = data.split_at(cipher.iv_len().unwrap() + TAG_SIZE);
    let (iv, tag) = iv_tag.split_at(cipher.iv_len().unwrap());

    let key = normalize_key(key, cipher.key_len());
    decrypt_aead(cipher, &key, Some(iv), &[], data, tag)
        .map(Into::into)
        .map_err(|_| IoError::new(IoErrorKind::Other, "failed to decrypt data"))
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
        cipher: Option<Cipher>,
        decrypter: Option<Crypter>,
        iv_len: usize,
        block_size: usize,
        state: State,
        input: Bytes,
        buffer: BytesMut,
    }
}

enum State {
    Init,
    Reading,
    Decoding,
    Flushing,
    Done,
}

impl<S> AESDecoder<S> {
    pub fn new(stream: S, key: Bytes) -> Self {
        let cipher = Cipher::aes_256_gcm();
        let iv_len = cipher.iv_len().unwrap();
        let block_size = cipher.block_size();

        Self {
            stream,
            key,
            cipher: Some(cipher),
            decrypter: None,
            iv_len,
            block_size,
            state: State::Init,
            input: Bytes::new(),
            buffer: BytesMut::new(),
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
            match *this.state {
                State::Init => {
                    // Fetch iv and tag to init decrypter
                    if let Some(chunk) = ready!(this.stream.as_mut().poll_next(cx)) {
                        this.buffer.extend_from_slice(&chunk?);
                        if this.buffer.len() < TAG_SIZE + *this.iv_len {
                            // Not enough data, continue
                            continue;
                        }

                        // Init decrypter
                        let iv_tag = this.buffer.split_to(*this.iv_len + TAG_SIZE);
                        let (iv, tag) = iv_tag.split_at(*this.iv_len);
                        let cipher = this.cipher.take().unwrap();
                        let key = normalize_key(this.key, cipher.key_len());
                        let mut decrypter = Crypter::new(cipher, Mode::Decrypt, &key, Some(iv))
                            .map_err(|_| {
                                IoError::new(IoErrorKind::Other, "failed to create decrypter")
                            })?;
                        decrypter.set_tag(tag).unwrap();
                        *this.decrypter = Some(decrypter);
                        *this.input = mem::take(this.buffer).freeze(); // Consume the rest of the buffer
                        *this.state = State::Decoding;
                    } else {
                        *this.state = State::Done;
                    }
                }

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

                    let decrypter = this.decrypter.as_mut().unwrap();
                    this.buffer.resize(this.input.len() + *this.block_size, 0);
                    let count = decrypter.update(this.input, this.buffer).map_err(|_| {
                        IoError::new(IoErrorKind::InvalidData, "failed to decrypt chunk")
                    })?;
                    *this.state = State::Reading;
                    if count > 0 {
                        break Poll::Ready(Some(Ok(Bytes::copy_from_slice(&this.buffer[..count]))));
                    }
                }

                State::Flushing => {
                    let decrypter = this.decrypter.as_mut().unwrap();
                    this.buffer.resize(*this.block_size, 0);
                    let count = decrypter.finalize(this.buffer).map_err(|_| {
                        IoError::new(IoErrorKind::InvalidData, "failed to finalize decryption")
                    })?;
                    *this.state = State::Done;
                    if count > 0 {
                        break Poll::Ready(Some(Ok(this.buffer.split_to(count).freeze())));
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

        let decrypted = aes256_decrypt(&encrypted, &key).unwrap();
        assert_eq!(decrypted, data);
    }

    #[ntex::test]
    async fn test_decrypt_stream() {
        let key = Bytes::from_static(b"some key");

        let data = b"hello world, this is a long string that will be encrypted and decrypted";
        let encrypted = aes256_encrypt(data, key.clone()).await.unwrap();

        let stream = stream::iter(
            encrypted
                .chunks(8)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
        );
        let decoder = AESDecoder::new(stream, key.clone());
        let decoded_chunks = decoder.try_collect::<Vec<Bytes>>().await.unwrap();
        assert_eq!(decoded_chunks.len(), 10);
        assert_eq!(decoded_chunks.concat(), data);
    }
}

use rand::{distributions::Alphanumeric, thread_rng, Rng, RngCore};

/// Generates a random string with a given length and charset
///
/// Default charset is Alphanumeric
pub fn random_string(len: usize, charset: Option<&str>) -> String {
    let mut rng = thread_rng();
    match charset {
        None | Some("") => rng
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect(),
        Some("hex") => {
            let mut buf = vec![0u8; (len + 1) / 2];
            rng.fill_bytes(&mut buf);
            let mut s = hex::encode(&buf);
            if len % 2 != 0 {
                s.pop();
            }
            s
        }
        Some(charset) => {
            let charset = charset.chars().collect::<Vec<_>>();
            (0..len)
                .map(|_| charset[rng.gen_range(0..charset.len())])
                .collect()
        }
    }
}

pub mod aes;
pub mod zstd;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_string() {
        assert!(random_string(0, None).len() == 0);
        assert!(random_string(8, Some("hex")).len() == 8);
        assert!(random_string(5, Some("hex")).len() == 5);

        // Custom charset
        for c in random_string(32, Some("qw!")).chars() {
            assert!(c == 'q' || c == 'w' || c == '!');
        }
    }
}

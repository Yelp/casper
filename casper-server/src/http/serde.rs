//! Based on `http-serde` crate

/// For `HeaderMap`
///
/// `#[serde(with = "casper_server::http::serde::header_map")]`
pub mod header_map {
    use std::borrow::Cow;
    use std::fmt;

    use ntex::http::header::{HeaderMap, HeaderName, HeaderValue};
    use serde::de::{self, Deserializer, MapAccess, Unexpected, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Serialize, Serializer};

    struct ToSeq<'a>(&'a HeaderMap, &'a HeaderName);

    impl<'a> Serialize for ToSeq<'a> {
        fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
            let name = self.1;
            let count = self.0.get_all(name).count();
            if ser.is_human_readable() {
                if count == 1 {
                    let v = self.0.get(name).expect("header is present");
                    if let Ok(s) = v.to_str() {
                        return ser.serialize_str(s);
                    }
                }
                ser.collect_seq(self.0.get_all(name).filter_map(|v| v.to_str().ok()))
            } else {
                let mut seq = ser.serialize_seq(Some(count))?;
                for v in self.0.get_all(name) {
                    seq.serialize_element(v.as_bytes())?;
                }
                seq.end()
            }
        }
    }

    /// Implementation detail. Use derive annotations instead.
    pub fn serialize<S: Serializer>(headers: &HeaderMap, ser: S) -> Result<S::Ok, S::Error> {
        ser.collect_map(headers.keys().map(|k| (k.as_str(), ToSeq(headers, k))))
    }

    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum OneOrMore<'a> {
        One(Cow<'a, str>),
        Strings(Vec<Cow<'a, str>>),
        Bytes(Vec<Cow<'a, [u8]>>),
    }

    struct HeaderMapVisitor {
        is_human_readable: bool,
    }

    impl<'de> Visitor<'de> for HeaderMapVisitor {
        type Value = HeaderMap;

        // Format a message stating what data this Visitor expects to receive.
        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("lots of things can go wrong with HeaderMap")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut map = HeaderMap::with_capacity(access.size_hint().unwrap_or(0));

            if !self.is_human_readable {
                while let Some((key, arr)) = access.next_entry::<Cow<str>, Vec<Cow<[u8]>>>()? {
                    let key = HeaderName::from_bytes(key.as_bytes())
                        .map_err(|_| de::Error::invalid_value(Unexpected::Str(&key), &self))?;
                    for val in arr {
                        let val = HeaderValue::from_bytes(&val).map_err(|_| {
                            de::Error::invalid_value(Unexpected::Bytes(&val), &self)
                        })?;
                        map.append(key.clone(), val);
                    }
                }
            } else {
                while let Some((key, val)) = access.next_entry::<Cow<str>, OneOrMore>()? {
                    let key = HeaderName::from_bytes(key.as_bytes())
                        .map_err(|_| de::Error::invalid_value(Unexpected::Str(&key), &self))?;
                    match val {
                        OneOrMore::One(val) => {
                            let val = val.parse().map_err(|_| {
                                de::Error::invalid_value(Unexpected::Str(&val), &self)
                            })?;
                            map.insert(key, val);
                        }
                        OneOrMore::Strings(arr) => {
                            for val in arr {
                                let val = val.parse().map_err(|_| {
                                    de::Error::invalid_value(Unexpected::Str(&val), &self)
                                })?;
                                map.append(key.clone(), val);
                            }
                        }
                        OneOrMore::Bytes(arr) => {
                            for val in arr {
                                let val = HeaderValue::from_bytes(&val).map_err(|_| {
                                    de::Error::invalid_value(Unexpected::Bytes(&val), &self)
                                })?;
                                map.append(key.clone(), val);
                            }
                        }
                    };
                }
            }
            Ok(map)
        }
    }

    /// Implementation detail.
    pub fn deserialize<'de, D>(de: D) -> Result<HeaderMap, D::Error>
    where
        D: Deserializer<'de>,
    {
        let is_human_readable = de.is_human_readable();
        de.deserialize_map(HeaderMapVisitor { is_human_readable })
    }
}

#[cfg(test)]
mod tests {
    use ntex::http::header::{HeaderMap, HeaderName, HeaderValue};

    #[test]
    fn test_serialize_headers_map() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("hey"),
            HeaderValue::from_static("ho"),
        );
        headers.insert(
            HeaderName::from_static("foo"),
            HeaderValue::from_static("bar"),
        );
        headers.append(
            HeaderName::from_static("multi-value"),
            HeaderValue::from_static("multi"),
        );
        headers.append(
            HeaderName::from_static("multi-value"),
            HeaderValue::from_static("valued"),
        );

        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
        struct Wrap {
            #[serde(with = "super::header_map")]
            headers: HeaderMap,
        }

        let wrapped = Wrap { headers };
        let json = serde_json::to_string(&wrapped).unwrap();
        assert_eq!(
            r#"{"headers":{"foo":"bar","multi-value":["multi","valued"],"hey":"ho"}}"#,
            &json
        );

        let wrapped_back: Wrap = serde_json::from_str(&json).unwrap();
        assert_eq!(wrapped, wrapped_back);
    }
}

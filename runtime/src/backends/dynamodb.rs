use std::iter::FromIterator;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, env};

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    error::{DeleteItemError, GetItemError, PutItemError},
    model::AttributeValue,
    output::{DeleteItemOutput, GetItemOutput, PutItemOutput},
    Blob, Client, Endpoint, SdkError,
};
use core::borrow::BorrowMut;
use http::Uri;
use hyper::{HeaderMap, Response, StatusCode};

use crate::storage::{Item, ItemKey, Storage};

pub struct DynamodDbBackend {
    client: Client,
    cache_key_name: String,
    cache_resp_headers_name: String,
    cache_resp_body_name: String,
    cache_resp_status_name: String,
    cache_surrogate_keys_name: String,
    cache_creation_ts_name: String,
    cache_expiry_ts_name: String,
    sk_timestamp_name: String,
    cache_table_name: String,
}

impl DynamodDbBackend {
    pub async fn new() -> DynamodDbBackend {
        let config = aws_config::load_from_env().await;
        let client = if env::var("AWS_DEFAULT_REGION") == Ok("local-stack".into()) {
            // Setup a local-stack based client
            let endpoint = env::var("DYNAMO_ENDPOINT").unwrap();
            let endpoint = Endpoint::immutable(Uri::from_str(&endpoint).unwrap());
            let dynamodb_config = aws_sdk_dynamodb::config::Builder::from(&config)
                .endpoint_resolver(endpoint)
                .build();
            Client::from_conf(dynamodb_config)
        } else {
            Client::new(&config)
        };

        DynamodDbBackend {
            client,
            cache_key_name: String::from("key"),
            cache_resp_headers_name: String::from("response_headers"),
            cache_resp_body_name: String::from("response_body"),
            cache_resp_status_name: String::from("response_status_code"),
            cache_surrogate_keys_name: String::from("surrogate_keys"),
            cache_creation_ts_name: String::from("creation_timestamp"),
            cache_expiry_ts_name: String::from("expiry_timestamp"),
            sk_timestamp_name: String::from("sk_timestamp"),
            cache_table_name: env::var("DYNAMODB_TABLE")
                .unwrap_or_else(|_| String::from("casper_cache")),
        }
    }

    async fn get_item(
        &self,
        key: Vec<u8>,
        projection_expression: Option<String>,
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        let key = HashMap::from_iter([(
            self.cache_key_name.clone(),
            AttributeValue::B(Blob::new(key)),
        )]);

        self.client
            .get_item()
            .set_key(Some(key))
            .set_table_name(Some(self.cache_table_name.clone()))
            .set_projection_expression(projection_expression)
            .set_consistent_read(Some(true))
            .send()
            .await
    }

    async fn delete_item(
        &self,
        key: Vec<u8>,
    ) -> Result<DeleteItemOutput, SdkError<DeleteItemError>> {
        let key = HashMap::from_iter([(
            self.cache_key_name.clone(),
            AttributeValue::B(Blob::new(key)),
        )]);

        self.client
            .delete_item()
            .set_key(Some(key))
            .set_table_name(Some(self.cache_table_name.clone()))
            .send()
            .await
    }

    fn serialize_headers(&self, headers: &HeaderMap) -> Vec<u8> {
        // Serializes the headers in the flexbuffer format
        flexbuffers::to_vec(&hyper_serde::Ser::new(headers)).unwrap()
    }

    fn deserialize_headers(&self, data: &[u8]) -> HeaderMap {
        // Deserializes the headers from the flexbuffer format
        let headers: hyper_serde::De<HeaderMap> = flexbuffers::from_slice(data).unwrap();
        headers.into_inner()
    }

    async fn cache_response(
        &self,
        key: &[u8],
        response: &mut Response<hyper::Body>,
        surrogate_keys: Vec<Vec<u8>>,
        ttl_duration: Option<Duration>,
    ) -> Result<()> {
        // Get the response body, headers, etc
        let resp_body = hyper::body::to_bytes(response.body_mut())
            .await
            .unwrap()
            .to_vec();
        let headers = response.headers();

        // ttl expiry is Unix timestamp now + ttl
        let ttl_expire_ts = (SystemTime::now() + ttl_duration.unwrap())
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Add creation timestamp for response record
        let unix_ts_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set attribute values
        let av_key = AttributeValue::B(Blob::new(key));
        let av_resp_headers = AttributeValue::B(Blob::new(self.serialize_headers(headers)));
        let av_resp_body = AttributeValue::B(Blob::new(resp_body));
        let av_resp_status = AttributeValue::N(response.status().to_string());
        let av_sks = AttributeValue::L(
            surrogate_keys
                .iter()
                .map(|k| AttributeValue::B(Blob::new(k.clone())))
                .collect(),
        );
        let av_creation_ts = AttributeValue::N(unix_ts_now.to_string());
        let av_expiry_ts = AttributeValue::N(ttl_expire_ts.to_string());

        // Create a PutItem using above attribute values
        let mut item = HashMap::new();
        item.insert(self.cache_key_name.clone(), av_key);
        item.insert(self.cache_resp_headers_name.clone(), av_resp_headers);
        item.insert(self.cache_resp_body_name.clone(), av_resp_body);
        item.insert(self.cache_resp_status_name.clone(), av_resp_status);
        item.insert(self.cache_surrogate_keys_name.clone(), av_sks);
        item.insert(self.cache_creation_ts_name.clone(), av_creation_ts);
        item.insert(self.cache_expiry_ts_name.clone(), av_expiry_ts);

        self.client
            .put_item()
            .set_item(Some(item))
            .set_table_name(Some(self.cache_table_name.clone()))
            .send()
            .await?;

        // put / update the surrogate key items / records
        for sk in surrogate_keys {
            // Currently setting the DynamoDB expiry to now + 24 hours
            self.update_surrogate_item(sk, false).await?;
        }

        Ok(())
    }

    async fn update_surrogate_item(&self, key: Vec<u8>, invalidate: bool) -> Result<()> {
        // Check if surrogate_key already exists
        let sk_item_output = self
            .get_item(key.clone(), Some(self.sk_timestamp_name.to_string()))
            .await?;
        let sk_item = sk_item_output.item.as_ref();

        let mut sk_ts: u64;
        if sk_item.is_none() {
            // No surrogate key exists, set initial sk_timestamp to 0
            sk_ts = 0;
        } else {
            // If exists, store the existing sk timestamp value
            let sk_ts_av = sk_item.unwrap().get(&self.sk_timestamp_name).unwrap();
            sk_ts = sk_ts_av.as_n().unwrap().parse::<u64>().unwrap();
        }

        // Expiry timestamp set to now + 24 hours
        let exp_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 86400;

        // If the invalidate flag is set, sk_timestamp is overwritten to now()
        if invalidate {
            sk_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }

        self.put_surrogate_item(key, sk_ts, exp_ts).await?;

        Ok(())
    }

    async fn put_surrogate_item(
        &self,
        key: Vec<u8>,
        sk_ts: u64,
        exp_ts: u64,
    ) -> Result<PutItemOutput, SdkError<PutItemError>> {
        let av_key = AttributeValue::B(Blob::new(key));
        let av_timestamp = AttributeValue::N(sk_ts.to_string());
        let av_expiry_ts = AttributeValue::N(exp_ts.to_string());

        // Create a PutItem using above attribute values
        let mut item = HashMap::new();
        item.insert(self.cache_key_name.clone(), av_key);
        item.insert(self.sk_timestamp_name.clone(), av_timestamp);
        item.insert(self.cache_expiry_ts_name.clone(), av_expiry_ts);

        self.client
            .put_item()
            .set_item(Some(item))
            .set_table_name(Some(self.cache_table_name.clone()))
            .send()
            .await
    }

    async fn get_cached_response(&self, key: &[u8]) -> Option<Response<hyper::Body>> {
        // Filter on required values only
        let projection_expression = Some(format!(
            "{},{},{},{},{},{}",
            self.cache_resp_body_name,
            self.cache_resp_headers_name,
            self.cache_resp_status_name,
            self.cache_creation_ts_name,
            self.cache_expiry_ts_name,
            self.cache_surrogate_keys_name
        ));

        // Get the GetItemOutput object
        let item_output = self
            .get_item(key.to_vec(), projection_expression)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Error when calling get_item with key: {:?}",
                    hex::encode(key.to_vec())
                )
            });

        // If we don't get an item back from dynamoDB return None
        item_output.item.as_ref()?;

        let mut item = item_output.item.unwrap();

        // Get the response creation and TTL timestamps before doing anything else
        let av_creation_ts = item.get(&self.cache_creation_ts_name).unwrap();
        let creation_ts = av_creation_ts.as_n().unwrap().parse::<u64>().unwrap();

        // Because the dynamoDB item expiry can take some time, check the current time against the item TTL
        let av_expiry_ts = item.get(&self.cache_expiry_ts_name).unwrap();
        let av_expiry_ts_extracted = av_expiry_ts.as_n().unwrap().parse::<u64>().unwrap();
        let now_unix_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // If record is expired return None
        if now_unix_ts > av_expiry_ts_extracted {
            return None;
        }

        //Get the surrogate keys, as we need to check these first to see if the record is valid (or expired)
        let av_sks = item.get(&self.cache_surrogate_keys_name).unwrap();
        let sks_extracted = av_sks.as_l().unwrap().clone();
        let mut record_expired = false;

        for sk in sks_extracted {
            let sk = value_to_vec(sk);

            // Get the surrogate key record, filter on the timestamp value only
            let projection_expression = Some(self.sk_timestamp_name.to_string());

            let sk_item = self.get_item(sk, projection_expression).await.unwrap();
            let sk_item_avs = sk_item.item.as_ref()?;
            let av_sk_item_ts = sk_item_avs.get(&self.sk_timestamp_name)?;

            // Convert the record number string into a u64 value
            let sk_ts = av_sk_item_ts.as_n().unwrap().parse::<u64>().unwrap();

            if creation_ts < sk_ts {
                record_expired = true;
            }
        }

        if !record_expired {
            // Extract the response headers, body and status code
            let av_resp_headers = value_to_vec(item.remove(&self.cache_resp_headers_name)?);
            let av_resp_body = value_to_vec(item.remove(&self.cache_resp_body_name)?);
            let av_resp_status = item.get(&self.cache_resp_status_name)?.as_n().unwrap();

            // Deserialize the response headers
            let d_resp_headers = self.deserialize_headers(&av_resp_headers);

            // Convert the body into a hyper::Body
            let resp_hyper_body = hyper::Body::from(av_resp_body);

            // Construct a response object
            let mut constructed_resp = Response::builder().body(resp_hyper_body).unwrap();

            // Dereference the response headers and set the value to that of the deserialized headers
            *constructed_resp.headers_mut() = d_resp_headers;

            // Construct a StatusCode object and set it as the value for the Response status
            let status_code_u16: u16 = av_resp_status.parse().unwrap();
            let status = StatusCode::from_u16(status_code_u16).unwrap();
            *constructed_resp.status_mut() = status;
            Some(constructed_resp)
        } else {
            None
        }
    }
}

#[inline]
fn value_to_vec(value: AttributeValue) -> Vec<u8> {
    match value {
        AttributeValue::B(b) => b.into_inner(),
        _ => panic!("AttributeValue is not Blob"),
    }
}

#[async_trait]
impl Storage for DynamodDbBackend {
    type Body = hyper::Body;
    type Error = anyhow::Error;

    async fn get_responses<K, KI>(
        &self,
        keys: KI,
    ) -> Result<Vec<Option<Response<Self::Body>>>, Self::Error>
    where
        KI: IntoIterator<Item = K> + Send,
        <KI as IntoIterator>::IntoIter: Send,
        K: AsRef<[u8]> + Send,
    {
        let mut result = Vec::new();
        for key in keys {
            // Currently calling the single get cached response method per key, but eventually need to
            // implement bulk get items
            let key = key.as_ref();
            let resp = self.get_cached_response(key).await;
            result.push(resp);
        }
        Ok(result)
    }

    async fn delete_responses<K, KI>(&self, keys: KI) -> Result<(), Self::Error>
    where
        KI: IntoIterator<Item = ItemKey<K>> + Send,
        <KI as IntoIterator>::IntoIter: Send,
        K: AsRef<[u8]> + Send,
    {
        for key in keys {
            match key {
                ItemKey::Primary(key) => {
                    let d_key = key.as_ref().to_vec();
                    self.delete_item(d_key.clone()).await?;
                }
                ItemKey::Surrogate(sk) => {
                    let s_key = sk.as_ref().to_vec();
                    self.update_surrogate_item(s_key.clone(), true).await?;
                }
            }
        }
        Ok(())
    }

    async fn cache_responses<K, R, SK, I>(&self, items: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = Item<K, R, SK>> + Send,
        <I as IntoIterator>::IntoIter: Send,
        K: AsRef<[u8]> + Send,
        R: BorrowMut<Response<Self::Body>> + Send,
        SK: AsRef<[u8]> + Send,
    {
        for mut it in items {
            // Extract the item components
            let key = it.key.as_ref();
            let resp = it.response.borrow_mut();
            let ttl = it.ttl;
            let sk = it
                .surrogate_keys
                .into_iter()
                .map(|x| x.as_ref().to_vec())
                .collect();

            // Call cache response with extracted components
            let _res = self.cache_response(key, resp, sk, ttl).await;
            let _res = match _res {
                Ok(_res) => _res,
                Err(error) => panic!(
                    "Error when trying to cache item with key: {:?} error: {}",
                    hex::encode(key.to_vec()).as_str(),
                    error
                ),
            };
        }
        Ok(())
    }
}

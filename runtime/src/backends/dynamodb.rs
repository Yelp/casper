use std::borrow::BorrowMut;
use std::iter::FromIterator;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, env};

use anyhow::Result;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::{
    error::{DeleteItemError, GetItemError, PutItemError},
    model::{AttributeValue, KeysAndAttributes},
    output::{DeleteItemOutput, GetItemOutput, PutItemOutput},
    Blob, Client, Endpoint, SdkError,
};
use http::Uri;
use hyper::{HeaderMap, Response, StatusCode};
use serde::Deserialize;

use crate::storage::{Item, ItemKey, Key, Storage};

pub struct DynamodDbBackend {
    client: Client,
    config: Config,
}

// DynamoDB backend configuration
#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "Config::default_table_name")]
    pub table_name: String,
    #[serde(default)]
    pub fields: Fields,
    #[serde(default)]
    pub timeouts: TimeoutConfig,
    #[serde(default)]
    pub retries: RetryConfig,
}

#[derive(Debug, Deserialize)]
pub struct Fields {
    #[serde(default = "Fields::default_key")]
    pub key: String,
    #[serde(default = "Fields::default_headers")]
    pub headers: String,
    #[serde(default = "Fields::default_body")]
    pub body: String,
    #[serde(default = "Fields::default_status_code")]
    pub status_code: String,
    #[serde(default = "Fields::default_surrogate_keys")]
    pub surrogate_keys: String,

    // For cache items: last update time
    // For surrogate keys: last refresh time after which associated items are valid
    #[serde(default = "Fields::default_timestamp")]
    pub timestamp: String,
    #[serde(default = "Fields::default_expiry_timestamp")]
    pub expiry_timestamp: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct TimeoutConfig {
    /// A limit on the amount of time after making an initial connect attempt
    /// on a socket to complete the connect-handshake.
    pub connect_timeout: Option<f64>,

    /// A limit on the amount of time an application takes to attempt to read the first byte
    /// over an established, open connection after write request.
    pub read_timeout: Option<f64>,

    /// A limit on the amount of time it takes for request to complete.
    /// A single request may be comprised of several attemps depending on an app’s RetryConfig.
    pub request_timeout: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct RetryConfig {
    pub mode: RetryMode,
    pub max_attempts: u32,
}

#[derive(Debug, Deserialize)]
pub enum RetryMode {
    Standard,
    Adaptive,
}

macro_rules! string_methods {
    ($($method:ident($($arg:tt)*) => $value:expr,)+) => {
        #[inline(always)]
        $(fn $method($($arg)?) -> String {
            $value.to_string()
        })+
    };
}

impl Config {
    fn default_table_name() -> String {
        env::var("DYNAMODB_TABLE").unwrap_or_else(|_| "casper_cache".to_string())
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            table_name: Config::default_table_name(),
            fields: Fields::default(),
            timeouts: TimeoutConfig::default(),
            retries: RetryConfig::default(),
        }
    }
}

impl Fields {
    string_methods! {
        default_key() => "key",
        default_headers() => "headers",
        default_body() => "body",
        default_status_code() => "status_code",
        default_surrogate_keys() => "surrogate_keys",
        default_timestamp() => "tstamp",
        default_expiry_timestamp() => "expiry_timestamp",
    }
}

impl Default for Fields {
    fn default() -> Self {
        Fields {
            key: Fields::default_key(),
            headers: Fields::default_headers(),
            body: Fields::default_body(),
            status_code: Fields::default_status_code(),
            surrogate_keys: Fields::default_surrogate_keys(),
            timestamp: Fields::default_timestamp(),
            expiry_timestamp: Fields::default_expiry_timestamp(),
        }
    }
}

impl TimeoutConfig {
    fn aws_timeout_config(&self) -> aws_smithy_types::timeout::TimeoutConfig {
        aws_smithy_types::timeout::TimeoutConfig::new()
            .with_connect_timeout(self.connect_timeout.map(Duration::from_secs_f64))
            .with_read_timeout(self.read_timeout.map(Duration::from_secs_f64))
            .with_api_call_timeout(self.request_timeout.map(Duration::from_secs_f64))
    }
}

impl RetryConfig {
    fn aws_retry_config(&self) -> aws_smithy_types::retry::RetryConfig {
        aws_smithy_types::retry::RetryConfig::new()
            .with_max_attempts(self.max_attempts)
            .with_retry_mode(match self.mode {
                RetryMode::Standard => aws_smithy_types::retry::RetryMode::Standard,
                RetryMode::Adaptive => aws_smithy_types::retry::RetryMode::Adaptive,
            })
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            mode: RetryMode::Standard,
            max_attempts: 3,
        }
    }
}

impl DynamodDbBackend {
    #[allow(unused)]
    pub async fn new(config: Config) -> DynamodDbBackend {
        let region_provider = RegionProviderChain::default_provider();
        let aws_config = aws_config::from_env()
            .region(region_provider)
            .timeout_config(config.timeouts.aws_timeout_config())
            .retry_config(config.retries.aws_retry_config())
            .load()
            .await;

        let client = if env::var("AWS_DEFAULT_REGION") == Ok("local-stack".into()) {
            // Setup a local-stack based client
            let endpoint = env::var("DYNAMODB_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:4566".to_string());
            let endpoint = Endpoint::immutable(Uri::from_str(&endpoint).unwrap());
            let dynamodb_config = aws_sdk_dynamodb::config::Builder::from(&aws_config)
                .endpoint_resolver(endpoint)
                .build();
            Client::from_conf(dynamodb_config)
        } else {
            Client::new(&aws_config)
        };

        DynamodDbBackend { client, config }
    }

    async fn get_item(
        &self,
        key: &[u8],
        projection_expression: Option<String>,
    ) -> Result<GetItemOutput, SdkError<GetItemError>> {
        let key = HashMap::from_iter([(self.key_field(), av_bin(key))]);

        self.client
            .get_item()
            .set_key(Some(key))
            .set_table_name(Some(self.table_name()))
            .set_projection_expression(projection_expression)
            .send()
            .await
    }

    async fn delete_item(&self, key: &[u8]) -> Result<DeleteItemOutput, SdkError<DeleteItemError>> {
        let key = HashMap::from_iter([(self.key_field(), av_bin(key))]);

        self.client
            .delete_item()
            .set_key(Some(key))
            .set_table_name(Some(self.table_name()))
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
        surrogate_keys: &[impl AsRef<[u8]>],
        ttl_duration: Option<Duration>,
    ) -> Result<()> {
        // Get the response body, headers, etc
        let resp_body = hyper::body::to_bytes(response.body_mut())
            .await
            .unwrap()
            .to_vec();
        let serialized_headers = self.serialize_headers(response.headers());

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

        // Create a PutItem using above attribute values
        let mut item = HashMap::new();
        item.insert(self.key_field(), av_bin(key));
        item.insert(self.headers_field(), av_bin(serialized_headers));
        item.insert(self.body_field(), av_bin(resp_body));
        item.insert(self.status_code_field(), av_num(response.status().as_u16()));
        item.insert(self.surrogate_keys_field(), av_bin_list(surrogate_keys));
        item.insert(self.timestamp_field(), av_num(unix_ts_now));
        item.insert(self.expiry_timestamp_field(), av_num(ttl_expire_ts));

        self.client
            .put_item()
            .set_item(Some(item))
            .set_table_name(Some(self.table_name()))
            .send()
            .await?;

        // put / update the surrogate key items / records
        for sk in surrogate_keys {
            // Currently setting the DynamoDB expiry to now + 24 hours
            self.update_surrogate_item(sk.as_ref(), false).await?;
        }

        Ok(())
    }

    async fn update_surrogate_item(&self, key: &[u8], invalidate: bool) -> Result<()> {
        // Check if surrogate_key already exists
        let sk_item_output = self.get_item(key, Some(self.timestamp_field())).await?;
        let sk_item = sk_item_output.item.as_ref();

        let mut sk_ts: u64;
        if sk_item.is_none() {
            // No surrogate key exists, set initial sk_timestamp to 0
            sk_ts = 0;
        } else {
            // If exists, store the existing sk timestamp value
            let sk_ts_av = sk_item.unwrap().get(&self.timestamp_field()).unwrap();
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
        key: &[u8],
        sk_ts: u64,
        exp_ts: u64,
    ) -> Result<PutItemOutput, SdkError<PutItemError>> {
        let mut item = HashMap::new();
        item.insert(self.key_field(), av_bin(key));
        item.insert(self.timestamp_field(), av_num(sk_ts));
        item.insert(self.expiry_timestamp_field(), av_num(exp_ts));

        self.client
            .put_item()
            .set_item(Some(item))
            .set_table_name(Some(self.table_name()))
            .send()
            .await
    }

    async fn get_cached_response(&self, key: &[u8]) -> Option<Response<hyper::Body>> {
        // Filter on required values only
        let projection_expression = Some(format!(
            "{},{},{},{},{},{}",
            self.body_field(),
            self.headers_field(),
            self.status_code_field(),
            self.timestamp_field(),
            self.expiry_timestamp_field(),
            self.surrogate_keys_field(),
        ));

        // Get the GetItemOutput object
        let item_output = self
            .get_item(key, projection_expression)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "Error when calling get_item with key {:?}: {}",
                    hex::encode(key),
                    err
                )
            });

        // If we don't get an item back from dynamoDB return None
        item_output.item.as_ref()?;

        let mut item = item_output.item.unwrap();

        // Get the response creation and TTL timestamps before doing anything else
        let av_creation_ts = item.get(&self.timestamp_field()).unwrap();
        let creation_ts = av_creation_ts.as_n().unwrap().parse::<u64>().unwrap();

        // Because the dynamoDB item expiry can take some time, check the current time against the item TTL
        let av_expiry_ts = item.get(&self.expiry_timestamp_field()).unwrap();
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
        let av_sks = item.get(&self.surrogate_keys_field()).unwrap();
        let sks_extracted = av_sks.as_l().unwrap().clone();
        let mut record_expired = false;

        let skeys_fetched = self
            .client
            .batch_get_item()
            .request_items(
                self.table_name(),
                KeysAndAttributes::builder()
                    .set_keys(Some(
                        sks_extracted
                            .into_iter()
                            .map(|sk| HashMap::from_iter([(self.key_field(), sk)]))
                            .collect(),
                    ))
                    .set_projection_expression(Some(self.timestamp_field()))
                    .build(),
            )
            .send()
            .await
            .unwrap()
            .responses?;

        for skey_item in &skeys_fetched[&self.table_name()] {
            let timestamp = skey_item.get(&self.timestamp_field())?;
            let timestamp = timestamp.as_n().unwrap().parse::<u64>().unwrap();
            if creation_ts < timestamp {
                record_expired = true;
                break;
            }
        }

        if !record_expired {
            // Extract the response headers, body and status code
            let av_resp_headers = av_bin_to_vec(item.remove(&self.headers_field())?);
            let av_resp_body = av_bin_to_vec(item.remove(&self.body_field())?);
            let av_resp_status = item.get(&self.status_code_field())?.as_n().unwrap();

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

    string_methods! {
        table_name(&self) => self.config.table_name,
        key_field(&self) => self.config.fields.key,
        headers_field(&self) => self.config.fields.headers,
        body_field(&self) => self.config.fields.body,
        status_code_field(&self) => self.config.fields.status_code,
        surrogate_keys_field(&self) => self.config.fields.surrogate_keys,
        timestamp_field(&self) => self.config.fields.timestamp,
        expiry_timestamp_field(&self) => self.config.fields.expiry_timestamp,
    }
}

#[async_trait]
impl Storage for DynamodDbBackend {
    type Body = hyper::Body;
    type Error = anyhow::Error;

    async fn get_responses<KI>(
        &self,
        keys: KI,
    ) -> Result<Vec<Option<Response<Self::Body>>>, Self::Error>
    where
        KI: IntoIterator<Item = Key> + Send,
        <KI as IntoIterator>::IntoIter: Send,
    {
        let mut result = Vec::new();
        for key in keys {
            // Currently calling the single get cached response method per key, but eventually need to
            // implement bulk get items
            let resp = self.get_cached_response(&key).await;
            result.push(resp);
        }
        Ok(result)
    }

    async fn delete_responses<KI>(&self, keys: KI) -> Result<(), Self::Error>
    where
        KI: IntoIterator<Item = ItemKey> + Send,
        <KI as IntoIterator>::IntoIter: Send,
    {
        for key in keys {
            match key {
                ItemKey::Primary(key) => {
                    self.delete_item(&key).await?;
                }
                ItemKey::Surrogate(sk) => {
                    self.update_surrogate_item(&sk, true).await?;
                }
            }
        }
        Ok(())
    }

    async fn cache_responses<R, I>(&self, items: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = Item<R>> + Send,
        <I as IntoIterator>::IntoIter: Send,
        R: BorrowMut<Response<Self::Body>> + Send,
    {
        for mut it in items {
            // Extract the item components
            let key = it.key.as_ref();
            let resp = it.response.borrow_mut();
            let ttl = it.ttl;

            // Call cache response with extracted components
            let _res = self
                .cache_response(key, resp, &it.surrogate_keys, ttl)
                .await;
            let _res = match _res {
                Ok(_res) => _res,
                Err(error) => panic!(
                    "Error when trying to cache item with key {:?}: {}",
                    hex::encode(key).as_str(),
                    error
                ),
            };
        }
        Ok(())
    }
}

#[inline]
fn av_bin(value: impl Into<Vec<u8>>) -> AttributeValue {
    AttributeValue::B(Blob::new(value))
}

#[inline]
fn av_num(value: impl ToString) -> AttributeValue {
    AttributeValue::N(value.to_string())
}

#[inline]
fn av_bin_list(items: &[impl AsRef<[u8]>]) -> AttributeValue {
    AttributeValue::L(items.iter().map(|k| av_bin(k.as_ref())).collect())
}

#[inline]
fn av_bin_to_vec(value: AttributeValue) -> Vec<u8> {
    match value {
        AttributeValue::B(b) => b.into_inner(),
        _ => panic!("AttributeValue is not 'B''"),
    }
}

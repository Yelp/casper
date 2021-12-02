use crate::storage::{Item, ItemKey, Storage};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use core::borrow::BorrowMut;
use hyper::{HeaderMap, Response, StatusCode};
use rusoto_core::{Region, RusotoError};
use rusoto_dynamodb::{
    AttributeValue, DeleteItemError, DeleteItemInput, DeleteItemOutput, DynamoDb, DynamoDbClient,
    GetItemError, GetItemInput, GetItemOutput, PutItemError, PutItemInput, PutItemOutput,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, env};

pub struct DynamodDbBackend {
    connector: DynamoDbClient,
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

impl Default for DynamodDbBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamodDbBackend {
    pub fn new() -> DynamodDbBackend {
        let connector;

        // Setup a local-stack based client
        if env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "".to_string()) == "local-stack" {
            connector = DynamoDbClient::new(Region::Custom {
                name: env::var("AWS_DEFAULT_REGION").unwrap(),
                endpoint: env::var("DYNAMO_ENDPOINT").unwrap(),
            });

        // Setup an AWS based client
        } else {
            connector = DynamoDbClient::new(Region::default());
        }

        DynamodDbBackend {
            connector,
            cache_key_name: String::from("key"),
            cache_resp_headers_name: String::from("response_headers"),
            cache_resp_body_name: String::from("response_body"),
            cache_resp_status_name: String::from("response_status_code"),
            cache_surrogate_keys_name: String::from("surrogate_keys"),
            cache_creation_ts_name: String::from("creation_timestamp"),
            cache_expiry_ts_name: String::from("expiry_timestamp"),
            sk_timestamp_name: String::from("sk_timestamp"),
            cache_table_name: env::var("DYNAMO_TABLE")
                .unwrap_or_else(|_| String::from("casper_cache")),
        }
    }

    async fn get_item(
        &self,
        key: Vec<u8>,
        projection_expression: Option<String>,
    ) -> Result<GetItemOutput, RusotoError<GetItemError>> {
        let attribute_value_key = AttributeValue {
            b: Option::Some(Bytes::from(key)),
            ..Default::default()
        };

        let mut get_item = HashMap::new();
        get_item.insert(self.cache_key_name.clone(), attribute_value_key);

        let get_item_input = GetItemInput {
            key: get_item,
            table_name: self.cache_table_name.clone(),
            projection_expression,
            consistent_read: Some(true),
            ..Default::default()
        };

        self.connector.get_item(get_item_input).await
    }

    async fn delete_item(
        &self,
        key: Vec<u8>,
    ) -> Result<DeleteItemOutput, RusotoError<DeleteItemError>> {
        let attribute_value_key = AttributeValue {
            b: Option::Some(Bytes::from(key)),
            ..Default::default()
        };
        let mut delete_item = HashMap::new();
        delete_item.insert(self.cache_key_name.clone(), attribute_value_key);

        let delete_item_input = DeleteItemInput {
            table_name: self.cache_table_name.clone(),
            key: delete_item,
            ..Default::default()
        };
        self.connector.delete_item(delete_item_input).await
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
        // Get the response body, serialize headers etc
        let resp_body = hyper::body::to_bytes(response.body_mut())
            .await
            .unwrap()
            .to_vec();
        let headers = response.headers();
        let s_resp_headers = self.serialize_headers(headers);
        let status = response.status().as_u16();

        // ttl expiry is Unix timestamp now + ttl
        let ttl_expire_ts = (SystemTime::now() + ttl_duration.unwrap())
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create the attribute values for the put item
        let mut av_key: AttributeValue = Default::default();
        let mut av_resp_headers: AttributeValue = Default::default();
        let mut av_resp_body: AttributeValue = Default::default();
        let mut av_resp_status: AttributeValue = Default::default();
        let mut av_sks: AttributeValue = Default::default();
        let mut av_creation_ts: AttributeValue = Default::default();
        let mut av_expiry_ts: AttributeValue = Default::default();

        // Create a vector of attributes for surrogate_keys
        let mut sk_av_vec: Vec<AttributeValue> = vec![];
        for sk in surrogate_keys.clone() {
            let av_sk = AttributeValue {
                b: Option::Some(Bytes::from(sk)),
                ..Default::default()
            };
            sk_av_vec.push(av_sk);
        }

        // Add creation timestamp for response record
        let unix_ts_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set attribute value contents
        av_key.b = Option::Some(Bytes::from(key.to_vec()));
        av_resp_headers.b = Option::Some(Bytes::from(s_resp_headers));
        av_resp_body.b = Option::Some(Bytes::from(resp_body));
        av_resp_status.n = Option::Some(status.to_string());
        av_sks.l = Option::Some(sk_av_vec);
        av_creation_ts.n = Option::Some(unix_ts_now.to_string());
        av_expiry_ts.n = Option::Some(ttl_expire_ts.to_string());

        // Create a PutItem using above attribute values
        let mut item = HashMap::new();
        item.insert(self.cache_key_name.clone(), av_key);
        item.insert(self.cache_resp_headers_name.clone(), av_resp_headers);
        item.insert(self.cache_resp_body_name.clone(), av_resp_body);
        item.insert(self.cache_resp_status_name.clone(), av_resp_status);
        item.insert(self.cache_surrogate_keys_name.clone(), av_sks);
        item.insert(self.cache_creation_ts_name.clone(), av_creation_ts);
        item.insert(self.cache_expiry_ts_name.clone(), av_expiry_ts);

        let put_item = PutItemInput {
            item,
            table_name: self.cache_table_name.clone(),
            ..Default::default()
        };

        // put the response item / records
        self.connector.put_item(put_item).await?;

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
            sk_ts = sk_ts_av.n.clone().unwrap().parse::<u64>().unwrap();
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
    ) -> Result<PutItemOutput, RusotoError<PutItemError>> {
        let mut av_key: AttributeValue = Default::default();
        let mut av_timestamp: AttributeValue = Default::default();
        let mut av_expiry_ts: AttributeValue = Default::default();

        av_key.b = Option::Some(Bytes::from(key.to_vec()));
        av_timestamp.n = Option::Some(sk_ts.to_string());
        av_expiry_ts.n = Option::Some(exp_ts.to_string());

        // Create a PutItem using above attribute values
        let mut item = HashMap::new();
        item.insert(self.cache_key_name.clone(), av_key);
        item.insert(self.sk_timestamp_name.clone(), av_timestamp);
        item.insert(self.cache_expiry_ts_name.clone(), av_expiry_ts);

        let put_item = PutItemInput {
            item,
            table_name: self.cache_table_name.clone(),
            ..Default::default()
        };

        self.connector.put_item(put_item).await
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

        let item = item_output.item.unwrap();

        // Get the response creation and TTL timestamps before doing anything else
        let av_creation_ts = item.get(&self.cache_creation_ts_name).unwrap();
        let creation_ts = av_creation_ts.n.clone()?.parse::<u64>().unwrap();

        // Because the dynamoDB item expiry can take some time, check the current time against the item TTL
        let av_expiry_ts = item.get(&self.cache_expiry_ts_name).unwrap();
        let av_expiry_ts_extracted = av_expiry_ts.n.clone()?.parse::<u64>().unwrap();
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
        let sks_extracted = av_sks.l.clone().unwrap();
        let mut record_expired = false;

        for sk in sks_extracted {
            let sk = sk.b.unwrap().to_vec();

            // Get the surrogate key record, filter on the timestamp value only
            let projection_expression = Some(self.sk_timestamp_name.to_string());

            let sk_item = self.get_item(sk, projection_expression).await.unwrap();
            let sk_item_avs = sk_item.item.as_ref()?;
            let av_sk_item_ts = sk_item_avs.get(&self.sk_timestamp_name)?;

            // Convert the record number string into a u64 value
            let sk_ts = av_sk_item_ts.n.clone()?.parse::<u64>().unwrap();

            if creation_ts < sk_ts {
                record_expired = true;
            }
        }

        if !record_expired {
            // Extract the response headers, body and status code
            let av_resp_headers = item.get(&self.cache_resp_headers_name)?;
            let av_resp_body = item.get(&self.cache_resp_body_name)?;
            let av_resp_status = item.get(&self.cache_resp_status_name)?;

            let resp_headers_extracted = av_resp_headers.b.as_ref();
            let resp_body_extracted = av_resp_body.b.clone()?;
            let resp_status_extracted = av_resp_status.clone().n?;

            // Deserialize the response headers
            let resp_headers_vec = resp_headers_extracted.unwrap().as_ref();
            let d_resp_headers = self.deserialize_headers(resp_headers_vec);

            // Convert the body into a hyper::Body
            let resp_hyper_body = hyper::Body::from(resp_body_extracted);

            // Construct a response object
            let mut constructed_resp = Response::builder().body(resp_hyper_body).unwrap();

            // Dereference the response headers and set the value to that of the deserialized headers
            *constructed_resp.headers_mut() = d_resp_headers;

            // Construct a StatusCode object and set it as the value for the Response status
            let status_code_u16: u16 = resp_status_extracted.parse().unwrap();
            let status = StatusCode::from_u16(status_code_u16).unwrap();
            *constructed_resp.status_mut() = status;
            Some(constructed_resp)
        } else {
            None
        }
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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use once_cell::sync::Lazy;
use serde_yaml::Value as YamlValue;
use tokio::fs;
use tokio::sync::RwLock;

static CONFIGS: Lazy<RwLock<HashMap<PathBuf, Value>>> = Lazy::new(Default::default);

#[derive(Debug, Clone)]
struct Value {
    yaml: YamlValue,
    last_checked: Instant,
    modified: SystemTime,
    interval: Duration,
}

pub enum IndexKey {
    None,
    Usize(usize),
    String(String),
}

pub async fn get_config<P>(
    config: &P,
    keys: &[IndexKey],
    bypass_cache: Option<bool>,
    interval: Option<Duration>,
) -> anyhow::Result<Option<YamlValue>>
where
    P: AsRef<Path> + ?Sized,
{
    let config = config.as_ref();

    if bypass_cache.unwrap_or(false) {
        let yaml = serde_yaml::from_slice(&fs::read(config).await?)?;
        return Ok(traverse_value(&yaml, keys).cloned());
    }

    match CONFIGS.read().await.get(config) {
        Some(value) if value.last_checked + value.interval > Instant::now() => {
            // Use cache
            Ok(traverse_value(&value.yaml, keys).cloned())
        }
        Some(_) => {
            // Check file modification time and optionally refresh the cache
            let meta = fs::metadata(config).await?;
            let modified = meta.modified()?;
            let mut configs = CONFIGS.write().await;
            let value = configs
                .get_mut(config)
                .expect("disappeared config from cache");
            if modified != value.modified {
                // Refresh cache
                value.yaml = serde_yaml::from_slice(&fs::read(config).await?)?;
                value.modified = modified;
            }
            value.last_checked = Instant::now();
            Ok(traverse_value(&value.yaml, keys).cloned())
        }
        None => {
            let modified = fs::metadata(config).await?.modified()?;
            let value = Value {
                yaml: serde_yaml::from_slice(&fs::read(config).await?)?,
                last_checked: Instant::now(),
                modified,
                interval: interval.unwrap_or(Duration::from_secs(1)),
            };
            CONFIGS.write().await.insert(config.into(), value.clone());
            Ok(traverse_value(&value.yaml, keys).cloned())
        }
    }
}

fn traverse_value<'a>(value: &'a YamlValue, keys: &[IndexKey]) -> Option<&'a YamlValue> {
    let next_value = match keys.get(0) {
        Some(IndexKey::Usize(i)) => value.get(i),
        Some(IndexKey::String(s)) => value.get(s),
        Some(IndexKey::None) | None => return Some(value),
    };
    match next_value {
        Some(value) => traverse_value(value, &keys[1..]),
        None => None,
    }
}

pub mod lua {
    use mlua::{ExternalError, ExternalResult, Lua, LuaSerdeExt, Result, SerializeOptions, Value};

    pub async fn get_config<'a>(
        lua: &'a Lua,
        (config, keys): (String, Option<Vec<Value<'a>>>),
    ) -> Result<Value<'a>> {
        let keys = keys
            .unwrap_or_default()
            .into_iter()
            .map(|k| match k {
                Value::Nil => Ok(super::IndexKey::None),
                Value::Integer(i) if i >= 0 => Ok(super::IndexKey::Usize(i as usize)),
                Value::Integer(i) => Ok(super::IndexKey::String(i.to_string())),
                Value::String(s) => Ok(super::IndexKey::String(s.to_string_lossy().to_string())),
                _ => Err(format!("invalid key: {}", k.type_name()).to_lua_err()),
            })
            .collect::<Result<Vec<_>>>()?;

        let value = super::get_config(&config, &keys, None, None)
            .await
            .to_lua_err()?;

        let options = SerializeOptions::new()
            .serialize_none_to_null(false)
            .set_array_metatable(false)
            .serialize_unit_to_null(false);

        lua.to_value_with(&value, options)
    }
}

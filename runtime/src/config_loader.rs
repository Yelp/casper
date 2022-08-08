use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use once_cell::sync::Lazy;
use serde_yaml::Value as YamlValue;
use tokio::{fs, sync::RwLock};

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

pub async fn get_config(
    config: impl AsRef<Path>,
    keys: &[IndexKey],
    bypass_cache: Option<bool>,
    interval: Option<Duration>,
) -> anyhow::Result<Option<YamlValue>> {
    let config = config.as_ref();

    if bypass_cache.unwrap_or(false) {
        let yaml = serde_yaml::from_slice(&fs::read(config).await?)?;
        return Ok(traverse_value(&yaml, keys).cloned());
    }

    let configs_guard = CONFIGS.read().await;
    match configs_guard.get(config) {
        Some(value) if value.last_checked + value.interval > Instant::now() => {
            // Use cache
            Ok(traverse_value(&value.yaml, keys).cloned())
        }
        Some(_) => {
            // Check file modification time and optionally refresh the cache
            drop(configs_guard); // To release the lock
            let meta = fs::metadata(config).await?;
            let modified = meta.modified()?;
            let mut configs_guard = CONFIGS.write().await;
            let value = configs_guard
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
            drop(configs_guard); // To release the lock

            let modified = fs::metadata(config).await?.modified()?;
            let value = Value {
                yaml: serde_yaml::from_slice(&fs::read(config).await?)?,
                last_checked: Instant::now(),
                modified,
                interval: interval.unwrap_or_else(|| Duration::from_secs(1)),
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
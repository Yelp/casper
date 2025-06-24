use std::collections::HashMap;
use std::env;
use std::fs;
use std::os::unix::prelude::OsStrExt;
use std::path::Path;

use anyhow::Result;
use mlua::{Lua, LuaSerdeExt, Value};
use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub main: MainConfig,
    #[serde(default)]
    pub http: HttpConfig,
    pub metrics: Option<MetricsConfig>,
    pub tracing: Option<TracingConfig>,
    #[serde(default)]
    pub storage: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MainConfig {
    #[serde(default)]
    pub service_name: Option<String>,

    #[serde(default = "MainConfig::default_workers")]
    pub workers: usize,

    #[serde(default = "MainConfig::default_listen")]
    pub listen: String,

    #[serde(default)]
    pub max_background_tasks: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct HttpConfig {
    pub filters: Vec<Filter>,
    pub handler: Option<LuaCode>,
    pub access_log: Option<LuaCode>,
    pub error_log: Option<LuaCode>,
}

#[derive(Debug, Deserialize)]
pub struct Filter {
    pub name: String,
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct LuaCode {
    pub code: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricsConfig {
    pub counters: Option<HashMap<String, MetricCounterConfig>>,
    pub extra_labels: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricCounterConfig {
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TracingConfig {
    #[serde(default)]
    pub enabled: bool,
    pub start_new_traces: Option<bool>,
    pub collector_endpoint: Option<String>,
    pub sampler: Option<String>, // Can be "AlwaysOn", "AlwaysOff", "ParentBased", "SilentOn"
    pub propagators: Option<Vec<String>>, // Supported values: "zipkin", "w3c"
}

pub(crate) fn read_config<P: AsRef<Path> + ?Sized>(path: &P) -> Result<Config> {
    let data = fs::read(path.as_ref())?;
    match path.as_ref().file_name() {
        Some(name) if name.as_bytes().ends_with(b".lua") => {
            let lua = Lua::new();
            configure_lua(&lua)?;
            let mut chunk = lua.load(&data);
            if let Some(name) = path.as_ref().to_str() {
                chunk = chunk.set_name(name);
            }
            let config = lua.from_value::<Config>(chunk.eval::<Value>()?)?;
            Ok(config)
        }
        Some(name) if name.as_bytes().ends_with(b".json") => Ok(serde_json::from_slice(&data)?),
        _ => Ok(serde_yaml::from_slice(&data)?),
    }
}

impl Default for MainConfig {
    fn default() -> Self {
        MainConfig {
            service_name: None,
            workers: Self::default_workers(),
            listen: Self::default_listen(),
            max_background_tasks: None,
        }
    }
}

impl MainConfig {
    fn default_workers() -> usize {
        num_cpus::get()
    }

    fn default_listen() -> String {
        "127.0.0.1:8080".to_string()
    }
}

fn configure_lua(lua: &Lua) -> Result<()> {
    let globals = lua.globals();
    globals.set(
        "getenv",
        lua.create_function(|_, key: String| Ok(env::var(key).ok()))?,
    )?;
    globals.set(
        "setenv",
        lua.create_function(|_, (key, val): (String, Option<String>)| {
            match val {
                Some(val) => env::set_var(key, val),
                None => env::remove_var(key),
            };
            Ok(())
        })?,
    )?;
    Ok(())
}

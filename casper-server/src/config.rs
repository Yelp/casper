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
    pub http: HttpConfig,
    pub metrics: Option<MetricsConfig>,
    pub storage: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MainConfig {
    #[serde(default = "MainConfig::default_workers")]
    pub workers: usize,

    #[serde(default)]
    pub pin_workers: bool,

    #[serde(default = "MainConfig::default_listen")]
    pub listen: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct HttpConfig {
    pub middleware: Vec<Middleware>,
    pub access_log: Option<AccessLog>,
    pub error_log: Option<ErrorLog>,
}

#[derive(Debug, Deserialize)]
pub struct Middleware {
    pub name: String,
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct AccessLog {
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct ErrorLog {
    pub code: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricsConfig {
    pub counters: HashMap<String, MetricCounterConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MetricCounterConfig {
    pub name: Option<String>,
    pub description: Option<String>,
}

pub(crate) fn read_config<P: AsRef<Path> + ?Sized>(path: &P) -> Result<Config> {
    let data = fs::read(path.as_ref())?;
    match path.as_ref().file_name() {
        Some(name) if name.as_bytes().ends_with(b".lua") => {
            let lua = Lua::new();
            configure_lua(&lua)?;
            let mut chunk = lua.load(&data);
            if let Some(name) = path.as_ref().to_str() {
                chunk = chunk.set_name(name)?;
            }
            let config = lua.from_value::<Config>(chunk.eval::<Value>()?)?;
            Ok(config)
        }
        _ => Ok(serde_yaml::from_slice(&data)?),
    }
}

impl Default for MainConfig {
    fn default() -> Self {
        MainConfig {
            workers: Self::default_workers(),
            pin_workers: false,
            listen: Self::default_listen(),
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
    Ok(())
}
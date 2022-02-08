use std::collections::HashMap;
use std::fs;
use std::os::unix::prelude::OsStrExt;
use std::path::Path;

use anyhow::Result;
use mlua::{Lua, LuaSerdeExt, Value};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub main: MainConfig,
    pub http: HttpConfig,
    pub storage: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MainConfig {
    #[serde(default = "MainConfig::default_worker_threads")]
    pub worker_threads: usize,

    #[serde(default)]
    pub pin_worker_threads: bool,

    #[serde(default = "MainConfig::default_listen")]
    pub listen: String,
}

#[derive(Debug, Deserialize)]
pub struct HttpConfig {
    pub middleware: Vec<Middleware>,
    pub access_log: Option<AccessLog>,
    pub error_log: Option<ErrorLog>,
}

#[derive(Debug, Deserialize)]
pub struct Middleware {
    pub name: Option<String>,
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

pub(crate) fn read_config<P: AsRef<Path> + ?Sized>(path: &P) -> Result<Config> {
    let data = fs::read(path.as_ref())?;
    match path.as_ref().file_name() {
        Some(name) if name.as_bytes().ends_with(b".lua") => {
            let lua = Lua::new();
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
            worker_threads: Self::default_worker_threads(),
            pin_worker_threads: false,
            listen: Self::default_listen(),
        }
    }
}

impl MainConfig {
    fn default_worker_threads() -> usize {
        num_cpus::get()
    }

    fn default_listen() -> String {
        "127.0.0.1:8080".to_string()
    }
}

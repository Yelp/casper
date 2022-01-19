use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub main: MainConfig,
    pub middleware: Vec<Middleware>,
    pub storage: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MainConfig {
    #[serde(default = "MainConfig::default_worker_threads")]
    pub worker_threads: usize,

    #[serde(default = "MainConfig::default_listen")]
    pub listen: String,
}

#[derive(Debug, Deserialize)]
pub struct Middleware {
    pub code: String,
}

pub(crate) fn read_config<P: AsRef<Path> + ?Sized>(path: &P) -> Result<Config> {
    let data = fs::read(path.as_ref())?;
    Ok(serde_yaml::from_slice(&data)?)
}

impl Default for MainConfig {
    fn default() -> Self {
        MainConfig {
            worker_threads: Self::default_worker_threads(),
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

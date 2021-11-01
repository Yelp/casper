#![allow(dead_code)]

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
    pub storage: HashMap<String, toml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct MainConfig {
    #[serde(default = "MainConfig::default_num_threads")]
    pub num_threads: usize,

    #[serde(default = "MainConfig::default_listen")]
    pub listen: String,
}

#[derive(Debug, Deserialize)]
pub struct Middleware {
    pub code: String,
}

pub(crate) fn read_config<P: AsRef<Path> + ?Sized>(path: &P) -> Result<Config> {
    let data = fs::read(path.as_ref())?;
    toml::from_slice(&data).map_err(|err| err.into())
}

impl Default for MainConfig {
    fn default() -> Self {
        MainConfig {
            num_threads: Self::default_num_threads(),
            listen: Self::default_listen(),
        }
    }
}

impl MainConfig {
    fn default_num_threads() -> usize {
        num_cpus::get()
    }

    fn default_listen() -> String {
        "127.0.0.1:8080".to_string()
    }
}

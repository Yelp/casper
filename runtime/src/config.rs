use std::fs;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub main: Option<MainConfig>,
    pub middleware: Vec<Middleware>,
}

#[derive(Debug, Deserialize)]
pub struct MainConfig {
    pub num_threads: usize,
}

#[derive(Debug, Deserialize)]
pub struct Middleware {
    pub code: String,
}

pub(crate) fn read_config<P: AsRef<Path> + ?Sized>(path: &P) -> Result<Config> {
    let data = fs::read(path.as_ref())?;
    toml::from_slice(&data).map_err(|err| err.into())
}

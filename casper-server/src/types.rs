use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::Deref;
use std::sync::Arc;

use hyper::client::{Client as HttpClient, HttpConnector};
use hyper_tls::HttpsConnector;
use mlua::{Lua, RegistryKey, Table};

#[derive(Clone, Copy)]
pub(crate) struct RemoteAddr(pub(crate) SocketAddr);

impl Default for RemoteAddr {
    fn default() -> Self {
        RemoteAddr(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0))
    }
}

#[derive(Clone)]
pub(crate) struct LuaContext(pub(crate) Arc<RegistryKey>);

impl Deref for LuaContext {
    type Target = RegistryKey;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl LuaContext {
    pub(crate) fn new(lua: &Lua) -> Self {
        let ctx = lua
            .create_table()
            .expect("Failed to create Lua context table");
        let key = lua
            .create_registry_value(ctx)
            .map(Arc::new)
            .expect("Failed to store Lua context table in the registry");
        LuaContext(key)
    }

    pub(crate) fn get<'lua>(&self, lua: &'lua Lua) -> Table<'lua> {
        lua.registry_value::<Table>(&self.0)
            .expect("Unable to get Lua context table from the registry")
    }
}

type WrappedHttpClient = HttpClient<HttpsConnector<HttpConnector>>;

#[derive(Clone, Debug)]
pub struct SimpleHttpClient(WrappedHttpClient);

impl SimpleHttpClient {
    pub fn new() -> Self {
        let https_connector = HttpsConnector::new();
        Self(HttpClient::builder().build(https_connector))
    }
}

impl Default for SimpleHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for SimpleHttpClient {
    type Target = WrappedHttpClient;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<WrappedHttpClient> for SimpleHttpClient {
    #[inline]
    fn from(client: WrappedHttpClient) -> Self {
        Self(client)
    }
}

impl From<SimpleHttpClient> for WrappedHttpClient {
    #[inline]
    fn from(client: SimpleHttpClient) -> Self {
        client.0
    }
}

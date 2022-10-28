use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{Context, Result};
use mlua::{Function, Lua, LuaOptions, RegistryKey, StdLib as LuaStdLib, Table};
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::lua::{self, LuaStorage};
use crate::storage::{Backend, Storage};
use crate::types::SimpleHttpClient;

use super::LocalWorkerHandle;

// TODO: Move to config
const LUA_THREAD_CACHE_SIZE: usize = 1024;

#[derive(Clone)]
pub struct WorkerContext(Rc<WorkerContextInner>);

#[derive(Default)]
pub struct WorkerContextBuilder {
    config: Arc<Config>,
    http_client: Option<SimpleHttpClient>,
    storage_backends: Vec<Backend>,
}

pub struct Filter {
    pub name: String,
    pub on_request: Option<RegistryKey>,
    pub on_response: Option<RegistryKey>,
}

pub struct WorkerContextInner {
    pub id: usize,
    pub config: Arc<Config>,
    handle: LocalWorkerHandle<WorkerContext>,

    pub lua: Rc<Lua>,
    pub filters: Vec<Filter>,
    pub handler: Option<RegistryKey>,
    pub access_log: Option<RegistryKey>,
    pub error_log: Option<RegistryKey>,

    // HTTP Client
    pub http_client: SimpleHttpClient,
    // Storage backends
    storage_backends: Vec<Backend>,
}

impl Deref for WorkerContext {
    type Target = WorkerContextInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for WorkerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("WorkerContext#{}", self.id))
    }
}

impl WorkerContextBuilder {
    pub fn new() -> Self {
        WorkerContextBuilder::default()
    }

    pub fn with_config(mut self, config: Arc<Config>) -> Self {
        self.config = config;
        self
    }

    pub fn with_http_client(mut self, client: SimpleHttpClient) -> Self {
        self.http_client = Some(client);
        self
    }

    pub fn with_storage_backends(mut self, backends: Vec<Backend>) -> Self {
        self.storage_backends = backends;
        self
    }

    pub fn build(self, handle: LocalWorkerHandle<WorkerContext>) -> Result<WorkerContext> {
        let http_client = self.http_client.unwrap_or_else(SimpleHttpClient::new);
        let storage_backends = self.storage_backends;

        WorkerContextInner::new(handle, self.config, http_client, storage_backends)
            .map(|inner| WorkerContext(Rc::new(inner)))
    }
}

impl WorkerContext {
    pub fn builder() -> WorkerContextBuilder {
        WorkerContextBuilder::new()
    }
}

impl WorkerContextInner {
    fn new(
        handle: LocalWorkerHandle<WorkerContext>,
        config: Arc<Config>,
        http_client: SimpleHttpClient,
        storage_backends: Vec<Backend>,
    ) -> Result<Self> {
        let lua_options = LuaOptions::new().thread_cache_size(LUA_THREAD_CACHE_SIZE);
        let lua = Lua::new_with(LuaStdLib::ALL_SAFE, lua_options)
            .with_context(|| "Failed to create worker Lua instance")?;
        let lua = Rc::new(lua);

        let mut worker_ctx = WorkerContextInner {
            id: handle.id(),
            config,
            lua,
            filters: Vec::new(),
            handler: None,
            access_log: None,
            error_log: None,
            handle,
            http_client,
            storage_backends,
        };

        Self::init_lua(&mut worker_ctx)
            .with_context(|| "Failed to initialize worker Lua instance")?;

        Ok(worker_ctx)
    }

    /// Initializes worker Lua instance
    fn init_lua(&mut self) -> Result<()> {
        let lua = &self.lua;

        // Register core module
        let core: Table = lua.load_from_function(
            "core",
            lua.create_function(|lua, ()| lua::core::create_module(lua))?,
        )?;

        // Set worker id
        core.set("worker_id", self.id)?;

        // Create storage backends
        let storage = lua.create_table()?;
        for backend in self.storage_backends.drain(..) {
            storage.set(backend.name(), LuaStorage::new(backend))?;
        }
        core.set("storage", storage)?;

        // Load filters code
        for filter in &self.config.http.filters {
            let handlers: Table = lua.load(&filter.code).eval()?;
            let on_request: Option<Function> = handlers.get("on_request")?;
            let on_response: Option<Function> = handlers.get("on_response")?;

            self.filters.push(Filter {
                name: filter.name.clone(),
                on_request: on_request
                    .map(|x| lua.create_registry_value(x))
                    .transpose()?,
                on_response: on_response
                    .map(|x| lua.create_registry_value(x))
                    .transpose()?,
            });
        }

        // Load main handler
        if let Some(handler) = &self.config.http.handler {
            let handler: Option<Function> = lua.load(&handler.code).eval()?;
            self.handler = handler.map(|x| lua.create_registry_value(x)).transpose()?;
        }

        // Load access logger
        if let Some(logger) = &self.config.http.access_log {
            let access_log: Option<Function> = lua.load(&logger.code).eval()?;
            self.access_log = access_log
                .map(|x| lua.create_registry_value(x))
                .transpose()?;
        }

        // Load error logger
        if let Some(logger) = &self.config.http.error_log {
            let error_log: Option<Function> = lua.load(&logger.code).eval()?;
            self.error_log = error_log
                .map(|x| lua.create_registry_value(x))
                .transpose()?;
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.handle.spawn_local(future)
    }
}

impl<F> hyper::rt::Executor<F> for WorkerContext
where
    F: Future + 'static, // not requiring `Send`
{
    #[inline]
    fn execute(&self, future: F) {
        self.handle.spawn_local(future);
    }
}

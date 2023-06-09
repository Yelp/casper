use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use mlua::{Function, Lua, LuaOptions, OwnedFunction, StdLib as LuaStdLib, Table};

use crate::config::Config;
use crate::lua::{self, LuaStorage};
use crate::storage::{Backend, Storage};

// TODO: Move to config
const LUA_THREAD_POOL_SIZE: usize = 1024;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct AppContext(Rc<AppContextInner>);

#[derive(Default)]
pub struct AppContextBuilder {
    config: Arc<Config>,
    storage_backends: Vec<Backend>,
}

pub struct Filter {
    pub name: String,
    pub on_request: Option<OwnedFunction>,
    pub on_response: Option<OwnedFunction>,
}

pub struct AppContextInner {
    pub id: usize,
    pub config: Arc<Config>,

    pub lua: Rc<Lua>,
    pub filters: Vec<Filter>,
    pub handler: Option<OwnedFunction>,
    pub access_log: Option<OwnedFunction>,
    pub error_log: Option<OwnedFunction>,

    storage_backends: Vec<Backend>,
}

impl Deref for AppContext {
    type Target = AppContextInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for AppContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("AppContext#{}", self.id))
    }
}

impl AppContextBuilder {
    pub fn new() -> Self {
        AppContextBuilder::default()
    }

    pub fn with_config(mut self, config: Arc<Config>) -> Self {
        self.config = config;
        self
    }

    pub fn with_storage_backends(mut self, backends: Vec<Backend>) -> Self {
        self.storage_backends = backends;
        self
    }

    pub fn build(self) -> Result<AppContext> {
        let storage_backends = self.storage_backends;

        AppContextInner::new(self.config, storage_backends).map(|inner| AppContext(Rc::new(inner)))
    }
}

impl AppContext {
    pub fn builder() -> AppContextBuilder {
        AppContextBuilder::new()
    }
}

impl Drop for AppContextInner {
    fn drop(&mut self) {
        lua::tasks::stop_task_scheduler(&self.lua);
    }
}

impl AppContextInner {
    fn new(config: Arc<Config>, storage_backends: Vec<Backend>) -> Result<Self> {
        let lua_options = LuaOptions::new().thread_pool_size(LUA_THREAD_POOL_SIZE);
        let lua = Lua::new_with(LuaStdLib::ALL_SAFE, lua_options)
            .with_context(|| "Failed to create Lua instance")?;
        let lua = Rc::new(lua);

        let mut worker_ctx = AppContextInner {
            id: NEXT_ID.fetch_add(1, Ordering::SeqCst),
            config,
            lua,
            filters: Vec::new(),
            handler: None,
            access_log: None,
            error_log: None,
            storage_backends,
        };

        Self::init_lua(&mut worker_ctx)
            .with_context(|| "Failed to initialize worker Lua instance")?;

        Ok(worker_ctx)
    }

    /// Initializes worker Lua instance
    fn init_lua(&mut self) -> Result<()> {
        let lua = &self.lua;

        // Use Lua optimization level "2" in release builds
        #[cfg(not(debug_assertions))]
        lua.set_compiler(mlua::Compiler::new().set_optimization_level(2));

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

        // Start task scheduler
        let max_background_tasks = self.config.main.max_background_tasks;
        lua::tasks::start_task_scheduler(lua.clone(), max_background_tasks);

        // Enable sandboxing before loading user code
        lua.sandbox(true)?;

        // Load filters code
        for filter in &self.config.http.filters {
            let handlers: Table = lua.load(filter.code.trim()).eval()?;
            let on_request: Option<Function> = handlers.get("on_request")?;
            let on_response: Option<Function> = handlers.get("on_response")?;

            self.filters.push(Filter {
                name: filter.name.clone(),
                on_request: on_request.map(|x| x.into_owned()),
                on_response: on_response.map(|x| x.into_owned()),
            });
        }

        // Load main handler
        if let Some(handler) = &self.config.http.handler {
            let handler: Option<Function> = lua.load(handler.code.trim()).eval()?;
            self.handler = handler.map(|x| x.into_owned());
        }

        // Load access logger
        if let Some(logger) = &self.config.http.access_log {
            let access_log: Option<Function> = lua.load(logger.code.trim()).eval()?;
            self.access_log = access_log.map(|x| x.into_owned());
        }

        // Load error logger
        if let Some(logger) = &self.config.http.error_log {
            let error_log: Option<Function> = lua.load(&logger.code).eval()?;
            self.error_log = error_log.map(|x| x.into_owned());
        }

        Ok(())
    }
}

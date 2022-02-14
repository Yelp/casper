use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::{Instant, SystemTime};

use anyhow::{Context, Result};
use hyper::server::conn::{AddrStream, Http};
use mlua::{Function, Lua, LuaOptions, RegistryKey, StdLib as LuaStdLib, Table};
use tokio::{runtime, sync::mpsc, task::LocalSet};
use tower::ServiceBuilder;
use tracing::error;

use crate::config::Config;
use crate::core;
use crate::metrics::{ActiveCounter, ActiveCounterHandler, InstrumentationLayer};
use crate::service::Svc;

const LUA_THREAD_CACHE_SIZE: usize = 128;

pub struct WorkerData {
    pub id: usize,
    pub config: Arc<Config>,
    pub middleware: Vec<Middleware>,
    pub access_log: Option<RegistryKey>,
    pub error_log: Option<RegistryKey>,

    // Worker stat numbers
    pub active_requests: ActiveCounter,
}

pub struct Middleware {
    pub name: String,
    pub on_request: Option<RegistryKey>,
    pub on_response: Option<RegistryKey>,
    pub after_response: Option<RegistryKey>,
}

#[derive(Debug)]
struct IncomingStream {
    stream: AddrStream,
    accept_time: Instant,
    #[allow(dead_code)] // TODO: remove
    system_time: SystemTime,
    _counter_handler: ActiveCounterHandler,
}

impl Deref for IncomingStream {
    type Target = AddrStream;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

pub struct LocalWorker {
    sender: mpsc::UnboundedSender<IncomingStream>,
}

impl LocalWorker {
    pub fn new(id: usize, config: Arc<Config>) -> Result<Self> {
        let (sender, mut recv) = mpsc::unbounded_channel::<IncomingStream>();

        let options = LuaOptions::new().thread_cache_size(LUA_THREAD_CACHE_SIZE);
        let lua = Lua::new_with(LuaStdLib::ALL_SAFE, options)
            .with_context(|| "Failed to create worker Lua")?;
        let mut worker_data = WorkerData {
            id,
            config,
            middleware: Vec::new(),
            access_log: None,
            error_log: None,
            active_requests: ActiveCounter::new(0),
        };
        Self::init_lua(&lua, &mut worker_data)
            .with_context(|| "Failed to initialize worker Lua")?;

        let handler = runtime::Handle::current();
        thread::spawn(move || {
            let lua = Rc::new(lua);
            let worker_data = Rc::new(worker_data);

            #[cfg(target_os = "linux")]
            if worker_data.config.main.pin_worker_threads {
                let cores = affinity::get_core_num();
                if let Err(err) = affinity::set_thread_affinity([id % cores]) {
                    error!("Failed to set worker thread affinity: {}", err);
                }
            }

            let local = LocalSet::new();
            local.spawn_local(async move {
                while let Some(stream) = recv.recv().await {
                    Self::process_connection(stream, lua.clone(), worker_data.clone()).await;
                }
            });
            handler.block_on(local);
        });

        Ok(Self { sender })
    }

    /// Initializes Lua instance for Worker updating WorkerData
    fn init_lua(lua: &Lua, worker_data: &mut WorkerData) -> Result<()> {
        // Register core module
        let core: Table = lua.load_from_function("core", core::make_core_module(lua)?)?;

        // Set worker id
        core.set("worker_id", worker_data.id)?;

        // Load middleware code
        for middleware in &worker_data.config.http.middleware {
            let handlers: Table = lua.load(&middleware.code).eval()?;
            let on_request: Option<Function> = handlers.get("on_request")?;
            let on_response: Option<Function> = handlers.get("on_response")?;
            let after_response: Option<Function> = handlers.get("after_response")?;

            worker_data.middleware.push(Middleware {
                name: middleware.name.clone(),
                on_request: on_request
                    .map(|x| lua.create_registry_value(x))
                    .transpose()?,
                on_response: on_response
                    .map(|x| lua.create_registry_value(x))
                    .transpose()?,
                after_response: after_response
                    .map(|x| lua.create_registry_value(x))
                    .transpose()?,
            });
        }

        // Load access logger
        if let Some(logger) = &worker_data.config.http.access_log {
            let access_log: Option<Function> = lua.load(&logger.code).eval()?;
            worker_data.access_log = access_log
                .map(|x| lua.create_registry_value(x))
                .transpose()?;
        }

        // Load error logger
        if let Some(logger) = &worker_data.config.http.error_log {
            let error_log: Option<Function> = lua.load(&logger.code).eval()?;
            worker_data.error_log = error_log
                .map(|x| lua.create_registry_value(x))
                .transpose()?;
        }

        Ok(())
    }

    pub fn spawn(&self, stream: AddrStream) {
        let in_stream = IncomingStream {
            stream,
            accept_time: Instant::now(),
            system_time: SystemTime::now(),
            _counter_handler: connections_counter_add!(1),
        };

        self.sender
            .send(in_stream)
            .expect("Thread with LocalSet has shut down.");
    }

    async fn process_connection(stream: IncomingStream, lua: Rc<Lua>, worker_data: Rc<WorkerData>) {
        // Time spent in a queue before processing
        let _queue_dur = stream.accept_time.elapsed();

        tokio::task::spawn_local(async move {
            let svc = Svc {
                lua,
                worker_data,
                remote_addr: stream.remote_addr(),
            };

            let service = ServiceBuilder::new()
                .layer(InstrumentationLayer::new("/metrics".to_string()))
                .service(svc);

            // One stream can send multiple http requests
            let result = Http::new()
                .with_executor(LocalExecutor)
                .http1_only(true)
                .http1_keep_alive(true)
                .serve_connection(stream.stream, service)
                .await;

            if let Err(err) = result {
                error!("{:?}", err);
            }
        });
    }
}

#[derive(Clone, Copy, Debug)]
struct LocalExecutor;

impl<F> hyper::rt::Executor<F> for LocalExecutor
where
    F: std::future::Future + 'static, // not requiring `Send`
{
    fn execute(&self, fut: F) {
        tokio::task::spawn_local(fut);
    }
}

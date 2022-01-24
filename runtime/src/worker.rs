use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::{Instant, SystemTime};

use anyhow::{Context, Result};
use hyper::server::conn::{AddrStream, Http};
use mlua::{Function, Lua, RegistryKey, Table, Value};
use tokio::{runtime, sync::mpsc, task::LocalSet};
use tracing::error;

use crate::config::Config;
use crate::core;
use crate::service::Svc;

pub struct WorkerData {
    pub id: usize,
    pub config: Arc<Config>,
    pub middleware: Vec<Middleware>,
    pub logger: AccessLogger,
}

pub struct Middleware {
    pub on_request: Option<RegistryKey>,
    pub on_response: Option<RegistryKey>,
    pub after_response: Option<RegistryKey>,
}

#[derive(Default)]
pub struct AccessLogger {
    pub on_access_log: Option<RegistryKey>,
}

#[derive(Debug)]
struct IncomingStream {
    stream: AddrStream,
    accept_time: Instant,
    #[allow(dead_code)] // TODO: remove
    system_time: SystemTime,
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

        let lua = Lua::new();
        let mut worker_data = WorkerData {
            id,
            config,
            middleware: Vec::new(),
            logger: AccessLogger::default(),
        };
        Self::init_lua(&lua, &mut worker_data).with_context(|| "Failed to initialize Lua")?;

        let handler = runtime::Handle::current();
        thread::spawn(move || {
            let lua = Rc::new(lua);
            let worker_data = Rc::new(worker_data);

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
        for middleware in &worker_data.config.middleware {
            let handlers: Table = lua.load(&middleware.code).eval()?;
            let on_request: Option<Function> = handlers.get("on_request")?;
            let on_response: Option<Function> = handlers.get("on_response")?;
            let after_response: Option<Function> = handlers.get("after_response")?;

            worker_data.middleware.push(Middleware {
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

        // Access logger
        if let Some(logger) = &worker_data.config.access_log {
            let handlers: Table = lua.load(&logger.code).eval()?;
            let access_log: Option<Function> = handlers.get("access_log")?;

            worker_data.logger.on_access_log = access_log
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
        };

        self.sender
            .send(in_stream)
            .expect("Thread with LocalSet has shut down.");
    }

    async fn process_connection(stream: IncomingStream, lua: Rc<Lua>, worker_data: Rc<WorkerData>) {
        // Time spent in a queue before processing
        let _queue_dur = stream.accept_time.elapsed();

        tokio::task::spawn_local(async move {
            let remote_addr = stream.remote_addr();

            // Create Lua context table
            let ctx = lua
                .create_table()
                .expect("Failed to create Lua context table");
            let ctx_key = lua
                .create_registry_value(ctx.clone())
                .expect("Failed to store Lua context table in the registry");

            let service = Svc {
                lua: lua.clone(),
                worker_data: worker_data.clone(),
                ctx_key: Rc::new(ctx_key),
                remote_addr,
            };

            let result = Http::new()
                .with_executor(LocalExecutor)
                .http1_only(true)
                .http1_keep_alive(true)
                .serve_connection(stream.stream, service)
                .await;

            // Total time to send response
            let _total_time = stream.accept_time.elapsed();

            if let Err(err) = result {
                error!("{:?}", err);
                return;
            }

            // Access logging
            if let Some(on_access_log) = &worker_data.logger.on_access_log {
                if let Ok(on_access_log) = lua.registry_value::<Function>(on_access_log) {
                    let _ = on_access_log.call_async::<_, Value>(ctx.clone()).await;
                }
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

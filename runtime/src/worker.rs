#![allow(dead_code)]

use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::{Instant, SystemTime};

use anyhow::Result;
use hyper::server::conn::{AddrStream, Http};
use mlua::{Function, Lua, RegistryKey as LuaRegistryKey, Table};
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::task::LocalSet;

use crate::config::Config;
use crate::core;
use crate::service::Svc;

pub struct WorkerData {
    pub id: usize,
    pub config: Arc<Config>,
    pub middleware: Vec<Middleware>,
}

pub struct Middleware {
    pub on_request: Option<LuaRegistryKey>,
    pub on_response: Option<LuaRegistryKey>,
    pub after_response: Option<LuaRegistryKey>,
}

#[derive(Debug)]
struct IncomingStream {
    stream: AddrStream,
    accept_time: Instant,
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
    pub fn new(id: usize, config: Arc<Config>) -> Self {
        let (sender, mut recv) = mpsc::unbounded_channel::<IncomingStream>();

        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let lua = Lua::new();
        let mut worker_data = WorkerData {
            id,
            config,
            middleware: Vec::new(),
        };
        Self::init_lua(&lua, &mut worker_data).unwrap();

        thread::spawn(move || {
            let local = LocalSet::new();
            let lua = Rc::new(lua);
            let worker_data = Rc::new(worker_data);

            local.spawn_local(async move {
                while let Some(stream) = recv.recv().await {
                    let lua = lua.clone();
                    let worker_data = worker_data.clone();
                    let remote_addr = stream.remote_addr();

                    tokio::task::spawn_local(async move {
                        let service = Svc {
                            lua,
                            worker_data,
                            remote_addr,
                        };

                        let result = Http::new()
                            .with_executor(LocalExecutor)
                            .http1_only(true)
                            .http1_keep_alive(true)
                            .serve_connection(stream.stream, service)
                            .await;

                        if let Err(err) = result {
                            println!("error: {}", err);
                        }
                    });
                }
            });
            rt.block_on(local);
        });

        Self { sender }
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

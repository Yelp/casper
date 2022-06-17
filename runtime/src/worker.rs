use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::future::{AbortHandle, Abortable};
use hyper::server::conn::{AddrStream, Http};
use mlua::{Function, Lua, LuaOptions, RegistryKey, StdLib as LuaStdLib, Table};
use tokio::runtime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::{spawn_local, JoinHandle, LocalSet};
use tower::ServiceBuilder;
use tracing::error;

use crate::config::Config;
use crate::core;
use crate::lua::tasks;
use crate::metrics::{ActiveCounter, ActiveCounterHandler, InstrumentationLayer};
use crate::service::Svc;

// TODO: Move to config
const LUA_THREAD_CACHE_SIZE: usize = 1024;

pub struct WorkerContext {
    pub id: usize,
    pub config: Arc<Config>,

    pub lua: Rc<Lua>,
    pub middleware: Vec<Middleware>,
    pub access_log: Option<RegistryKey>,
    pub error_log: Option<RegistryKey>,

    // Worker stat numbers
    pub active_requests: ActiveCounter,
    task_count: Arc<AtomicUsize>,
}

pub struct Middleware {
    pub name: String,
    pub on_request: Option<RegistryKey>,
    pub on_response: Option<RegistryKey>,
    pub after_response: Option<RegistryKey>,
}

/// A handle to a local pool, used for spawning `!Send` tasks.
#[derive(Clone)]
pub struct WorkerPoolHandle {
    pool: Arc<WorkerPool>,
}

impl WorkerPoolHandle {
    /// Create a new pool of threads to handle `!Send` tasks. Spawn tasks onto this
    /// pool via [`WorkerPoolHandle::spawn_pinned`].
    ///
    /// # Panics
    /// Panics if the pool size is less than one.
    pub fn new(pool_size: usize, config: Arc<Config>) -> Result<Self> {
        assert!(pool_size > 0);

        let workers = (0..pool_size)
            .map(|id| LocalWorkerHandle::new(id, Arc::clone(&config)))
            .collect::<Result<_>>()?;

        let pool = Arc::new(WorkerPool { workers });

        Ok(WorkerPoolHandle { pool })
    }

    pub fn process_connection(&self, stream: AddrStream) {
        let accept_time = Instant::now();
        let counter_handle = connections_counter_add!(1);

        self.pool.spawn_pinned(move |worker_ctx| async move {
            // Time spent in a queue before processing
            let _queue_dur = accept_time.elapsed();

            let svc = Svc {
                worker_ctx: Rc::clone(&worker_ctx),
                remote_addr: stream.remote_addr(),
            };

            let service = ServiceBuilder::new()
                .layer(InstrumentationLayer::new("/metrics".to_string()))
                .service(svc);

            let result = Http::new()
                .with_executor(LocalExecutor(worker_ctx))
                .http1_only(true)
                .http1_keep_alive(true)
                .serve_connection(stream, service)
                .await;

            if let Err(err) = result {
                error!("{:?}", err);
            }

            drop(counter_handle);
        });
    }
}

impl Debug for WorkerPoolHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("WorkerPoolHandle")
    }
}

struct WorkerPool {
    workers: Vec<LocalWorkerHandle>,
}

impl WorkerPool {
    /// Spawn a `?Send` future onto a worker.
    /// Note that the future is not [`Send`], but the [`FnOnce`] which creates it is.
    fn spawn_pinned<F, Fut>(&self, create_task: F) -> JoinHandle<Fut::Output>
    where
        F: FnOnce(Rc<WorkerContext>) -> Fut + Send + 'static,
        Fut: Future + 'static,
        Fut::Output: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();

        let (worker, job_guard) = self.find_and_incr_least_burdened_worker();
        let worker_spawner = worker.spawner.clone();

        // Spawn a future onto the worker's runtime so we can immediately return
        // a join handle.
        worker.runtime_handle.spawn(async move {
            // Move the job guard into the task
            let _job_guard = job_guard;

            // Propagate aborts via Abortable/AbortHandle
            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let _abort_guard = AbortGuard(abort_handle);

            // Inside the future we can't run spawn_local yet because we're not
            // in the context of a LocalSet. We need to send create_task to the
            // LocalSet task for spawning.
            let spawn_task = Box::new(move |ctx: Rc<WorkerContext>| {
                // Once we're in the LocalSet context we can call spawn_local
                let join_handle = spawn_local(async move {
                    Abortable::new(create_task(ctx), abort_registration).await
                });

                // Send the join handle back to the spawner. If sending fails,
                // we assume the parent task was canceled, so cancel this task
                // as well.
                if let Err(join_handle) = sender.send(join_handle) {
                    join_handle.abort()
                }
            });

            // Send the callback to the LocalSet task
            if let Err(e) = worker_spawner.send(spawn_task) {
                // Propagate the error as a panic in the join handle.
                panic!("Failed to send job to worker: {}", e);
            }

            // Wait for the task's join handle
            let join_handle = match receiver.await {
                Ok(handle) => handle,
                Err(e) => {
                    // We sent the task successfully, but failed to get its
                    // join handle... We assume something happened to the worker
                    // and the task was not spawned. Propagate the error as a
                    // panic in the join handle.
                    panic!("Worker failed to send join handle: {}", e);
                }
            };

            // Wait for the task to complete
            let join_result = join_handle.await;

            match join_result {
                Ok(Ok(output)) => output,
                Ok(Err(_)) => {
                    // Pinned task was aborted. But that only happens if this
                    // task is aborted. So this is an impossible branch.
                    unreachable!(
                        "Reaching this branch means this task was previously \
                         aborted but it continued running anyways"
                    )
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else if e.is_cancelled() {
                        // No one else should have the join handle, so this is
                        // unexpected. Forward this error as a panic in the join
                        // handle.
                        panic!("spawn_pinned task was canceled: {}", e);
                    } else {
                        // Something unknown happened (not a panic or
                        // cancellation). Forward this error as a panic in the
                        // join handle.
                        panic!("spawn_pinned task failed: {}", e);
                    }
                }
            }
        })
    }

    /// Find the worker with the least number of tasks, increment its task
    /// count, and return its handle. Make sure to actually spawn a task on
    /// the worker so the task count is kept consistent with load.
    ///
    /// A job count guard is also returned to ensure the task count gets
    /// decremented when the job is done.
    fn find_and_incr_least_burdened_worker(&self) -> (&LocalWorkerHandle, JobCountGuard) {
        loop {
            let (worker, task_count) = self
                .workers
                .iter()
                .map(|worker| (worker, worker.task_count.load(Ordering::SeqCst)))
                .min_by_key(|&(_, count)| count)
                .expect("There must be more than one worker");

            // Make sure the task count hasn't changed since when we choose this
            // worker. Otherwise, restart the search.
            if worker
                .task_count
                .compare_exchange(
                    task_count,
                    task_count + 1,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return (worker, JobCountGuard(Arc::clone(&worker.task_count)));
            }
        }
    }
}

/// Automatically decrements a worker's job count when a job finishes (when
/// this gets dropped).
struct JobCountGuard(Arc<AtomicUsize>);

impl JobCountGuard {
    pub fn inc(counter: Arc<AtomicUsize>, val: usize) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        JobCountGuard(counter)
    }
}

impl Drop for JobCountGuard {
    fn drop(&mut self) {
        // Decrement the job count
        let previous_value = self.0.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous_value >= 1);
    }
}

/// Calls abort on the handle when dropped.
struct AbortGuard(AbortHandle);

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type PinnedFutureSpawner = Box<dyn FnOnce(Rc<WorkerContext>) + Send + 'static>;

struct LocalWorkerHandle {
    runtime_handle: tokio::runtime::Handle,
    spawner: UnboundedSender<PinnedFutureSpawner>,
    task_count: Arc<AtomicUsize>,
}

impl LocalWorkerHandle {
    /// Create a new worker for executing pinned tasks
    fn new(id: usize, config: Arc<Config>) -> Result<Self> {
        let (sender, receiver) = unbounded_channel();
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to start a worker thread runtime");
        let runtime_handle = runtime.handle().clone();
        let task_count = Arc::new(AtomicUsize::new(0));
        let task_count_clone = Arc::clone(&task_count);

        let (error_tx, error_rx) = std::sync::mpsc::sync_channel::<Result<()>>(1);
        thread::spawn(move || {
            #[cfg(target_os = "linux")]
            if config.main.pin_worker_threads {
                let cores = affinity::get_core_num();
                if let Err(err) = affinity::set_thread_affinity([id % cores]) {
                    error!("Failed to set worker thread affinity: {err}");
                }
            }

            let lua_options = LuaOptions::new().thread_cache_size(LUA_THREAD_CACHE_SIZE);
            let lua = Lua::new_with(LuaStdLib::ALL_SAFE, lua_options)
                .expect("Failed to create worker Lua instance");
            let lua = Rc::new(lua);

            let mut worker_ctx = WorkerContext {
                id,
                config,
                lua,
                middleware: Vec::new(),
                access_log: None,
                error_log: None,
                active_requests: ActiveCounter::new(0),
                task_count: task_count_clone,
            };

            if let Err(err) = Self::init_lua(&mut worker_ctx)
                .with_context(|| "Failed to initialize worker Lua instance")
            {
                error_tx.send(Err(err)).expect("Failed to send Lua Result");
                return;
            }
            error_tx.send(Ok(())).expect("Failed to send Lua Result");

            Self::run(runtime, receiver, worker_ctx)
        });

        error_rx.recv().expect("Failed to receive Lua Result")?;

        Ok(LocalWorkerHandle {
            runtime_handle,
            spawner: sender,
            task_count,
        })
    }

    fn run(
        runtime: tokio::runtime::Runtime,
        mut task_receiver: UnboundedReceiver<PinnedFutureSpawner>,
        worker_ctx: WorkerContext,
    ) {
        let local_set = LocalSet::new();
        let task_count = Arc::clone(&worker_ctx.task_count);

        // Track Lua used memory every 10 seconds
        let (worker_id, lua_clone) = (worker_ctx.id, Rc::clone(&worker_ctx.lua));
        local_set.spawn_local(async move {
            loop {
                lua_used_memory_update!(worker_id, lua_clone.used_memory());
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });

        local_set.block_on(&runtime, async {
            // Launch Lua task processor
            tasks::spawn_tasks(Rc::clone(&worker_ctx.lua));

            let worker_ctx = Rc::new(worker_ctx);
            while let Some(spawn_task) = task_receiver.recv().await {
                // Calls spawn_local(future)
                (spawn_task)(Rc::clone(&worker_ctx));
            }
        });

        // If there are any tasks on the runtime associated with a LocalSet task
        // that has already completed, but whose output has not yet been
        // reported, let that task complete.
        //
        // Since the task_count is decremented when the runtime task exits,
        // reading that counter lets us know if any such tasks completed during
        // the call to `block_on`.
        //
        // Tasks on the LocalSet can't complete during this loop since they're
        // stored on the LocalSet and we aren't accessing it.
        let mut previous_task_count = task_count.load(Ordering::SeqCst);
        loop {
            // This call will also run tasks spawned on the runtime.
            runtime.block_on(tokio::task::yield_now());
            let new_task_count = task_count.load(Ordering::SeqCst);
            if new_task_count == previous_task_count {
                break;
            } else {
                previous_task_count = new_task_count;
            }
        }

        // It's now no longer possible for a task on the runtime to be
        // associated with a LocalSet task that has completed. Drop both the
        // LocalSet and runtime to let tasks on the runtime be cancelled if and
        // only if they are still on the LocalSet.
        //
        // Drop the LocalSet task first so that anyone awaiting the runtime
        // JoinHandle will see the cancelled error after the LocalSet task
        // destructor has completed.
        drop(local_set);
        drop(runtime);
    }

    /// Initializes Lua instance for Worker updating WorkerData
    fn init_lua(worker_ctx: &mut WorkerContext) -> Result<()> {
        let lua = &worker_ctx.lua;

        // Register core module
        let core: Table = lua.load_from_function("core", core::make_core_module(lua)?)?;

        // Set worker id
        core.set("worker_id", worker_ctx.id)?;

        // Load middleware code
        for middleware in &worker_ctx.config.http.middleware {
            let handlers: Table = lua.load(&middleware.code).eval()?;
            let on_request: Option<Function> = handlers.get("on_request")?;
            let on_response: Option<Function> = handlers.get("on_response")?;
            let after_response: Option<Function> = handlers.get("after_response")?;

            worker_ctx.middleware.push(Middleware {
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
        if let Some(logger) = &worker_ctx.config.http.access_log {
            let access_log: Option<Function> = lua.load(&logger.code).eval()?;
            worker_ctx.access_log = access_log
                .map(|x| lua.create_registry_value(x))
                .transpose()?;
        }

        // Load error logger
        if let Some(logger) = &worker_ctx.config.http.error_log {
            let error_log: Option<Function> = lua.load(&logger.code).eval()?;
            worker_ctx.error_log = error_log
                .map(|x| lua.create_registry_value(x))
                .transpose()?;
        }

        Ok(())
    }
}

impl Debug for WorkerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("WorkerContext#{}", self.id))
    }
}

impl WorkerContext {
    fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        // We have access to WorkerContext only inside LocalSet,
        // so we can spawn local tasks.
        let task_count = Arc::clone(&self.task_count);
        tokio::task::spawn_local(async move {
            let _guard = JobCountGuard::inc(task_count, 1);
            future.await
        })
    }
}

#[derive(Clone, Debug)]
struct LocalExecutor(Rc<WorkerContext>);

impl<F> hyper::rt::Executor<F> for LocalExecutor
where
    F: Future + 'static, // not requiring `Send`
{
    fn execute(&self, futute: F) {
        self.0.spawn_local(futute);
    }
}

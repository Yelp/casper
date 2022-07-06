use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use dyn_clone::{clone_box, DynClone};
use futures_util::future::{self, AbortHandle, Abortable, LocalBoxFuture};
use tokio::runtime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::{spawn_local, JoinHandle, LocalSet};

use super::JobCountGuard;

trait InitWorkerFn<C>: DynClone + Send {
    fn call(&self, _: LocalWorkerHandle<C>) -> LocalBoxFuture<'static, Result<C>>;
}

impl<F, C, Fut> InitWorkerFn<C> for F
where
    F: Fn(LocalWorkerHandle<C>) -> Fut,
    F: Send + Clone + 'static,
    Fut: Future<Output = Result<C>> + 'static,
{
    fn call(&self, handle: LocalWorkerHandle<C>) -> LocalBoxFuture<'static, Result<C>> {
        Box::pin((self)(handle))
    }
}

pub struct WorkerPoolBuilder<C = ()> {
    pool_size: usize,
    on_init: Box<dyn InitWorkerFn<C>>,
}

impl WorkerPoolBuilder {
    pub fn new() -> Self {
        WorkerPoolBuilder {
            pool_size: num_cpus::get(),
            on_init: Box::new(|_| Box::pin(future::ok(()))),
        }
    }
}

impl<C> WorkerPoolBuilder<C>
where
    C: Clone + 'static,
{
    pub fn pool_size(mut self, size: usize) -> Self {
        self.pool_size = size;
        self
    }

    pub fn on_worker_init<F, Fut, C2>(self, f: F) -> WorkerPoolBuilder<C2>
    where
        F: Fn(LocalWorkerHandle<C2>) -> Fut,
        F: Send + Clone + 'static,
        Fut: Future<Output = Result<C2>> + 'static,
    {
        WorkerPoolBuilder {
            pool_size: self.pool_size,
            on_init: Box::new(f),
        }
    }

    #[track_caller]
    pub fn build(self) -> Result<WorkerPoolHandle<C>> {
        WorkerPoolHandle::new(self.pool_size, self.on_init)
    }
}

/// A cloneable handle to a local pool, used for spawning `!Send` tasks.
///
/// Internally the local pool uses a [`tokio::task::LocalSet`] for each worker thread
/// in the pool. Consequently you can also use [`tokio::task::spawn_local`] (which will
/// execute on the same thread) inside the Future you supply to the various spawn methods
/// of `LocalPoolHandle`,
///
/// [`tokio::task::LocalSet`]: tokio::task::LocalSet
/// [`tokio::task::spawn_local`]: tokio::task::spawn_local
#[derive(Clone)]
pub struct WorkerPoolHandle<C = ()> {
    pool: Arc<WorkerPool<C>>,
}

impl WorkerPoolHandle {
    pub fn build() -> WorkerPoolBuilder {
        WorkerPoolBuilder::new()
    }
}

impl<C> WorkerPoolHandle<C>
where
    C: Clone + 'static,
{
    /// Create a new pool of threads to handle `!Send` tasks. Spawn tasks onto this
    /// pool via [`LocalPoolHandle::spawn_pinned`].
    ///
    /// # Panics
    ///
    /// Panics if the pool size is less than one.
    #[track_caller]
    fn new(pool_size: usize, init: Box<dyn InitWorkerFn<C>>) -> Result<WorkerPoolHandle<C>> {
        assert!(pool_size > 0);

        let workers = (0..pool_size)
            .map(|id| LocalWorkerHandle::new_worker(id, clone_box(&*init)))
            .collect::<Result<_, _>>()?;

        let pool = Arc::new(WorkerPool { workers });

        Ok(WorkerPoolHandle { pool })
    }

    /// Returns the number of threads of the Pool.
    #[allow(unused)]
    #[inline]
    pub fn num_threads(&self) -> usize {
        self.pool.workers.len()
    }

    /// Returns the number of tasks scheduled on each worker. The indices of the
    /// worker threads correspond to the indices of the returned `Vec`.
    #[allow(unused)]
    pub fn get_task_loads_for_each_worker(&self) -> Vec<usize> {
        self.pool
            .workers
            .iter()
            .map(|worker| worker.task_count.load(Ordering::SeqCst))
            .collect::<Vec<_>>()
    }

    /// Spawn a task onto a worker thread and pin it there so it can't be moved
    /// off of the thread. Note that the future is not [`Send`], but the
    /// [`FnOnce`] which creates it is.
    pub fn spawn_pinned<F, Fut>(&self, create_task: F) -> JoinHandle<Fut::Output>
    where
        F: FnOnce(C) -> Fut,
        F: Send + 'static,
        Fut: Future + 'static,
        Fut::Output: Send + 'static,
    {
        self.pool
            .spawn_pinned(create_task, WorkerChoice::LeastBurdened)
    }

    /// Differs from `spawn_pinned` only in that you can choose a specific worker thread
    /// of the pool, whereas `spawn_pinned` chooses the worker with the smallest
    /// number of tasks scheduled.
    ///
    /// A worker thread is chosen by index. Indices are 0 based and the largest index
    /// is given by `num_threads() - 1`
    ///
    /// # Panics
    ///
    /// This method panics if the index is out of bounds.
    #[allow(unused)]
    #[track_caller]
    pub fn spawn_pinned_by_idx<F, Fut>(&self, create_task: F, idx: usize) -> JoinHandle<Fut::Output>
    where
        F: FnOnce(C) -> Fut,
        F: Send + 'static,
        Fut: Future + 'static,
        Fut::Output: Send + 'static,
    {
        self.pool
            .spawn_pinned(create_task, WorkerChoice::ByIdx(idx))
    }
}

impl<C> Debug for WorkerPoolHandle<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("WorkerPoolHandle")
    }
}

enum WorkerChoice {
    LeastBurdened,
    ByIdx(usize),
}

struct WorkerPool<C> {
    workers: Vec<LocalWorkerHandle<C>>,
}

impl<C> WorkerPool<C>
where
    C: Clone + 'static,
{
    /// Spawn a `?Send` future onto a worker
    #[track_caller]
    fn spawn_pinned<F, Fut>(
        &self,
        create_task: F,
        worker_choice: WorkerChoice,
    ) -> JoinHandle<Fut::Output>
    where
        F: FnOnce(C) -> Fut,
        F: Send + 'static,
        Fut: Future + 'static,
        Fut::Output: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let (worker, job_guard) = match worker_choice {
            WorkerChoice::LeastBurdened => self.find_and_incr_least_burdened_worker(),
            WorkerChoice::ByIdx(idx) => self.find_worker_by_idx(idx),
        };
        let worker_spawner = worker.spawner.clone();

        // Spawn a future onto the worker's thread so we can immediately return
        // a join handle.
        tokio::task::spawn(async move {
            // Move the job guard into the task
            let _job_guard = job_guard;

            // Propagate aborts via Abortable/AbortHandle
            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let _abort_guard = AbortGuard(abort_handle);

            // Inside the future we can't run spawn_local yet because we're not
            // in the context of a LocalSet. We need to send create_task to the
            // LocalSet task for spawning.
            let spawn_task = Box::new(move |context| {
                // Once we're in the LocalSet context we can call spawn_local
                let join_handle = spawn_local(async move {
                    Abortable::new(create_task(context), abort_registration).await
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
    fn find_and_incr_least_burdened_worker(&self) -> (&LocalWorkerHandle<C>, JobCountGuard) {
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

    #[track_caller]
    fn find_worker_by_idx(&self, idx: usize) -> (&LocalWorkerHandle<C>, JobCountGuard) {
        let worker = &self.workers[idx];
        (worker, JobCountGuard::inc(&worker.task_count))
    }
}

/// Calls abort on the handle when dropped.
struct AbortGuard(AbortHandle);

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type PinnedFutureSpawner<C> = Box<dyn FnOnce(C) + Send + 'static>;

#[derive(Clone)]
pub struct LocalWorkerHandle<C> {
    id: usize,
    spawner: UnboundedSender<PinnedFutureSpawner<C>>,
    task_count: Arc<AtomicUsize>,
}

impl<C> LocalWorkerHandle<C>
where
    C: Clone + 'static,
{
    /// Create a new worker for executing pinned tasks
    fn new_worker(id: usize, init: Box<dyn InitWorkerFn<C>>) -> Result<LocalWorkerHandle<C>> {
        let (sender, receiver) = unbounded_channel();
        let runtime_handle = runtime::Handle::current();
        let task_count = Arc::new(AtomicUsize::new(0));

        let worker_handle = LocalWorkerHandle {
            id,
            spawner: sender,
            task_count,
        };
        let worker_handle2 = worker_handle.clone();

        let (init_tx, init_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(format!("casper-worker-{id}"))
            .spawn(move || {
                let init_fut = init.call(worker_handle2);
                Self::run(runtime_handle, receiver, init_fut, init_tx)
            })
            .expect("Failed to spawn worker thread");

        // Wait for init results
        init_rx
            .recv()
            .expect("Failed to receive worker init result")?;

        Ok(worker_handle)
    }

    fn run(
        runtime_handle: runtime::Handle,
        mut task_receiver: UnboundedReceiver<PinnedFutureSpawner<C>>,
        init_fut: LocalBoxFuture<'static, Result<C>>,
        init_tx: std::sync::mpsc::SyncSender<Result<()>>,
    ) {
        let local_set = LocalSet::new();
        runtime_handle.block_on(local_set.run_until(async {
            let context = match init_fut.await {
                Ok(ctx) => ctx,
                Err(err) => {
                    init_tx
                        .send(Err(err))
                        .expect("Failed to send worker init result");
                    return;
                }
            };
            init_tx
                .send(Ok(()))
                .expect("Failed to send worker init result");

            while let Some(spawn_task) = task_receiver.recv().await {
                // Calls spawn_local(future)
                (spawn_task)(context.clone());
            }
        }));
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let job_guard = JobCountGuard::inc(&self.task_count);
        tokio::task::spawn_local(async move {
            let _guard = job_guard;
            future.await
        })
    }
}

use std::rc::Rc;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicU64, Ordering};

use mlua::{
    AnyUserData, ExternalError, ExternalResult, Function, Lua, OwnedFunction, RegistryKey, Result,
    Table, UserData, Value,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tracing::warn;

// TODO: Support recurring tasks

type TaskJoinHandle = JoinHandle<Result<RegistryKey>>;

#[derive(Debug)]
struct Task {
    id: u64,
    name: Option<String>,
    timeout: Option<Duration>,
    handler: OwnedFunction,
    join_handle_tx: oneshot::Sender<TaskJoinHandle>,
}

struct TaskHandle {
    id: u64,
    name: Option<String>,
    join_handle: Option<TaskJoinHandle>,
    join_handle_rx: Option<oneshot::Receiver<TaskJoinHandle>>,
}

struct MaxBackgroundTasks(Option<u64>);

// Global task identifier
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

#[allow(clippy::await_holding_refcell_ref)]
impl UserData for TaskHandle {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("id", |_, this| Ok(this.id));
        fields.add_field_method_get("name", |lua, this| lua.pack(this.name.as_deref()));
    }

    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_function("join", |lua, this: AnyUserData| async move {
            let mut this = this.take::<Self>()?;
            if let Some(rx) = this.join_handle_rx.take() {
                this.join_handle = Some(rx.await.expect("Failed to get task join handle"));
            }
            let result = lua_try!(this.join_handle.unwrap().await);
            let key = lua_try!(result);
            Ok(Ok(lua.registry_value::<Value>(&key)?))
        });

        methods.add_async_function("abort", |_, this: AnyUserData| async move {
            let mut this = this.take::<Self>()?;
            if let Some(rx) = this.join_handle_rx.take() {
                this.join_handle = Some(rx.await.expect("Failed to get task join handle"));
            }
            this.join_handle.unwrap().abort();
            Ok(())
        });

        methods.add_async_function("is_finished", |_, this: AnyUserData| async move {
            let mut this = this.borrow_mut::<Self>()?;
            if let Some(rx) = this.join_handle_rx.take() {
                this.join_handle = Some(rx.await.expect("Failed to get task join handle"));
            }
            Ok(this.join_handle.as_ref().unwrap().is_finished())
        });
    }
}

fn spawn_task(lua: &Lua, arg: Value) -> Result<StdResult<TaskHandle, String>> {
    let max_background_tasks = lua.app_data_ref::<MaxBackgroundTasks>().unwrap();
    let current_tasks = tasks_counter_get!();

    if let Some(max_tasks) = max_background_tasks.0 {
        if current_tasks >= max_tasks {
            return Ok(Err("max background task limit reached".to_string()));
        }
    }

    let task_tx = lua
        .app_data_ref::<UnboundedSender<Task>>()
        .expect("Failed to get task sender");

    // Oneshot channel to send join handler
    let (join_handle_tx, join_handle_rx) = oneshot::channel();

    let mut name = None;
    let mut timeout = None;
    let handler = match arg {
        Value::Function(task_fn) => task_fn.into_owned(),
        Value::Table(params) => {
            name = params.get::<_, Option<String>>("name")?;
            timeout = params
                .get::<_, Option<u64>>("timeout")?
                .map(tokio::time::Duration::from_millis);
            let task_fn = params.get::<_, Function>("handler")?;
            task_fn.into_owned()
        }
        v => {
            let err = format!(
                "cannot spawn task: invalid argument type `{}`",
                v.type_name()
            );
            return Err(err.into_lua_err());
        }
    };

    let id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
    task_tx
        .send(Task {
            id,
            name: name.clone(),
            timeout,
            handler,
            join_handle_tx,
        })
        .map_err(|err| format!("cannot spawn task: {err}"))
        .into_lua_err()?;

    Ok(Ok(TaskHandle {
        id,
        name,
        join_handle: None,
        join_handle_rx: Some(join_handle_rx),
    }))
}

pub fn start_task_scheduler(lua: Rc<Lua>, max_background_tasks: Option<u64>) {
    let mut task_rx = lua
        .remove_app_data::<UnboundedReceiver<Task>>()
        .expect("Failed to get task receiver");

    lua.set_app_data(MaxBackgroundTasks(max_background_tasks));

    tokio::task::spawn_local(async move {
        while let Some(task) = task_rx.recv().await {
            let lua = lua.clone();

            let join_handle = tokio::task::spawn_local(async move {
                let start = Instant::now();
                let _task_count_guard = tasks_counter_inc!();
                let task_future = task.handler.to_ref().call_async::<_, Value>(());

                let result = match task.timeout {
                    Some(timeout) => ntex::time::timeout(timeout, task_future).await,
                    None => Ok(task_future.await),
                };

                let result = match result {
                    Ok(result) => result.and_then(|v| lua.create_registry_value(v)),
                    // Outer Result errors will always be timeouts
                    Err(_) => Err("task exceeded timeout".to_string()).into_lua_err(),
                };

                // Record task metrics
                match task.name {
                    Some(name) => {
                        task_histogram_rec!(start, "name" => name.clone());
                        if let Err(ref err) = result {
                            warn!("task '{name}' error: {err:?}");
                            task_error_counter_add!(1, "name" => name);
                        }
                    }
                    None => {
                        task_histogram_rec!(start);
                        if let Err(ref err) = result {
                            warn!("task #{} error: {err:?}", task.id);
                            task_error_counter_add!(1);
                        }
                    }
                };

                result
            });
            // Receiver can be dropped, it's not an error
            let _ = task.join_handle_tx.send(join_handle);
        }
    });
}

pub fn stop_task_scheduler(lua: &Rc<Lua>) {
    lua.remove_app_data::<UnboundedSender<Task>>();
    lua.remove_app_data::<UnboundedReceiver<Task>>();
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    // Create channel to send tasks
    let (tx, rx) = mpsc::unbounded_channel::<Task>();
    lua.set_app_data(tx);
    lua.set_app_data(rx);

    lua.create_table_from([("spawn", lua.create_function(spawn_task)?)])
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::time::Duration;

    use mlua::{chunk, Lua, Result};

    #[ntex::test]
    async fn test_tasks() -> Result<()> {
        let lua = Rc::new(Lua::new());

        lua.globals().set("tasks", super::create_module(&lua)?)?;
        lua.globals().set(
            "sleep",
            lua.create_async_function(|_, secs| async move {
                tokio::time::sleep(Duration::from_secs_f32(secs)).await;
                Ok(())
            })?,
        )?;

        super::start_task_scheduler(lua.clone(), Some(2));

        // Test normal task run and result collection
        lua.load(chunk! {
            local handle = tasks.spawn(function()
                sleep(0.1)
                return "hello"
            end)
            assert(handle.id > 0)
            assert(handle:join() == "hello")
        })
        .exec_async()
        .await
        .unwrap();

        // Test named task
        lua.load(chunk! {
            local handle = tasks.spawn({
                handler = function()
                    sleep(0.1)
                    return "hello2"
                end,
                name = "test_task",
            })
            assert(handle.id > 0)
            assert(handle.name == "test_task")
            assert(handle:join() == "hello2")
        })
        .exec_async()
        .await
        .unwrap();

        // Test error inside task
        lua.load(chunk! {
            local handle = tasks.spawn(function()
                error("error inside task")
            end)
            local ok, err = handle:join()
            assert(not ok)
            assert(err:find("error inside task"))
        })
        .exec_async()
        .await
        .unwrap();

        // Test aborting task
        lua.load(chunk! {
            local result
            local handle = tasks.spawn(function()
                sleep(0.1)
                result = "hello"
            end)
            assert(handle:is_finished() == false)
            handle:abort()
            sleep(0.2)
            assert(result == nil)
        })
        .exec_async()
        .await
        .unwrap();

        // Test max background tasks
        lua.load(chunk! {
            local handle1, err1 = tasks.spawn(function()
                sleep(0.1)
                result = "task1"
            end)
            assert(handle1)
            assert(not err1)
            local handle2, err2 = tasks.spawn(function()
                sleep(0.1)
                result = "task2"
            end)
            assert(handle2)
            assert(not err2)
            sleep(0.1)
            local handle3, err3 = tasks.spawn(function()
                sleep(0.1)
                result = "task3"
            end)
            assert(not handle3)
            assert(err3:find("max background task limit reached"))
            sleep(0.1)
        })
        .exec_async()
        .await
        .unwrap();

        // Test timeout
        lua.load(chunk! {
            local handler1, err1 = tasks.spawn({
                handler = function()
                    sleep(0.1)
                    return "hello"
                end,
                name = "test_no_timeout",
                timeout = 200,
            })
            assert(handler1)
            assert(not err1)
            sleep(0.1)
            local ok1, err1_2 = handler1:join()
            assert(ok1 == "hello")
            assert(not err1_2)
            local handler2, err2 = tasks.spawn({
                handler = function()
                    sleep(0.3)
                    return "hello"
                end,
                name = "test_timeout",
                timeout = 200,
            })
            assert(handler2)
            assert(not err2)
            local ok2, err2_2 = handler2:join()
            assert(not ok2)
            assert(err2_2:find("task exceeded timeout"))
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }
}

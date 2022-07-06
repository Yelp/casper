use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use mlua::{Function, Lua, RegistryKey, Result, Table, UserData};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::warn;

// TODO for tasks:
// Add API to Lua to track running tasks and abort them
// Support named tasks
// Support task timeouts

#[derive(Debug)]
struct Task {
    id: usize,
    handler: RegistryKey,
}

struct TaskHandle(usize);

struct NextTaskId(AtomicUsize);

impl UserData for TaskHandle {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("id", |_, this| Ok(this.0));
    }

    // TODO: stop
}

fn register_task(lua: &Lua, task: Function) -> Result<TaskHandle> {
    let tx = lua
        .app_data_ref::<UnboundedSender<Task>>()
        .expect("cannot get task sender");

    let next_task_id = lua
        .app_data_ref::<NextTaskId>()
        .expect("cannot get next task id");

    let task_id = next_task_id.0.fetch_add(1, Ordering::Relaxed);
    tx.send(Task {
        id: task_id,
        handler: lua.create_registry_value(task)?,
    })
    .expect("cannot send task");

    Ok(TaskHandle(task_id))
}

pub fn spawn_tasks(lua: Rc<Lua>) {
    let mut rx = lua
        .remove_app_data::<UnboundedReceiver<Task>>()
        .expect("cannot get task receiver");

    tokio::task::spawn_local(async move {
        while let Some(task) = rx.recv().await {
            let lua2 = lua.clone();
            tokio::task::spawn_local(async move {
                if let Ok(task_fn) = lua2.registry_value::<Function>(&task.handler) {
                    if let Err(err) = task_fn.call_async::<_, ()>(()).await {
                        warn!("task #{} error: {:?}", task.id, err);
                    }
                }
            });
        }
    });
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    // Create channel to send tasks
    let (tx, rx) = mpsc::unbounded_channel::<Task>();
    lua.set_app_data(tx);
    lua.set_app_data(rx);

    // Create NextTaskId counter
    lua.set_app_data(NextTaskId(AtomicUsize::new(0)));

    lua.create_table_from([("register_task", lua.create_function(register_task)?)])
}

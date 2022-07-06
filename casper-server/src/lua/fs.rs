use std::time::UNIX_EPOCH;

use mlua::{Lua, Result, Table};

async fn get_metadata(lua: &'_ Lua, path: String) -> Result<Table<'_>> {
    let metadata = tokio::fs::metadata(path).await?;
    let table = lua.create_table()?;

    if let Ok(accessed) = metadata.accessed() {
        if let Ok(accessed) = accessed.duration_since(UNIX_EPOCH) {
            table.set("accessed", accessed.as_secs())?;
        }
    }
    if let Ok(created) = metadata.created() {
        if let Ok(created) = created.duration_since(UNIX_EPOCH) {
            table.set("created", created.as_secs())?;
        }
    }
    if let Ok(modified) = metadata.modified() {
        if let Ok(modified) = modified.duration_since(UNIX_EPOCH) {
            table.set("modified", modified.as_secs())?;
        }
    }

    Ok(table)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([("get_metadata", lua.create_async_function(get_metadata)?)])
}

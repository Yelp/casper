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

/// Returns a table containing a list of files within a given path
///
/// # Arguments
///
/// * `path` - A string that contains the path of the directory to read
async fn get_files_in_dir(lua: &'_ Lua, path: String) -> Result<Table<'_>> {
    let mut dir_read = tokio::fs::read_dir(path).await?;
    let table = lua.create_table()?;
    let mut i = 1;
    while let Some(dir_entry) = dir_read.next_entry().await? {
        if dir_entry.metadata().await?.is_file() {
            let file_string = dir_entry
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned();
            table.set(i, file_string)?;
            i += 1;
        }
    }
    Ok(table)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("get_metadata", lua.create_async_function(get_metadata)?),
        (
            "get_files_in_dir",
            lua.create_async_function(get_files_in_dir)?,
        ),
    ])
}

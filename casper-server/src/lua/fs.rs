use std::result::Result as StdResult;
use std::time::UNIX_EPOCH;

use mlua::{Function, Lua, Result, String as LuaString, Table, Value};

/// Reads the entire contents of a file and returns a Lua string.
///
/// In case of error, returns nil and a string containing the error message.
async fn read(lua: Lua, path: String) -> Result<StdResult<LuaString, String>> {
    let data = lua_try!(tokio::fs::read(path).await);
    Ok(Ok(lua.create_string(data)?))
}

/// Writes the entire contents to a file.
///
/// In case of error, returns nil and a string containing the error message.
async fn write(_: Lua, (path, data): (String, LuaString)) -> Result<StdResult<bool, String>> {
    lua_try!(tokio::fs::write(path, data.as_bytes()).await);
    Ok(Ok(true))
}

/// Queries the file system to get information about a file, directory, etc.
///
/// Returns a table containing the following fields:
///     - accessed: The time the file was last accessed
///     - created: The time the file was created
///     - modified: The time the file was last modified
///     - is_dir: Whether the path points to a directory
///     - is_file: Whether the path points to a file
///     - is_symlink: Whether the path points to a symlink
///     - len: The length of the file
///
/// In case of error, returns nil and a string containing the error message.
async fn metadata(lua: Lua, path: String) -> Result<StdResult<Table, String>> {
    let metadata = lua_try!(tokio::fs::metadata(path).await);

    let table = lua.create_table()?;
    if let Ok(accessed) = metadata.accessed() {
        if let Ok(accessed) = accessed.duration_since(UNIX_EPOCH) {
            table.raw_set("accessed", accessed.as_secs())?;
        }
    }
    if let Ok(created) = metadata.created() {
        if let Ok(created) = created.duration_since(UNIX_EPOCH) {
            table.raw_set("created", created.as_secs())?;
        }
    }
    if let Ok(modified) = metadata.modified() {
        if let Ok(modified) = modified.duration_since(UNIX_EPOCH) {
            table.raw_set("modified", modified.as_secs())?;
        }
    }
    table.raw_set("is_dir", metadata.is_dir())?;
    table.raw_set("is_file", metadata.is_file())?;
    table.raw_set("is_symlink", metadata.is_symlink())?;
    table.raw_set("len", metadata.len())?;

    Ok(Ok(table))
}

/// Reads the contents of a directory and returns a Lua table containing the file names.
///
/// In case of error, returns nil and a string containing the error message.
async fn read_dir(lua: Lua, path: String) -> Result<StdResult<Table, String>> {
    let result = tokio::task::spawn_blocking(move || {
        let dir = std::fs::read_dir(path)?;
        dir.into_iter()
            .map(|r| r.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<std::io::Result<Vec<_>>>()
    })
    .await;
    let files = lua_try!(lua_try!(result));
    Ok(Ok(lua.create_sequence_from(files)?))
}

/// Creates a temporary directory and calls the given function with its path.
///
/// The directory will be automatically deleted when the function returns.
///
/// In case of error, returns nil and a string containing the error message.
async fn tempdir_scope(_: Lua, f: Function) -> Result<StdResult<Value, String>> {
    let dir = lua_try!(tempfile::tempdir());
    Ok(Ok(f.call_async(dir.path().display().to_string()).await?))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("read", lua.create_async_function(read)?),
        ("write", lua.create_async_function(write)?),
        ("metadata", lua.create_async_function(metadata)?),
        ("read_dir", lua.create_async_function(read_dir)?),
        ("tempdir_scope", lua.create_async_function(tempdir_scope)?),
    ])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[tokio::test]
    async fn test_fs() -> Result<()> {
        let lua = Lua::new();

        let fs = super::create_module(&lua)?;
        lua.load(chunk! {
            $fs.tempdir_scope(function (dir)
                // Read non-existent file
                local ok, err = $fs.read(dir .. "/foo.txt")
                assert(ok == nil and err ~= nil)

                // Write file
                local ok, err = $fs.write(dir .. "/foo.txt", "Hello, world!")
                assert(ok and err == nil)

                // Read it back
                local ok, err = $fs.read(dir .. "/foo.txt")
                assert(ok == "Hello, world!" and err == nil)

                // Read file metadata
                local ok, err = $fs.metadata(dir .. "/foo.txt")
                assert(err == nil)
                assert(type(ok.accessed) == "number")
                assert(type(ok.created) == "number")
                assert(type(ok.modified) == "number")
                assert(ok.is_dir == false)
                assert(ok.is_file == true)
                assert(ok.is_symlink == false)
                assert(ok.len == 13)

                // Read dir metadata
                local ok, err = $fs.metadata(dir)
                assert(err == nil)
                assert(ok.is_dir == true)

                // Read files
                assert(table.concat($fs.read_dir(dir), ",") == "foo.txt")
            end)
        })
        .exec_async()
        .await
    }
}

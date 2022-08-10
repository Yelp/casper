use mlua::{Lua, Result, Table};
use tracing::{error, info, warn};

fn info(_: &'_ Lua, message: String) -> Result<()> {
    info!(message);
    Ok(())
}

fn warn(_: &'_ Lua, message: String) -> Result<()> {
    warn!(message);
    Ok(())
}

fn error(_: &'_ Lua, message: String) -> Result<()> {
    error!(message);
    Ok(())
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("info", lua.create_function(info)?),
        ("warn", lua.create_function(warn)?),
        ("error", lua.create_function(error)?),
    ])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_module() -> Result<()> {
        let lua = Lua::new();

        let log = super::create_module(&lua)?;
        lua.load(chunk! {
            $log.info("test info")
            $log.warn("test warn")
            $log.error("test error")
        })
        .exec()?;

        Ok(())
    }
}

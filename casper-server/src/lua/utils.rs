use mlua::{Lua, Result, Table};

fn random(_: &Lua, upper_bound: Option<u32>) -> Result<u32> {
    let n = rand::random::<u32>();
    match upper_bound {
        Some(upper_bound) => Ok(n % upper_bound),
        None => Ok(n),
    }
}

fn random_string(_: &Lua, (len, mode): (usize, Option<String>)) -> Result<String> {
    Ok(crate::utils::random_string(len, mode.as_deref()))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        ("random", lua.create_function(random)?),
        ("random_string", lua.create_function(random_string)?),
    ])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_module() -> Result<()> {
        let lua = Lua::new();

        let utils = super::create_module(&lua)?;
        lua.load(chunk! {
            local n = $utils.random()
            assert(type(n) == "number" and math.floor(n) == n)
            local s = $utils.random_string(5)
            assert(type(s) == "string" and #s == 5)
            assert($utils.random_string(5, "hex"):match("^[0-9a-f]+$"))
        })
        .exec()
        .unwrap();

        Ok(())
    }
}

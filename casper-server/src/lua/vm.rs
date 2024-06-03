use std::result::Result as StdResult;

use mlua::{Function, Lua, RegistryKey, Result, Table, UserData};

struct CompiledFunction(RegistryKey);

impl UserData for CompiledFunction {
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("with_environment", |lua, this, env: Table| {
            let function = lua.registry_value::<Function>(&this.0)?;
            let new_function = function.deep_clone();
            new_function.set_environment(env)?;
            let new_key = lua.create_registry_value(new_function)?;
            Ok(CompiledFunction(new_key))
        });

        methods.add_method("finish", |lua, this, ()| {
            lua.registry_value::<Function>(&this.0)
        });
    }
}

fn compile(
    lua: &Lua,
    (chunk, name): (String, Option<String>),
) -> Result<StdResult<CompiledFunction, String>> {
    let env = lua.create_table()?;
    env.set_readonly(true);
    let chunk = lua
        .load(chunk)
        .set_name(name.unwrap_or_else(|| "chunk".to_string()))
        .set_environment(env);
    let key = lua.create_registry_value(lua_try!(chunk.into_function()))?;
    Ok(Ok(CompiledFunction(key)))
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([("compile", lua.create_function(compile)?)])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Function, Lua, Result};

    #[ntex::test]
    async fn test() -> Result<()> {
        let lua = Lua::new();

        let sleep = Function::wrap_async(|_, ()| async {
            tokio::time::sleep(tokio::time::Duration::from_secs_f32(0.01)).await;
            Ok("done")
        });

        let vm = super::create_module(&lua)?;
        lua.load(chunk! {
            local func = $vm.compile("return \"hello, world\"", "test"):finish()
            assert(func() == "hello, world", "expected 'hello, world'")

            local find = $vm.compile("return string.find(...)", "string_find")
            local ok, err = pcall(find:finish(), "hello", "l")
            assert(not ok and err:find("attempt to index nil with") ~= nil, "function should have no environment")

            // Pass custom environment
            local find2 = find:with_environment({string = {find = string.find}}):finish()
            assert(find2("hello", "l") == 3, "expected 3")

            // Async function
            local async = $vm.compile("return sleep()", "async"):with_environment({sleep = $sleep}):finish()
            assert(async() == "done")

            // Invalid syntax
            local invalid, err = $vm.compile("(", "invalid")
            assert(invalid == nil, "`invalid` variable must be nil")
            assert(err:find("Expected identifier when parsing expression") ~= nil, "expected syntax error")
        })
        .exec_async()
        .await
    }
}

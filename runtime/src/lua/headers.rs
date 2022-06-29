use mlua::{Function, Lua, RegistryKey, Result as LuaResult, Table};

pub(crate) fn set_headers_metatable(lua: &Lua, headers: Table) -> LuaResult<()> {
    struct MetatableHelperKey(RegistryKey);

    if let Some(key) = lua.app_data_ref::<MetatableHelperKey>() {
        return lua.registry_value::<Function>(&key.0)?.call(headers);
    }

    // Create new metatable helper and cache it
    let metatable_helper: Function = lua
        .load(
            r#"
            local headers = ...
            local metatable = {
                -- A mapping from a normalized (all lowercase) header name to its
                -- first-seen case, populated the first time a header is seen.
                normalized_to_original_case = {},
            }

            -- Add existing keys
            for key in pairs(headers) do
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                metatable.normalized_to_original_case[normalized_key] = key
            end

            -- When looking up a key that doesn't exist from the headers table, check
            -- if we've seen this header with a different casing, and return it if so.
            metatable.__index = function(tbl, key)
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                local original_key = metatable.normalized_to_original_case[normalized_key]
                if original_key ~= nil and original_key ~= key then
                    return tbl[original_key]
                end
                return nil
            end

            -- When adding a new key to the headers table, check if we've seen this
            -- header with a different casing, and set that key instead.
            metatable.__newindex = function(tbl, key, value)
                local normalized_key = string.gsub(string.lower(key), '_', '-')
                local original_key = metatable.normalized_to_original_case[normalized_key]
                if original_key == nil then
                    metatable.normalized_to_original_case[normalized_key] = key
                    original_key = key
                end
                rawset(tbl, original_key, value)
            end

            setmetatable(headers, metatable)
        "#,
        )
        .into_function()?;

    // Store the helper in the Lua registry
    let registry_key = lua.create_registry_value(metatable_helper.clone())?;
    lua.set_app_data(MetatableHelperKey(registry_key));

    metatable_helper.call(headers)
}

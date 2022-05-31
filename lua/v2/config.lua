local core = require("core")

-- TODO: Rename this variables
local SRV_CONFIGS_PATH = os.getenv("SRV_CONFIGS_PATH") or "/nail/srv/configs/spectre"
local SERVICES_YAML_PATH = os.getenv("SERVICES_YAML_PATH") or "/nail/etc/services/services.yaml"
local ENVOY_CONFIGS_PATH = os.getenv("ENVOY_CONFIGS_PATH") or "/nail/srv/configs"

local CACHED_CONFIGS = {}
local CONFIGS_RELOAD_DELAY = 10

core.tasks.register_task(function()
    while true do
        core.sleep(CONFIGS_RELOAD_DELAY)

        for file_path, data in pairs(CACHED_CONFIGS) do
            pcall(function()
                local metadata = core.fs.get_metadata(file_path)
                if metadata.modified ~= data.modified then
                    data.value = core.config.get_config(file_path)
                    data.modified = metadata.modified
                    print("reloaded '" .. file_path .. "'")
                end
            end)
        end
    end
end)

local function traverse_value(value, keys, idx)
    if value == nil or #keys < idx then
        return value
    end

    local next_value = value[keys[idx]]
    if next_value ~= nil then
        return traverse_value(next_value, keys, idx + 1)
    end
end

local function get_config(file_path, ...)
    if not CACHED_CONFIGS[file_path] then
        local ok, err = pcall(function()
            local metadata = core.fs.get_metadata(file_path)
            local value = core.config.get_config(file_path)

            CACHED_CONFIGS[file_path] = {
                modified = metadata.modified,
                value = value,
            }
        end)
        if not ok then
            print("failed to load '" .. file_path .. "': " .. tostring(err))
            CACHED_CONFIGS[file_path] = {}
        end
    end

    return traverse_value(CACHED_CONFIGS[file_path].value, { ... }, 1)
end

local function get_service_config(service, ...)
    return get_config(SRV_CONFIGS_PATH .. "/" .. service .. ".yaml", ...)
end

local function get_casper_config(...)
    return get_service_config("casper.internal", ...)
end

local function get_envoy_client_config(...)
    return get_config(ENVOY_CONFIGS_PATH .. "/envoy_client.yaml", ...)
end

return {
    get_service_config = get_service_config,
    get_casper_config = get_casper_config,
    get_envoy_client_config = get_envoy_client_config,

    SERVICES_YAML_PATH = SERVICES_YAML_PATH,
}

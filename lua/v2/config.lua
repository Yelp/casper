local core = require("core")

-- TODO: Rename this variables
local SRV_CONFIGS_PATH = os.getenv("SRV_CONFIGS_PATH") or "/nail/srv/configs/spectre"
local SERVICES_YAML_PATH = os.getenv("SERVICES_YAML_PATH") or "/nail/etc/services/services.yaml"
local ENVOY_CONFIGS_PATH = os.getenv("ENVOY_CONFIGS_PATH") or "/nail/srv/configs"

local function get_service_config(path, ...)
    return core.config.get_config(SRV_CONFIGS_PATH .. "/" .. path .. ".yaml", ...)
end

local function get_casper_config(...)
    return get_service_config("casper.internal", ...)
end

return {
    get_service_config = get_service_config,
    get_casper_config = get_casper_config,

    SERVICES_YAML_PATH = SERVICES_YAML_PATH,
    ENVOY_CONFIGS_PATH = ENVOY_CONFIGS_PATH,
}

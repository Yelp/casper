local lyaml = require 'lyaml'
local lfs = require 'lfs'
local util = require 'util'

local SRV_CONFIGS_PATH = os.getenv('SRV_CONFIGS_PATH')
local SERVICES_YAML_PATH = os.getenv('SERVICES_YAML_PATH')
local CASPER_INTERNAL_NAMESPACE = 'casper.internal'

local RELOAD_DELAY = 30  -- seconds

-- _mod_time_table keeps track of files and their last modification time to
-- trigger reload only if files change
--
-- _mod_time_table = {
--   '/nail/etc/services/services.yaml' = <timestamp>,
--   '/nail/srv/configs/spectre/service.namespace.yaml' = <timestamp>,
-- }
--
local _mod_time_table = {}

local function get_mod_time_table()
    return _mod_time_table
end

local function set_mod_time(file, mod_time)
    _mod_time_table[file] = mod_time
end

local function clear_mod_time_table()
    _mod_time_table = {}
end

-- _smartstack_info contains the entire /nail/etc/services/services.yaml file
--
-- _smartstack_info = {
--   'service1.main' = { 'host' = '1.2.3.4', 'port' = 1000 },
--   'service2.main' = { 'host' = '1.2.3.4', 'port' = 1001 },
--   'service2.canary' = { 'host' = '1.2.3.4', 'port' = 1002 },
-- }
local _smartstack_info = {}

local function get_smartstack_info_for_namespace(service_name)
    return _smartstack_info[service_name]
end

local function set_smartstack_info_for_namespace(namespace, info)
    _smartstack_info[namespace] = info
end

local function get_all_smartstack_info()
    return _smartstack_info
end

-- Checks the presence of smartstack info for healthcheck purposes
local function has_smartstack_info()
    return util.is_non_empty_table(_smartstack_info)
end

-- _services_configs: contains the spectre configs for all services.
--
-- _services_configs = {
--   'service1.main' = { 'uncacheable_headers' = ['XY'], 'caches' = [{ttl: 100, pattern: ^/category_yelp/.*$}] },
--   'service2.main' = { 'uncacheable_headers' = ['XYZ'], 'caches' = [{ttl: 100, pattern: ^/category_yelp/.*$}] },
--   'service2.canary' = { 'caches' = [{ttl: 100, pattern: ^/category_yelp/.*$}] },
-- }
local _services_configs = {}

local function set_spectre_config_for_namespace(namespace, configs)
    _services_configs[namespace] = configs
end

local function get_spectre_config_for_namespace(namespace)
    return _services_configs[namespace]
end

-- Handy getter for /configs
local function get_all_spectre_configs()
    return _services_configs
end

-- Checks the presence of configs for healthchecks purposes
local function has_spectre_configs()
    return util.is_non_empty_table(_services_configs)
end

-- Load configuration from YAML file. If file is missing or YAML is invalid,
-- return nil. Otherwise, return configuration as a dictionary.
local function parse_configs(config_file_path)
    if not config_file_path then
        ngx.log(ngx.ERR, 'No file provided. Cannot parse configs.')
        return nil
    end

    -- If the file hasn't changed since last read, exit
    local mod_time = lfs.attributes(config_file_path, 'modification')
    if _mod_time_table[config_file_path] ~= nil and
            mod_time == _mod_time_table[config_file_path] then
        return nil
    end
    _mod_time_table[config_file_path] = mod_time

    ngx.log(ngx.INFO, 'File has changed, reloading: ' .. config_file_path)

    local config_file = io.open(config_file_path, 'r')
    if not config_file then
        ngx.log(ngx.ERR, 'File missing, cannot parse: ' .. config_file_path)
        return nil
    end

    local yaml_content = config_file:read('*a')
    config_file:close()

    return lyaml.load(yaml_content)
end

-- Load all services configs
local function load_services_configs(path)
    for file in lfs.dir(path) do
        local file_path = path..'/'..file
        if lfs.attributes(file_path, 'mode') == 'file' and
                ngx.re.match(file, '.yaml$') then
            local config = parse_configs(file_path)
            if config ~= nil then
                local service_namespace = string.gsub(file, '.yaml', '')
                set_spectre_config_for_namespace(service_namespace, config)
            end
        end
    end
end

-- Load the smartstack information from disk if the file has changed
local function load_smartstack_info(path)
    local new_smartstack_info = parse_configs(path)

    -- Only update the global table if the configs have changed
    if new_smartstack_info ~= nil then
        for namespace, smartstack_info in pairs(new_smartstack_info) do
            set_smartstack_info_for_namespace(namespace, smartstack_info)
        end
    end
end

-- Triggers configs reload
local function reload_configs(premature)
    load_services_configs(SRV_CONFIGS_PATH)
    load_smartstack_info(SERVICES_YAML_PATH)

    -- https://github.com/openresty/lua-nginx-module#ngxtimerat
    if premature then
        return
    end

    local ok, err = ngx.timer.at(RELOAD_DELAY, reload_configs)
    if not ok then
        ngx.log(ngx.ERR, "failed to create the timer: ", err)
        return
    end
end


return {
    parse_configs = parse_configs,
    load_services_configs = load_services_configs,
    load_smartstack_info = load_smartstack_info,
    reload_configs = reload_configs,

    get_mod_time_table = get_mod_time_table,
    set_mod_time = set_mod_time,
    clear_mod_time_table = clear_mod_time_table,

    get_smartstack_info_for_namespace = get_smartstack_info_for_namespace,
    set_smartstack_info_for_namespace = set_smartstack_info_for_namespace,
    get_all_smartstack_info = get_all_smartstack_info,
    has_smartstack_info = has_smartstack_info,

    get_spectre_config_for_namespace = get_spectre_config_for_namespace,
    set_spectre_config_for_namespace = set_spectre_config_for_namespace,
    get_all_spectre_configs = get_all_spectre_configs,
    has_spectre_configs = has_spectre_configs,

    CASPER_INTERNAL_NAMESPACE = CASPER_INTERNAL_NAMESPACE,
}

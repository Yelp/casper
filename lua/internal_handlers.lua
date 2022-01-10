local datastores = require 'datastores'
local json = require 'vendor.json'
local config_loader = require 'config_loader'
local spectre_common = require 'spectre_common'

local cassandra_helper = datastores.cassandra_helper
local _swagger_json = nil

local dynamodb = require 'dynamodb_helper'

-- Handle PURGE requests
local function _purge_handler(smartstack_destination)
    local uri_args = ngx.req.get_uri_args()
    -- Delete smartstack_destination once biz_claims is using the generated clientlib PERF-2453
    local namespace = uri_args['namespace'] or smartstack_destination
    local cache_name = uri_args['cache_name']
    local id = uri_args['id']

    if namespace == nil or cache_name == nil then
        return ngx.HTTP_BAD_REQUEST, 'namespace and cache_name are required arguments', nil
    end

    if config_loader.get_smartstack_info_for_namespace(namespace) == nil then
        return ngx.HTTP_BAD_REQUEST, string.format('Unknown namespace %s', namespace), nil
    end

    local configs = config_loader.get_spectre_config_for_namespace(namespace)
    if configs == nil or configs['cached_endpoints'] == nil or
            configs['cached_endpoints'][cache_name] == nil then
        return ngx.HTTP_BAD_REQUEST,
            string.format('Unknown cache_name %s for namespace %s', cache_name, namespace),
            nil
    end

    local status, body = spectre_common.purge_cache(
        cassandra_helper,
        namespace,
        cache_name,
        id
    )

    return status, body, nil
end

-- Handle requests to /status, returns info about Spectre
local function status_handler(_)
    local status_info = {
        ['cassandra_status'] = 'skipped',
    }
    local status = ngx.HTTP_OK
    local uri_args = ngx.req.get_uri_args()

    -- Check Cassandra's health only if check_cassandra=true is set
    if uri_args['check_cassandra'] == 'true' then
        local connection = cassandra_helper.get_connection(cassandra_helper.READ_CONN)
        local is_cassandra_healthy = cassandra_helper.healthcheck(connection)
        if is_cassandra_healthy == true then
            status_info['cassandra_status'] = 'up'
        else
            status_info['cassandra_status'] = 'down'
            status = ngx.HTTP_INTERNAL_SERVER_ERROR
        end

        local read_conn = cassandra_helper.get_connection(cassandra_helper.READ_CONN)
        local peers = read_conn.get_peers(read_conn)
        local nodes_status = {}
        for i = 1, #peers do
            table.insert(nodes_status, {
                host = peers[i].host,
                data_center = peers[i].data_center,
                up = peers[i].up,
                err = peers[i].err,
            })
        end
        status_info['cassandra_nodes'] = nodes_status

    end

    -- Ensure config file at /nail/etc/services/services.yaml is parsed
    if config_loader.has_smartstack_info() ~= true then
        status_info['smartstack_configs'] = 'missing'
        status = ngx.HTTP_INTERNAL_SERVER_ERROR
    else
        status_info['smartstack_configs'] = 'present'
    end

    -- Ensure configs at /nail/srv/configs/spectre/* are parsed
    if config_loader.has_spectre_configs() ~= true then
        status_info['spectre_configs'] = 'missing'
        status = ngx.HTTP_INTERNAL_SERVER_ERROR
    else
        status_info['spectre_configs'] = 'present'
    end

    -- Gather all the host/port pairs for the services proxied by Spectre
    local proxied_services = {}
    for namespace in pairs(config_loader.get_all_spectre_configs()) do
        if namespace ~= 1 then
            local info = config_loader.get_smartstack_info_for_namespace(namespace)
            if info == nil and namespace ~= config_loader.CASPER_INTERNAL_NAMESPACE
                    and namespace ~= config_loader.ENVOY_NAMESPACE then
                proxied_services[namespace] = 'missing'
                status = ngx.HTTP_INTERNAL_SERVER_ERROR
            else
                proxied_services[namespace] = info
            end
        end
    end
    status_info['proxied_services'] = proxied_services

    local body = json:encode(status_info, false, {})
    return status, body, {['Content-Type'] = 'application/json'}
end

-- Handle requests to /configs
local function configs_handler(_)
    local configs = {}
    configs['service_configs'] = config_loader.get_all_spectre_configs()
    configs['smartstack_configs'] = {}
    -- Only add useful smartstack info here otherwise we return a super long response
    for k, _ in pairs(configs['service_configs']) do
        configs['smartstack_configs'][k] = config_loader.get_all_smartstack_info()[k]
    end
    configs['mod_time_table'] = config_loader.get_mod_time_table()
    configs['worker_id'] = ngx.worker.id()

    return ngx.HTTP_OK, json:encode(configs), {['Content-Type'] = 'application/json'}
end

-- Handles requests to /swagger.json
local function swagger_handler(_)
    if _swagger_json == nil then
        local fp, err = io.open('api_docs/swagger.json', 'r')
        if err ~= nil then
            return ngx.HTTP_INTERNAL_SERVER_ERROR, err
        end
        _swagger_json = fp:read('*all')  -- reads the entire file
        fp:close()
    end

    return ngx.HTTP_OK, _swagger_json, {['Content-Type'] = 'application/json'}
end

-- Handle requests to URLs only used in itests.
-- There are some special actions taken once these URLs have
-- responded correctly:
-- (1) /internal_error/dogslow: Spectre sleeps for a while after responding to request
-- (2) /internal_error/crash: Spectre crashes after responding to request
-- Both these special cases are used in itests to confirm that Spectre's slowness/errors
-- don't impact successful requests.
local function itest_urls_handler(_)
    return ngx.HTTP_OK, 'OK', nil
end

-- Handler calls to endpoints not implemented internally by Spectre
local function not_found_handler(_)
    local body = 'Not found: ' .. ngx.var.request_method .. ' ' .. ngx.var.request_uri
    for key, val in pairs(ngx.req.get_headers()) do
        body = body .. '  ' .. key .. ': ' .. val
    end

    return ngx.HTTP_NOT_FOUND, body, nil
end

-- When a request is not cacheable, there are certain endpoints that have special
-- meaning in Spectre. This router handles all those endpoints.
local function router(_, namespace)
    local handlers = {
       ['GET /status'] = status_handler,
       ['GET /stats'] = dynamodb.stats_handler,
       ['GET /configs'] = configs_handler,
       ['GET /internal_error/dogslow'] = itest_urls_handler,
       ['GET /internal_error/crash'] = itest_urls_handler,
       ['GET /swagger.json'] = swagger_handler,
       ['PURGE /'] = _purge_handler,
       ['DELETE /purge'] = _purge_handler,
    }

    local key = ngx.var.request_method .. ' ' .. ngx.var.uri
    local fn = handlers[key] or not_found_handler

    local status, body, headers = fn(namespace)
    local name, _ = debug.getlocal(1, 1, fn)

    -- This cacheability_info table is used to emit post-request metrics in
    -- metrics_helper.emit_post_request_metrics
    local cacheability_info = {is_cacheable = false, internal_handler = name}

    return {
        status = status,
        body = body,
        headers = headers,
        cacheability_info = cacheability_info
    }
end

return {
    _purge_handler = _purge_handler,
    router = router,
}

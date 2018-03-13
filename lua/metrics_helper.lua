local config_loader = require 'config_loader'
local socket = require('socket')

local _sock
local _default_dimensions


local function _get_sock()
    if _sock == nil then
        local configs = config_loader.get_spectre_config_for_namespace('casper.internal')['yelp_meteorite']
        _sock = socket.udp()
        _sock:setsockname("*", 0)
        _sock:setpeername(
            configs['metrics-relay']['host'],
            configs['metrics-relay']['port']
        )
    end
    return _sock
end

local function get_system_dimension(name)
    local configs = config_loader.get_spectre_config_for_namespace('casper.internal')['yelp_meteorite']
    return io.open(configs['etc_path'] .. '/' .. name):read()
end

local function _get_default_dimensions()
    if _default_dimensions == nil then
        _default_dimensions = {
            {'habitat', get_system_dimension('habitat')},
            {'service_name', os.getenv('PAASTA_SERVICE')},
            {'instance_name', os.getenv('PAASTA_INSTANCE')},
        }
    end
    return _default_dimensions
end

local function send_to_metrics_relay(payload)
    _get_sock():send(payload)
end

-- Encodes a metric in the meteorite format
local function encode_metric(name, value, metric_type, dimensions)

    local metric_str = '['
    -- Add default dimensions first
    -- NOTE: right now it's not possible to override "default" dimensions
    for _, v in ipairs(_get_default_dimensions()) do
        metric_str = metric_str .. '["' .. v[1] .. '", "' .. v[2] .. '"],'
    end
    -- Then add custom dimensions
    for _, v in ipairs(dimensions) do
        metric_str = metric_str .. '["' .. v[1] .. '", "' .. tostring(v[2]) .. '"],'
    end

    -- Add metric name
    metric_str = metric_str .. '["metric_name", "' .. name .. '"]'
    -- Finally add the value and type
    metric_str = metric_str .. ']:' .. tostring(value) .. '|' .. metric_type

    return metric_str
end

-- Encodes the metric and sends it to the metrics relay
local function emit_metric(name, value, metric_type, dimensions)
    local payload = encode_metric(name, value, metric_type, dimensions)
    send_to_metrics_relay(payload)
end

-- Emits a timer metric
-- You can optionally specify custom dimensions here that will be
-- appended to the default dimensions
local function emit_timing(name, timing, dimensions)
    emit_metric(name, timing, 'ms', dimensions)
end

-- Emits a counter metric
-- You can optionally specify custom dimensions here that will be
-- appended to the default dimensions
local function emit_counter(name, dimensions)
    emit_metric(name, 1, 'c', dimensions)
end

-- Emit timer for a request
local function emit_request_timing(timing, namespace, cache_name, status)
    for _, c in pairs({cache_name, '__ALL__'}) do
        for _, n in pairs({namespace, '__ALL__'}) do
            local dimensions = {{'namespace', n}, {'cache_name', c}, {'status', status}}
            emit_timing('spectre.request_timing', timing, dimensions)
        end
    end
end

-- Emit metrics after response is sent, for external requests
local function emit_cache_metrics(start_time, end_time, namespace, response, status)
    if response.cacheability_info.is_cacheable then
        emit_request_timing(
            (end_time - start_time) * 1000,
            namespace,
            response.cacheability_info.cache_name,
            status
        )
    end

    if response.cacheability_info.reason == 'no-cache-header' then
        emit_counter('spectre.no_cache_header', {
            {'namespace', namespace},
            {'cache_name', response.cacheability_info.cache_name},
            {'reason', response.cacheability_info.reason},
        })
    end

    if response.cacheability_info.bulk_support then
        emit_counter('spectre.bulk_hit_rate', {
            {'namespace', namespace},
            {'cache_name', response.cacheability_info.cache_name},
            {'cache_status', response.headers['Spectre-Cache-Status'] }
        })

        if response.error and response.error:find('unable to process response; content-type is') then
            emit_counter('spectre.unprocessable_responses', {
                {'message', 'non_json_response'},
                {'status_code', status},
                {'namespace', namespace},
                {'cache_name', response.cacheability_info.cache_name},
            })
        end

        if status ~= 200 then
            emit_counter('spectre.unprocessable_responses', {
                {'message', 'unexpected_status_code'},
                {'status_code', status},
                {'namespace', namespace},
                {'cache_name', response.cacheability_info.cache_name},
            })
        end
    end
end

-- Emit metrics after response is sent, for internal requests
local function emit_internal_metrics(start_time, end_time, _, response, status)
    if response.cacheability_info.internal_handler then
        emit_timing(
            'spectre.internal_endpoint_timing',
            (end_time - start_time) * 1000,
            {{'handler', response.cacheability_info.internal_handler}}
        )
    end

    if status == ngx.HTTP_NOT_FOUND then
        emit_counter('spectre.errors', {
            {'message', 'spectre_endpoint_not_found'},
            {'status_code', ngx.HTTP_NOT_FOUND},
        })
    end
end

return {
    emit_timing = emit_timing,
    emit_counter = emit_counter,
    emit_request_timing = emit_request_timing,
    emit_internal_metrics = emit_internal_metrics,
    emit_cache_metrics = emit_cache_metrics,
    get_system_dimension = get_system_dimension,
    _get_sock = _get_sock,
}

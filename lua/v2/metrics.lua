local config = require("lua.v2.config")
local core = require("core")

local _default_dimensions
local _sock

local function _get_sock()
    if _sock == nil then
        local metrics_relay = config.get_casper_config("yelp_meteorite", "metrics-relay")
        _sock = core.udp.bind()
        _sock:connect(metrics_relay.host .. ":" .. metrics_relay.port)
    end
    return _sock
end

local function get_system_dimension(name)
    local configs = config.get_casper_config('yelp_meteorite', 'etc_path')
    return io.open(configs .. '/' .. name):read()
end

local function _get_default_dimensions()
    if _default_dimensions == nil then
        _default_dimensions = {
            {'habitat', get_system_dimension('habitat')},
            {'service_name', os.getenv('PAASTA_SERVICE')},
            {'instance_name', os.getenv('PAASTA_INSTANCE')},
            {'casper_version', "v2"},
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
local function emit_request_timing(timing, namespace, cache_name, status, cache_status)
    for _, c in pairs({cache_name, '__ALL__'}) do
        for _, n in pairs({namespace, '__ALL__'}) do
            local dimensions = {{'namespace', n}, {'cache_name', c}, {'status', status}, {'cache_status', cache_status}}
            emit_timing('spectre.request_timing', timing, dimensions)
        end
    end
end

-- Emit metrics after response is sent, for external requests
local function emit_cache_metrics(start_time, end_time, namespace, cacheability_info, cache_status, response_status)

    if cacheability_info.is_cacheable then
        emit_request_timing(
            (end_time - start_time) * 1000,
            namespace,
            cacheability_info.cache_name,
            response_status,
            cache_status
        )
    end

    if cacheability_info.reason == 'no-cache-header' then
        emit_counter('spectre.no_cache_header', {
            {'namespace', namespace},
            {'cache_name', cacheability_info.cache_name},
            {'reason', cacheability_info.reason},
        })
    end
end

return {
    emit_timing = emit_timing,
    emit_counter = emit_counter,
    emit_request_timing = emit_request_timing,
    emit_cache_metrics = emit_cache_metrics,
    get_system_dimension = get_system_dimension,
    _get_sock = _get_sock,
}

local config_loader = require 'config_loader'
local resty_random = require 'resty.random'
local resty_string = require 'resty.string'
local rfc5424 = require 'resty.rfc5424'
local socket = require('socket')


local zipkin = {}

-- Store header list in a module-level constant so we can access it in tests
zipkin['ZIPKIN_HEADERS'] = {
    'X-B3-TraceId',
    'X-B3-SpanId',
    'X-B3-ParentSpanId',
    'X-B3-Flags',
    'X-B3-Sampled'
}

local _sock

function zipkin._get_sock()
    if _sock == nil then
        _sock = socket.udp()
        _sock:setsockname("*", 0)
        local configs = config_loader.get_spectre_config_for_namespace(
            config_loader.CASPER_INTERNAL_NAMESPACE
        )['zipkin']
        _sock:setpeername(
            configs['syslog']['host'],
            configs['syslog']['port']
        )
    end
    return _sock
end


-- Generate random 16 character string
function zipkin.random_string()
    local binary_bytes = resty_random.bytes(8)
    return resty_string.to_hex(binary_bytes)
end


-- Gets Zipkin headers off incoming request and stores them in a table.
function zipkin.extract_zipkin_headers(incoming_headers)
    local headers = {}
    for index = 1, #zipkin['ZIPKIN_HEADERS'] do
        local header = zipkin['ZIPKIN_HEADERS'][index]
        headers[header] = incoming_headers[header]
    end
    return headers
end


-- If Zipkin span ID present in request headers, set parent ID to current
-- span ID and randomly generate span ID for child.
function zipkin.get_new_headers(incoming_zipkin_headers)
    local new_headers = {}
    local span_id = incoming_zipkin_headers['X-B3-SpanId']
    if span_id then
        new_headers['X-B3-ParentSpanId'] = span_id
        new_headers['X-B3-SpanId'] = zipkin.random_string()
    end
    return new_headers
end


-- Modify relevant Zipkin headers for downstream request.
function zipkin.inject_zipkin_headers(incoming_zipkin_headers)
    local new_zipkin_headers = zipkin.get_new_headers(incoming_zipkin_headers)
    local headers = {}
    for header_name, header_val in pairs(new_zipkin_headers) do
        ngx.req.set_header(header_name, header_val)
        headers[header_name] = header_val
    end

    return headers
end

-- If Zipkin headers exist, then log them to syslog. X-B3-Flags and X-B3-Sampled
-- are optional in the Zipkin spec, so we'll emit a '-' if they're not present.
-- Start and end times are in epoch seconds, but Zipkin wants them in microseconds.
function zipkin.emit_syslog(headers, start_time, end_time, response)
    if headers['X-B3-TraceId'] ~= nil and
            headers['X-B3-SpanId'] ~= nil and
            headers['X-B3-ParentSpanId'] ~= nil then

        local request_string = string.format('"%s %s %s"',
            ngx.var.request_method,
            ngx.var.request_uri,
            ngx.var.server_protocol
        )

        local spectre_cache_status
        -- For DELETEs which might not have any response, assume spectre_cache_status is miss
        if ngx.var.request_method == 'DELETE' then
            spectre_cache_status = "miss"
        else
            if response ~= nil then
                if response.headers ~= nil then
                    spectre_cache_status = response.headers['Spectre-Cache-Status'] or "miss"
                else
                    spectre_cache_status = "miss"
                end
            else
                spectre_cache_status = "miss"
            end
        end

        local message = string.format(
            'spectre/zipkin %s %s %s %s %s %d %d, client: %s, server: , cache_status: %s, request: %s',
            headers['X-B3-TraceId'],
            headers['X-B3-SpanId'],
            headers['X-B3-ParentSpanId'],
            headers['X-B3-Flags'] or '-',
            headers['X-B3-Sampled'] or '-',
            start_time * 1000000,
            end_time * 1000000,
            ngx.var.remote_addr,
            spectre_cache_status,
            request_string
        )

        -- RFC5424 is the syslog format. We encode the message and send it along to syslog2scribe
        local encoded_message = rfc5424.encode(
            "LOCAL0",
            "INFO",
            ngx.var.hostname,
            ngx.var.pid,
            "nginx_spectre",
            message
        )
        zipkin._get_sock():send(encoded_message)
    end
end

return zipkin

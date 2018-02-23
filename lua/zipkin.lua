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

local sock = socket.udp()
sock:setsockname("*", 0)
sock:setpeername(
    os.getenv("SYSLOG_HOST"),
    os.getenv("SYSLOG_PORT")
)


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
function zipkin.emit_syslog(headers, start_time, end_time)
    if headers['X-B3-TraceId'] ~= nil and
            headers['X-B3-SpanId'] ~= nil and
            headers['X-B3-ParentSpanId'] ~= nil then

        local request_string = string.format('"%s %s %s"',
            ngx.var.request_method,
            ngx.var.request_uri,
            ngx.var.server_protocol
        )

        local message = string.format(
            'spectre/zipkin %s %s %s %s %s %d %d, client: %s, server: , request: %s',
            headers['X-B3-TraceId'],
            headers['X-B3-SpanId'],
            headers['X-B3-ParentSpanId'],
            headers['X-B3-Flags'] or '-',
            headers['X-B3-Sampled'] or '-',
            start_time * 1000000,
            end_time * 1000000,
            ngx.var.remote_addr,
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
        sock:send(encoded_message)
    end
end

return zipkin

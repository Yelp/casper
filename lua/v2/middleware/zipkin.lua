local core = require("core")
local config = require("lua.v2.config")
local metrics = require("lua.v2.metrics")

local os_date = os.date
local string_format = string.format
local string_match = string.match

local now = core.datetime.now
local random_string = core.utils.random_string

local ZIPKIN_HEADERS = {
    "x-b3-traceid",
    "x-b3-spanid",
    "x-b3-parentspanid",
    "x-b3-flags",
    "x-b3-sampled",
}

local _sock
local function _get_sock()
    if _sock == nil then
        local syslog2scribe = config.get_casper_config("zipkin", "syslog")
        _sock = core.udp.bind()
        _sock:connect(syslog2scribe.host .. ":" .. syslog2scribe.port)
    end
    return _sock
end

--
-- Middleware methods
--

local function on_request(req, ctx)
    ctx.start_time = now()

    -- Get Zipkin headers off the incoming request and store them in a table.
    local zipkin_headers = {}
    for _, name in ipairs(ZIPKIN_HEADERS) do
        zipkin_headers[name] = req:header(name)
    end
    ctx.zipkin_headers = zipkin_headers

    -- Start a new span before proxying to a downstream service
    local span_id = zipkin_headers["x-b3-spanid"]
    if span_id then
        req:set_header("x-b3-parentspanid", span_id)
        req:set_header("x-b3-spanid", random_string(16, "hex"))
    end
end

local function on_response(resp, ctx)
    ctx.end_time = now()
    ctx.response_status = resp.status

    if resp.is_proxied then
        -- Nothing to do, new span has been created
        return
    else
        resp:set_header("x-zipkin-id", ctx.zipkin_headers["x-b3-traceid"])
    end
end

local function after_response(ctx)

    -- Emit metrics for hit rate, and latency
    -- Included in the Zipkin middleware as this is expected to be temporary
    metrics.emit_counter(
        'spectre.hit_rate',
        {
            {'namespace', ctx.destination},
            {'cache_name', ctx.cacheability_info.cache_name},
            {'cache_status', ctx.cache_status},
            {'backend', 'redis'}
        }
    )
    metrics.emit_cache_metrics(
        ctx.start_time:unix_timestamp(),
        ctx.end_time:unix_timestamp(),
        ctx.destination,
        ctx.cacheability_info,
        ctx.cache_status,
        ctx.response_status
    )

    -- Emit syslog
    -- If Zipkin headers exist, then log them to syslog.
    -- `X-B3-Flags` and `X-B3-Sampled` are optional in the Zipkin spec,
    -- so we'll emit a '-' if they're not present.
    -- Start and end times are in epoch seconds, but Zipkin wants them in microseconds.

    local zipkin_headers = ctx.zipkin_headers
    if
        zipkin_headers["x-b3-traceid"] == nil
        or zipkin_headers["x-b3-spanid"] == nil
        or zipkin_headers["x-b3-parentspanid"] == nil
    then
        return
    end

    -- For DELETEs which might not have any response, assume `spectre_cache_status` is miss
    local cache_status = ctx.cache_status
    if ctx.request_method == "DELETE" then
        cache_status = "miss"
    end

    local remote_addr = string_match(ctx.remote_addr, "^[^:]+")
    local message = string_format(
        'spectre/zipkin %s %s %s %s %s %d %d, client: %s, server: , cache_status: %s, request: "%s %s HTTP/1.1"',
        zipkin_headers["x-b3-traceid"],
        zipkin_headers["x-b3-spanid"],
        zipkin_headers["x-b3-parentspanid"],
        zipkin_headers["x-b3-flags"] or "-",
        zipkin_headers["x-b3-sampled"] or "-",
        ctx.start_time:unix_timestamp() * 1000000,
        ctx.end_time:unix_timestamp() * 1000000,
        remote_addr,
        cache_status,
        ctx.request_method,
        ctx.request_uri
    )

    -- We encode the message to RFC5424 format and send it to syslog2scribe.
    -- syslog2scribe doesn't actually care about the priority, timestamp, PID, or
    -- most other fields -- it really just looks at the "appname" (really the
    -- stream name) and the message. We just hard-code everything else for
    -- simplicity.
    local encoded_message = string_format(
        "<64>%s %s nginx_spectre[%d]: %s\n",
        os_date("%b %d %H:%M:%S", ctx.start_time:unix_timestamp()),
        core.hostname,
        core.pid,
        message
    )
    _get_sock():send(encoded_message)
end

return {
    on_request = on_request,
    on_response = on_response,
    after_response = after_response,
}

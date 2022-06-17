local caching_handlers = require 'caching_handlers'
local internal_handlers = require 'internal_handlers'
local itest_handlers = require 'itest_post_request_handlers'
local metrics_helper = require 'metrics_helper'
local socket = require 'socket'
local spectre_common = require 'spectre_common'
local traceback = require 'traceback'
local zipkin = require 'zipkin'
local casper_v2 = require 'v2_helper'
local config_loader = require 'config_loader'


-- The single point from where responses are sent back for proxied requests
local function send_response(status, body, headers, callback_fn)
    if headers then
        for name, val in pairs(headers) do
            ngx.header[name] = val
        end
    end

    -- EDGE-569, if the x-casper-sync header is set, we adjust the order of operations
    -- e.g. we store the response in the backend, then return the response to client
    -- used for solving itest order of operation issues which cause test failures
    if ngx.req.get_headers()['x-casper-sync'] ~= nil then
        if callback_fn then callback_fn() end

        ngx.status = status
        ngx.print(body)
        -- Send the response back to the client
        ngx.flush()
        ngx.eof()

    else
        ngx.status = status
        ngx.print(body)
        -- Send the response back to the client
        ngx.flush()
        ngx.eof()

        if callback_fn then callback_fn() end
    end
end

-- Called after response is sent; based on status and cacheability_info
-- metrics are sent. Also, Spectre's zipkin log line is emitted.
local function post_request(incoming_zipkin_headers, start_time, end_time, namespace, response, status, internal)
    local fn = itest_handlers.get_handler(ngx.var.request_uri)
    if fn then
        fn()
        return
    end

    if internal then
        metrics_helper.emit_internal_metrics(start_time, end_time, namespace, response, status)
    else
        metrics_helper.emit_cache_metrics(start_time, end_time, namespace, response, status)
    end

    zipkin.emit_syslog(incoming_zipkin_headers, start_time, end_time, response)
end

-- Handles any errors arising from processing in any part of Spectre
-- Logs errors and returns a 500 response.
local function err_handler(err)
    local tb = debug.traceback()
    spectre_common.log(ngx.ERR, traceback.format_critical(tb, err))
    return {
        status = ngx.HTTP_INTERNAL_SERVER_ERROR,
        body = tostring(err),
        cacheability_info = {}
    }
end

-- Wrapper to perform Zipkin and timing instrumentation.
-- Grabs Zipkin headers off the incoming request, mutates span and parent span
-- headers for downstream request, and optionally logs values to syslog for
-- out-of-band logging.
-- handler needs to be a table that contains status, body, headers, callback_function
local function request_handler_wrapper(handler, internal)
    -- Returns time in seconds since epoch, with slightly more than
    -- millisecond granularity
    local start_time = socket.gettime()

    local incoming_zipkin_headers = ngx.ctx.incoming_zipkin_headers
                                    or zipkin.extract_zipkin_headers(ngx.req.get_headers())
    local namespace = spectre_common.get_smartstack_destination(ngx.req.get_headers())

    -- Catch and format spectre handler errors.
    local _, res = xpcall(
        function() return handler(incoming_zipkin_headers, namespace) end,
        err_handler
    )

    send_response(res.status, res.body, res.headers, res.post_request)

    local end_time = socket.gettime()
    -- The code in this function is executed after the response is sent back to the client
    -- so slow operations or even crashes won't affect the response
    local success, err = xpcall(
        function()
            post_request(
                incoming_zipkin_headers,
                start_time,
                end_time,
                namespace,
                res,
                ngx.status,
                internal
            )
        end,
        debug.traceback
    )
    if not success then
        spectre_common.log(ngx.ERR, { err=err, critical=false })
    end
end

-- Spectre entry point
local function main()
    -- Route to main or purge endpoint depending on HTTP request method. This needs
    -- to be done in Lua because routing on HTTP method is not supported in nginx.
    local should_proxy, err = spectre_common.is_request_for_proxied_service(
        ngx.req.get_method(),
        ngx.req.get_headers()
    )

    if should_proxy then
        local incoming_zipkin_headers = zipkin.extract_zipkin_headers(ngx.req.get_headers())
        local cacheability_info, request_info = caching_handlers._parse_request(incoming_zipkin_headers)
        ngx.ctx.incoming_zipkin_headers = incoming_zipkin_headers
        ngx.ctx.cacheability_info = cacheability_info
        ngx.ctx.request_info = request_info

        if not(cacheability_info.is_cacheable and cacheability_info.cache_entry.bulk_support) then
            local spectre_config = config_loader.get_spectre_config_for_namespace(
                config_loader.CASPER_INTERNAL_NAMESPACE
            )
            local enabled_percent = spectre_config['v2_single_enabled_pct'] or 0

            if enabled_percent > 0 then
                -- Deterministic random
                local hash = ngx.md5(ngx.var.http_x_smartstack_destination .. ngx.var.request_uri):sub(1, 7)
                local hash_mod = math.fmod(tonumber(hash, 16), 100)

                if hash_mod < enabled_percent then
                    local err2 = casper_v2.forward_to_v2()
                    if err2 == nil then
                        return
                    end
                    spectre_common.log(ngx.ERR, {err=err2, critical=false, v2=true})
                end
            end
        end

        request_handler_wrapper(caching_handlers.caching_proxy, false)
    elseif err then
        metrics_helper.emit_counter('spectre.errors', {
            {'message', 'bad_request'},
            {'status_code', ngx.HTTP_BAD_REQUEST},
        })
        ngx.status = ngx.HTTP_BAD_REQUEST
        ngx.say(err)
        ngx.exit(ngx.HTTP_BAD_REQUEST)
    else
        request_handler_wrapper(internal_handlers.router, true)
    end
end

return {
    main = main,
}

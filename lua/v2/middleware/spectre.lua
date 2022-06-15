local core = require("core")
local common = require("lua/v2/common")
local config = require("lua/v2/config")
local filters = require("lua/v2/filters")

local Response = core.Response
local normalize_uri = core.utils.normalize_uri
local string_sub = string.sub

local function set_destination(req, destination)
    local uri = req.uri
    if config.get_casper_config("route_through_envoy") then
        local envoy_url = config.get_envoy_client_config("url")
        req:set_header("X-Yelp-Svc", destination)
        -- in `envoy_url`, we have a '/' at the end of the url, so we need to remove it
        req:set_destination(envoy_url .. string_sub(uri, 2))
    else
        local info = core.config.get_config(config.SERVICES_YAML_PATH, destination)
        req:set_destination("http://" .. info.host .. ":" .. info.port .. uri)
    end
end

--
-- Middleware methods
--

local function on_request(req, ctx)
    local destination = req:header("X-Smartstack-Destination")

    -- Is this request for a proxied service?
    if req:header_cnt("X-Smartstack-Source") == 0 or destination == nil then
        return Response(400, "missing `x-smartstack-source/destination`")
    end

    -- Set destination ahead of any logic to forward in case of exceptions
    set_destination(req, destination)
    local timeout = config.get_casper_config("http", "timeout_ms") or 60000
    req:set_timeout(timeout / 1000)

    -- Fill context table
    ctx.request_method = req.method
    ctx.request_uri = req.uri
    ctx.normalized_uri = normalize_uri(ctx.request_uri)
    ctx.remote_addr = req.remote_addr
    ctx.destination = destination
    ctx.service_config = config.get_service_config(destination)

    local cacheability_info = common.get_cacheability_info(req, ctx)
    ctx.cacheability_info = cacheability_info
    ctx.cache_status = cacheability_info.reason

    if cacheability_info.is_cacheable or cacheability_info.refresh_cache then
        ctx.primary_key = common.calculate_primary_key(req, ctx)
    end

    local use_filter = cacheability_info.cache_entry.use_filter
    if use_filter then
        ctx.filter = filters[use_filter]
    end
end

local function on_response(resp, ctx)
    resp:set_header("Spectre-Cache-Status", ctx.cache_status)
    if resp.is_proxied then
        resp:set_header("X-Original-Status", resp.status)
    end
end

return {
    on_request = on_request,
    on_response = on_response,
}

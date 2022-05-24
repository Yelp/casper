local core = require("core")
local common = require("lua.v2.common")

local cache_hits_counter = core.metrics.cache_hits_counter
local cache_misses_counter = core.metrics.cache_misses_counter

local function on_request(req, ctx)
    local ci = ctx.cacheability_info
    ctx.is_single_endpoint = (ci.is_cacheable or ci.refresh_cache) and not ci.cache_entry.bulk_support
    if not (ctx.is_single_endpoint and ci.is_cacheable) then
        return
    end

    local on_request_filter = ctx.filter and ctx.filter.on_request
    if on_request_filter then
        local filter_resp = on_request_filter(req, ctx)
        if filter_resp ~= nil then
            return filter_resp
        end
    end

    -- Fetch from cache
    return core.storage.primary:get_response(ctx.primary_key)
end

local function on_response(resp, ctx)
    if not ctx.is_single_endpoint then
        return
    end

    local dims = {namespace = ctx.destination, cache_name = ctx.cacheability_info.cache_name}

    if resp.is_cached then
        -- Cache hit
        ctx.cache_status = "hit"
        cache_hits_counter:add(1, dims)
    elseif resp.is_proxied then
        if resp.status == 200 then
            -- Cache miss, mark response for storing
            resp:use_after_response(true)
            ctx.cache_status = ctx.cache_status or "miss"
        else
            ctx.cache_status = string.format("non-cacheable-response: status code is %d", resp.status)
        end
        cache_misses_counter:add(1, dims)
    end

    local on_response_filter = ctx.filter and ctx.filter.on_response
    if on_response_filter then
        on_response_filter(resp, ctx)
    end
end

local function after_response(ctx, resp)
    if resp == nil or not ctx.is_single_endpoint then
        -- Nothing to cache
        return
    end

    -- Remove non-cacheable headers
    local service_config = ctx.service_config
    for _, name in ipairs(service_config.uncacheable_headers or {}) do
        resp:del_header(name)
    end

    ctx.surrogate_keys = common.calculate_surrogate_keys(resp, ctx)

    local after_response_filter = ctx.filter and ctx.filter.after_response
    if after_response_filter then
        after_response_filter(resp, ctx)
    end

    core.storage.primary:store_response({
        key = ctx.primary_key,
        surrogate_keys = ctx.surrogate_keys,
        response = resp,
        ttl = ctx.cacheability_info.cache_entry.ttl,
    })
end

return {
    on_request = on_request,
    on_response = on_response,
    after_response = after_response,
}

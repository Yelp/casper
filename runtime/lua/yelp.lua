local core = require("core")
local Response = core.Response

local function on_request(req)
    -- Is this request for a proxied service?
    if req:header_cnt("X-Smartstack-Source") ~= 1 or
        req:header_cnt("X-Smartstack-Destination") ~= 1 then
        -- TODO: emit metric
        return Response(400, "invalid or empty `x-smartstack-<source|destination>`")
    end

    -- TODO: Normalize uri
    local cache_info = is_cacheable(req)
    req.stash.cache_info = cache_info

    -- Remove accept-encoding header to work with text responses
    req:del_header("accept-encoding")

    if not cache_info.is_cacheable then
        -- Forward to downstream
        return
    end

    -- local cache_key = get_cache_key(require)

    return
end

local function on_response(resp)
end

local function after_response(resp)
    if resp.stash.cache_info.refresh then

    end
end

return {
    on_request = on_request,
    on_response = on_response,
    after_response = after_response,
}

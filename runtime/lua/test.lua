local core = require("core")

local function on_request(req, ctx)
    print("on_request")

    -- req:set_destination("127.0.0.1:8080")

    ctx.caching_key = req.uri

    -- Fetch from cache
    local resp = core.storage:get_response(ctx.caching_key)
    if resp ~= nil then
        print("got response from the cache")
        return resp
    end

    -- Make dummy response
    local resp = core.Response(200, "Ok")
    resp:set_header("hello", "world")
    return resp
end

local function on_response(resp, ctx)
    print("on_response")
    resp:use_after_response(true)
end

local function after_response(ctx, resp)
    print("after_response")

    -- Cache response
    if resp ~= nil and not resp.is_cached then
        print("caching response")
        core.storage:cache_response(ctx.caching_key, resp)
    end
end

return {
    on_request = on_request,
    on_response = on_response,
    after_response = after_response,
}

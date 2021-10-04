local core = require("core")

local function on_request(req, ctx)
    print("on_request")

    -- req:set_destination("127.0.0.1:8080")

    -- Make dummy response
    local resp = core.Response(200, "Ok")
    resp:set_header("hello", "world")
    return resp
end

local function on_response(resp, ctx)
    print("on_response")
end

local function after_response(ctx)
    print("after_response")
end

return {
    on_request = on_request,
    on_response = on_response,
    after_response = after_response,
}

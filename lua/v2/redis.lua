local core = require("core")

local Response = core.Response
local redis_pri = core.storage.primary
local redis_xdc = core.storage.crossdc

local table_insert = table.insert
local table_remove = table.remove
local string_sub = string.sub

local function make_primary_key(args)
    local primary_key = { args.cache_key, args.namespace, args.cache_name }
    if args.id then
        table_insert(primary_key, args.id)
    end
    if args.vary_headers then
        table_insert(primary_key, args.vary_headers)
    end
    return primary_key
end

local function make_surrogate_keys(args)
    local namespace_cache = args.namespace .. "|" .. args.cache_name
    local surrogate_keys = { namespace_cache }
    if args.id then
        table_insert(surrogate_keys, namespace_cache .. "|" .. args.id)
    end
    return surrogate_keys
end

local function on_request(req)
    local method, uri_path = req.method, req.uri_path

    if method == "GET" and uri_path == "/fetch_body_and_headers" then
        local primary_key = make_primary_key(req:uri_args())
        -- Fetch Response from Redis
        local resp = redis_pri:get_response(primary_key)
        if resp then
            -- Mark original headers using the prefix `x-res-`
            local headers = {}
            for k, v in pairs(resp:headers()) do
                headers["x-res-" .. k] = v
            end
            resp:set_headers(headers)
            return resp
        end
    elseif method == "POST" and uri_path == "/store_body_and_headers" then
        local args = req:uri_args()
        local primary_key = make_primary_key(args)
        local surrogate_keys = make_surrogate_keys(args)

        -- Extract original headers using the prefix `x-res-`
        local headers = {}
        for k, v in pairs(req:headers()) do
            if string_sub(k, 1, 6) == "x-res-" then
                headers[string_sub(k, 7)] = v
            end
        end

        -- Build Response for storing
        local resp = Response({
            headers = headers,
            body = req:body(),
        })

        -- Store it
        redis_pri:store_response({
            key = primary_key,
            surrogate_keys = surrogate_keys,
            response = resp,
            ttl = args.ttl,
        })

        return Response(200, "OK")
    elseif method == "DELETE" and uri_path == "/purge" then
        -- Only take last (less wide if `id` is given) key
        local surrogate_key = table_remove(make_surrogate_keys(req:uri_args()))

        redis_pri:delete_responses({ surrogate_keys = { surrogate_key } })
        if redis_xdc then
            redis_xdc:delete_responses({ surrogate_keys = { surrogate_key } })
        end

        return Response(200, "OK")
    end

    -- Make dummy response
    return Response(404, "Not found")
end

return {
    on_request = on_request,
}

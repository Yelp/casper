local resty_http = require("resty.http")

--
-- Casper v2 helper
--

local TIMEOUT = 1000
local BACKEND_ADDR = 'http://127.0.0.1:34567'

local HOP_BY_HOP_HEADERS = {
    ["connection"]          = true,
    ["keep-alive"]          = true,
    ["proxy-authenticate"]  = true,
    ["proxy-authorization"] = true,
    ["te"]                  = true,
    ["trailers"]            = true,
    ["transfer-encoding"]   = true,
    ["upgrade"]             = true,
}

local function forward_to_v2()
    local httpc = resty_http.new()
    httpc:set_timeouts(1000, 60000, 60000)

    local parsed_uri = httpc:parse_uri("http://nil" .. ngx.var.request_uri, false)
    local headers = ngx.req.get_headers()
    for h, _ in pairs(HOP_BY_HOP_HEADERS) do headers[h] = nil end

    local res, err = httpc:request_uri(BACKEND_ADDR, {
        method = ngx.req.get_method(),
        path = parsed_uri[4],
        query = parsed_uri[5],
        headers = headers,
        body = ngx.var.request_body or httpc:get_client_body_reader(),
    })
    if err ~= nil then return err end

    ngx.status = res.status
    for h, _ in pairs(HOP_BY_HOP_HEADERS) do res.headers[h] = nil end
    for key, val in pairs(res.headers) do ngx.header[key] = val end
    ngx.print(res.body)
    ngx.flush()
    ngx.eof()
end

local function fetch_body_and_headers(id, cache_key, namespace, cache_name, vary_headers)
    if id == "" or id == "null" then
        id = nil
    end

    local httpc = resty_http.new()
    httpc:set_timeout(TIMEOUT)
    local res, err = httpc:request_uri(BACKEND_ADDR, {
        method = "GET",
        path = "/fetch_body_and_headers",
        query = {
            id = id,
            cache_key = cache_key,
            namespace = namespace,
            cache_name = cache_name,
            vary_headers = vary_headers,
        },
        headers = { host = "casper.v2.redis" },
    })

    if err ~= nil or res.status ~= 200 then
        return {datastore_error = err ~= nil or res.status >= 500}
    end

    local body = res.body
    if body == '' then return {} end

    -- Extract original headers using the prefix `x-res-`
    local headers = {}
    for k, v in pairs(res.headers) do
        if string.sub(k, 1, 6) == "x-res-" then
            headers[string.sub(k, 7)] = v
        end
    end

    return {
        headers = headers,
        body = body,
    }
end

local function store_body_and_headers(ids, cache_key, namespace, cache_name,
                                            body, headers, vary_headers, ttl)
    local id = ids[1]
    if id == "" or id == "null" then
        id = nil
    end

    -- Mark original headers using the prefix `x-res-`
    local new_headers = { host = "casper.v2.redis" }
    for k, v in pairs(headers) do
        new_headers["x-res-" .. k] = v
    end

    local httpc = resty_http.new()
    httpc:set_timeout(TIMEOUT)
    local res, err = httpc:request_uri(BACKEND_ADDR, {
        method = "POST",
        path = "/store_body_and_headers",
        query = {
            id = id,
            cache_key = cache_key,
            namespace = namespace,
            cache_name = cache_name,
            vary_headers = vary_headers,
            ttl = ttl,
        },
        headers = new_headers,
        body = body,
    })
    if err ~= nil or res.status ~= 200 then
        error(err or ("bad status: "..res.status))
    end
end

local function purge(namespace, cache_name, id)
    if id == "" or id == "null" then
        id = nil
    end

    local httpc = resty_http.new()
    httpc:set_timeout(TIMEOUT)
    local res, err = httpc:request_uri(BACKEND_ADDR, {
        method = "DELETE",
        path = "/purge",
        query = {
            id = id,
            namespace = namespace,
            cache_name = cache_name,
        },
        headers = { host = "casper.v2.redis" },
    })

    if err ~= nil or res.status ~= 200 then
        return ngx.HTTP_INTERNAL_SERVER_ERROR, 'Failed to purge some keys. Check spectre logs'
    end

    local response = string.format(
        'Purged namespace: %s & cache_name: %s', namespace, cache_name
    )
    if id then
        response = string.format('%s & id: %s', response, id)
    end
    return ngx.HTTP_OK, response
end

-- Handle requests to /metrics, returns Prometheus metrics
local function metrics_handler()
    local httpc = resty_http.new()
    httpc:set_timeout(TIMEOUT)
    local res, err = httpc:request_uri(BACKEND_ADDR, {
        method = "GET",
        path = "/metrics",
    })

    if err ~= nil or res.status ~= 200 then
        return ngx.HTTP_INTERNAL_SERVER_ERROR, 'Failed to get metrics'
    end

    return ngx.HTTP_OK, res.body, res.headers
end

return {
    forward_to_v2 = forward_to_v2,
    store_body_and_headers = store_body_and_headers,
    fetch_body_and_headers = fetch_body_and_headers,
    purge = purge,
    metrics_handler = metrics_handler,
}

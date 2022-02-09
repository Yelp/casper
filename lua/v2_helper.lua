local resty_http = require("resty.http")

--
-- Casper v2 helper
--

local TIMEOUT = 1000
local BACKEND_ADDR = 'http://127.0.0.1:34567'

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
    })

    if err ~= nil or res.status ~= 200 then
        return {cassandra_error = err ~= nil or res.status >= 500}
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
    local new_headers = {}
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
    store_body_and_headers = store_body_and_headers,
    fetch_body_and_headers = fetch_body_and_headers,
    purge = purge,
    metrics_handler = metrics_handler,
}

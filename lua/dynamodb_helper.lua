local dynamodb = require("dynamodb")
local resty_http = require("resty.http")
local json = require("cjson")

--
-- DynamoDB plugin helper (via Rust)
--

local TIMEOUT = 1000

local function fetch_body_and_headers(id, cache_key, namespace, cache_name, vary_headers)
    local args = json.encode({
        method = "fetch_body_and_headers",
        id = id,
        cache_key = cache_key,
        namespace = namespace,
        cache_name = cache_name,
        vary_headers = vary_headers,
    })
    local httpc = resty_http.new()
    httpc:set_timeout(TIMEOUT)
    local res, err = httpc:request_uri(dynamodb.uri, {
        method = "POST",
        body = string.len(args).."|"..args,
    })

    if err ~= nil or res.status ~= 200 then
        return {cassandra_error = true}
    end

    local body = res.body
    if body == '' then return {} end

    -- Decode format: `<len>|<headers><body>`, where `len` is length of `headers`
    local headers_idx = string.find(body, "|", 1, true) + 1
    local headers_len = tonumber(string.sub(body, 1, headers_idx - 2))
    local headers = json.decode(string.sub(body, headers_idx, headers_idx + headers_len - 1))
    body = string.sub(body, headers_idx + headers_len)

    return {
        headers = headers,
        body = body,
    }
end

local function store_body_and_headers(ids, cache_key, namespace, cache_name,
                                            body, headers, vary_headers, ttl)
    local args = json.encode({
        method = "store_body_and_headers",
        id = ids[1],
        cache_key = cache_key,
        namespace = namespace,
        cache_name = cache_name,
        headers = headers,
        vary_headers = vary_headers,
        ttl = ttl,
    })
    local httpc = resty_http.new()
    httpc:set_timeout(TIMEOUT)
    local res, err = httpc:request_uri(dynamodb.uri, {
        method = "POST",
        body = string.len(args).."|"..args..body,
    })
    if err ~= nil or res.status ~= 200 then
        error(err or ("bad status: "..res.status))
    end
end

local function purge(namespace, cache_name, id)
    local args = json.encode({
        method = "purge",
        id = id,
        namespace = namespace,
        cache_name = cache_name,
    })
    local httpc = resty_http.new()
    httpc:set_timeout(TIMEOUT)
    local res, err = httpc:request_uri(dynamodb.uri, {
        method = "POST",
        body = string.len(args).."|"..args,
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

return {
    store_body_and_headers = store_body_and_headers,
    fetch_body_and_headers = fetch_body_and_headers,
    purge = purge,
}

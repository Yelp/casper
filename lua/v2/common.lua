local core = require("core")
local config = require("lua.v2.config")

local regex_new = core.regex.new
local json_decode = core.json.decode

local DEFAULT_REQUEST_METHOD = "GET"

local PLEASE_DO_NOT_CACHE_HEADERS = {
    ["x-strongly-consistent-read"] = "^(?i)1|true$",
    ["x-force-master-read"] = "^(?i)1|true$",
    ["cache-control"] = "^(?i)no-cache$",
    ["pragma"] = "^(?i)(spectre-)?no-cache$",
}

local function get_cacheability_info(req, ctx)
    local service_config = ctx.service_config

    local cacheability_info = {
        is_cacheable = false,
        cache_entry = {
            ttl = nil,
            pattern = nil,
            pattern_v2 = nil, -- for migration only
            bulk_support = false,
            id_identifier = nil,
            dont_cache_missing_ids = false,
            enable_id_extraction = false,
            num_buckets = 0,
            post_body_id = nil,
            vary_body_field_list = nil,
            request_method = DEFAULT_REQUEST_METHOD,
            use_filter = nil,
        },
        cache_name = nil,
        reason = "non-cacheable-uri (" .. ctx.destination .. ")",
        refresh_cache = false,
    }

    if config.get_casper_config("disable_caching") then
        cacheability_info.reason = "caching disabled via configs"
        return cacheability_info
    end

    if not service_config or not service_config.cached_endpoints then
        cacheability_info.reason = "non-configured-namespace (" .. ctx.destination .. ")"
        return cacheability_info
    end

    for cache_name, cache_entry in pairs(service_config.cached_endpoints) do
        local pattern = cache_entry.pattern_v2 or cache_entry.pattern
        if
            (cache_entry.request_method or DEFAULT_REQUEST_METHOD) == ctx.request_method
            and regex_new(pattern):is_match(ctx.normalized_uri)
        then
            cacheability_info.is_cacheable = true
            cacheability_info.cache_name = cache_name
            cacheability_info.cache_entry = cache_entry
            cacheability_info.reason = nil
            cacheability_info.refresh_cache = false

            -- Check if client sent no-cache header
            for name, hdr_pattern in pairs(PLEASE_DO_NOT_CACHE_HEADERS) do
                if req:header_match(name, hdr_pattern) then
                    cacheability_info.is_cacheable = false
                    cacheability_info.reason = "no-cache-header"
                    cacheability_info.refresh_cache = true
                    return cacheability_info
                end
            end

            if ctx.request_method == "POST" then
                local reason
                if not req:header_match("content-type", "^application/json") then
                    -- For POST requests check the content type is `application/json`
                    reason = "non-cacheable-content-type"
                elseif cacheability_info.cache_entry.bulk_support then
                    -- Stop caching if bulk support is added for a POST endpoint.
                    reason = "no-bulk-support-for-post"
                elseif
                    -- For a POST method id extraction and vary fields are obtained from body.
                    (cache_entry.enable_id_extraction or cache_entry.vary_body_field_list ~= nil)
                    and req:body() == ""
                then
                    reason = "non-cacheable-missing-body"
                end
                if reason then
                    cacheability_info.is_cacheable = false
                    cacheability_info.reason = reason
                    cacheability_info.refresh_cache = false
                    return cacheability_info
                end
            end

            return cacheability_info
        end
    end
    return cacheability_info
end

local function calculate_primary_key(req, ctx)
    local primary_key = { ctx.normalized_uri }
    local cache_entry = ctx.cacheability_info.cache_entry

    -- Process body
    if ctx.request_method == "POST" then
        local body = req:body()
        if body ~= "" then
            local body_json = json_decode(body)
            local keys = {}
            if cache_entry.enable_id_extraction then
                ctx.extracted_id = body_json[cache_entry.post_body_id]
                table.insert(keys, cache_entry.post_body_id)
            end
            for _, vary_key in ipairs(cache_entry.vary_body_field_list or {}) do
                table.insert(keys, vary_key)
            end
            table.sort(keys)
            for _, key in ipairs(keys) do
                table.insert(primary_key, key)
                table.insert(primary_key, body_json[key])
            end
        end
    end

    table.insert(primary_key, ctx.destination)
    table.insert(primary_key, ctx.cacheability_info.cache_name)

    -- Extract ID
    if ctx.request_method == "GET" and cache_entry.enable_id_extraction then
        local ids = regex_new(cache_entry.pattern):match(ctx.normalized_uri)[1]
        -- We don't need to add ID to a primary_key as the whole URL is already used
        ctx.extracted_id = regex_new("%2C|,"):splitn(ids, 2)[1]
    end

    -- Process vary headers
    local vary_headers = cache_entry.vary_headers or ctx.service_config.vary_headers or {}
    for _, name in ipairs(vary_headers) do
        table.insert(primary_key, req:header(name) or "")
    end

    return primary_key
end

local function calculate_surrogate_keys(_, ctx)
    local surrogate_keys = { ctx.destination .. "|" .. ctx.cacheability_info.cache_name }
    if ctx.extracted_id ~= nil then
        table.insert(surrogate_keys, surrogate_keys[1] .. "|" .. ctx.extracted_id)
    end
    return surrogate_keys
end

return {
    get_cacheability_info = get_cacheability_info,
    calculate_primary_key = calculate_primary_key,
    calculate_surrogate_keys = calculate_surrogate_keys,
}

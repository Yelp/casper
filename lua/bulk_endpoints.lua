local json = require 'vendor.json'

local datastores = require 'datastores'
local spectre_common = require 'spectre_common'

local cassandra_helper = datastores.cassandra_helper
local JSON_NULL_VALUE = '\0'

json.decodeNumbersAsObjects = true
json.strictTypes = true


-- Kick off and get futures for retrieving bulk endpoint data from the cache
local function kick_off_requests_for_bulk_request(endpoint_ids, separator, request_info, cacheability_info)
    local futures = {}

    for ordinal, endpoint_id in pairs(endpoint_ids) do
        local indiv_request = spectre_common.construct_uri(
            cacheability_info.pattern,
            {endpoint_id},
            request_info.normalized_uri,
            separator,
            1
        )
        local thread_spawn = ngx.thread.spawn(
            spectre_common.fetch_from_cache,
            cassandra_helper,
            endpoint_id,
            indiv_request,
            request_info.destination,
            cacheability_info.cache_name,
            request_info.vary_headers
        )
        futures[ordinal] = {thread_spawn, indiv_request}
    end

    return futures
end

-- Get bulk endpoint from cache
local function get_bulk_data_from_cache(endpoint_ids, separator, request_info, cacheability_info)
    local final_responses, miss_ids, miss_requests, headers = {}, {}, {}, {}
    local miss_exists = false
    local num_ids = 0
    local futures = kick_off_requests_for_bulk_request(endpoint_ids, separator, request_info, cacheability_info)
    local read_failure = false

    -- Place hits into final_responses and record misses
    for ordinal,endpoint_id in pairs(endpoint_ids) do
        local success, cached_value = ngx.thread.wait(futures[ordinal][1])
        local indiv_request = futures[ordinal][2]
        if not success then
            -- cached_value is the error msg in case of failure
            error("Async call to cassandra has failed: " .. json:encode(cached_value))
        end
        if cached_value['body'] ~= nil then
            -- If body is null, we don't populate it into the response
            if cached_value['body'] ~= 'null' then
                for header_name, header_value in pairs(cached_value['headers']) do
                    headers[header_name] = header_value
                end
                final_responses[ordinal] = json:decode(cached_value['body'], nil, { null = JSON_NULL_VALUE })[1]
            end
        else
            miss_exists = true
            miss_requests[ordinal] = indiv_request
            miss_ids[ordinal] = endpoint_id
        end
        num_ids = num_ids + 1
        if cached_value['cassandra_error'] == true then
            read_failure = true
        end
    end

    return {
        miss_exists = miss_exists,
        final_responses = final_responses,
        miss_ids = miss_ids,
        miss_requests = miss_requests,
        num_ids = num_ids,
        headers = headers,
        read_failure = read_failure,
    }
end

-- Store individual responses into cache after the response has been sent back
local function bulk_proxy_post_request_handler(response, request_info, cacheability_info, final_responses, headers)
    for ordinal, miss_request in pairs(response.miss_requests) do
        if final_responses[ordinal] ~= nil or cacheability_info.dont_cache_missing_ids ~= true then
            local success, err = xpcall(
                function()
                    spectre_common.cache_store(
                        cassandra_helper,
                        {response.miss_ids[ordinal]},
                        miss_request,
                        request_info.destination,
                        cacheability_info.cache_name,
                        json:encode({final_responses[ordinal]}, nil, { null = JSON_NULL_VALUE }),
                        headers,
                        request_info.vary_headers,
                        cacheability_info.ttl
                    )
                end, debug.traceback)

            if not success then
                spectre_common.log(ngx.ERR, { err=err, critical=false })
            end
        end
    end
end

local function extract_ids_from_uri(uri, pattern)
    local res, _ = ngx.re.match(uri, pattern)
    local ids, separator = spectre_common.extract_ids_from_string(res[2])
    return ids, separator
end

-- Respond to requests for caching bulk endpoints
local function bulk_endpoint_caching_handler(request_info, cacheability_info)
    local bulk_resp_body, bulk_resp_headers_cacheable, bulk_resp_headers_uncacheable
    local bulk_status = ngx.HTTP_OK
    local all_endpoint_ids, separator = extract_ids_from_uri(
        request_info.normalized_uri,
        cacheability_info.pattern
    )

    local cache_response = get_bulk_data_from_cache(all_endpoint_ids, separator, request_info, cacheability_info)
    local bulk_cache_status = cache_response.miss_exists and 'miss' or 'hit'
    local final_responses = cache_response.final_responses
    local headers = cache_response.headers

    if bulk_cache_status == 'hit' then
        headers[spectre_common.HEADERS.CACHE_STATUS] = 'hit'
        headers = spectre_common.add_zipkin_headers_to_response_headers(request_info, headers)
    else
        -- If there are misses, then construct requests from the misses and forward them
        local bulk_request = spectre_common.construct_uri(
            cacheability_info.pattern,
            cache_response.miss_ids,
            request_info.normalized_uri,
            separator,
            cache_response.num_ids
        )

        local response = spectre_common.get_response_from_remote_service(
            request_info.incoming_zipkin_headers,
            ngx.req.get_method(),
            bulk_request,
            ngx.req.get_headers()
        )
        bulk_status = response.status
        bulk_resp_body = response.body
        bulk_resp_headers_cacheable = response.cacheable_headers
        bulk_resp_headers_uncacheable = response.uncacheable_headers
        for k,v in pairs(bulk_resp_headers_uncacheable) do headers[k] = v end

        -- If there's an error in the request, send back the error body
        if bulk_status ~= 200 then
            headers['Spectre-Cache-Status'] = string.format(
                'non-cacheable-response: status code is %d',
                bulk_status
            )
            return {
                status = bulk_status,
                body = bulk_resp_body,
                headers = headers,
                post_request = nil,
            }
        end

        -- If the application is not application/json, throw an error
        local content_type = bulk_resp_headers_cacheable['Content-Type']
        if not ngx.re.match(content_type, 'application/json') then
            error(string.format(
                'unable to process response; content-type is %s',
                content_type
            ))
        end

        headers['Spectre-Cache-Status'] = 'miss'
        bulk_resp_body = json:decode(bulk_resp_body, nil, { null = JSON_NULL_VALUE })
        local miss_id_responses = {}
        for _, single_resp in ipairs(bulk_resp_body) do
            local request_id = spectre_common.get_response_id(single_resp, cacheability_info.id_identifier)
            miss_id_responses[request_id] = single_resp
        end

        -- Insert non cached results into the final responses in the order it was requested.
        for ordinal,_ in pairs(cache_response.miss_requests) do
            local miss_request_id = cache_response.miss_ids[ordinal]
            final_responses[ordinal] = miss_id_responses[miss_request_id]
        end
    end

    local formatted_final_response = spectre_common.format_into_json(
        final_responses,
        cache_response.num_ids,
        { null = JSON_NULL_VALUE }
    )

    return {
        status = bulk_status,
        body = formatted_final_response,
        headers = headers,
        post_request = function()
            if cache_response['read_failure'] ~= true then
                bulk_proxy_post_request_handler(
                    cache_response,
                    request_info,
                    cacheability_info,
                    final_responses,
                    bulk_resp_headers_cacheable
                )
            end
        end
    }
end

return {
    bulk_endpoint_caching_handler = bulk_endpoint_caching_handler,
}

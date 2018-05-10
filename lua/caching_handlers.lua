local bulk_endpoints = require 'bulk_endpoints'
local datastores = require 'datastores'
local spectre_common = require 'spectre_common'

local cassandra_helper = datastores.cassandra_helper
local caching_handlers = {}


function caching_handlers._extract_ids_from_uri(uri, pattern)
    -- pattern needs to have a single capture group surrounding the ids
    -- i.e. "/business/load?biz_id=([\d,]+)&.*$"
    local res, _ = ngx.re.match(uri, pattern)
    -- lua arrays are 1-based, so res[1] is the first group
    local ids, _ = spectre_common.extract_ids_from_string(res[1])
    return ids
end

-- Function to compute the id field used in the cache.
function caching_handlers._get_cache_key(request_info, cacheability_info)
    local ids = {'null'}
    if cacheability_info.cache_entry.enable_id_extraction then
        if request_info.request_method ~= 'POST' then
            ids = caching_handlers._extract_ids_from_uri(
                request_info.normalized_uri,
                cacheability_info.cache_entry.pattern
            )
        elseif request_info.request_body ~= nil then
            ids = {
                    spectre_common.get_id_from_req_body(
                        cacheability_info.cache_entry.post_id_fields,
                        request_info.request_body
                    )
            }
        else
            error ("POST endpoint with id_extraction enabled has no request body" .. request_info.normalized_uri)
        end
    end
    return ids
end

-- Callback to save response to cache, to be executed after the response has been sent
function caching_handlers._post_request_callback(response, request_info, cacheability_info)
    local ids = caching_handlers._get_cache_key(request_info, cacheability_info)
    local success, err = xpcall(
        function()
            spectre_common.cache_store(
                cassandra_helper,
                ids,
                request_info.normalized_uri,
                request_info.destination,
                cacheability_info.cache_name,
                response.body,
                response.cacheable_headers,
                request_info.vary_headers,
                cacheability_info.cache_entry.ttl,
                cacheability_info.cache_entry.num_buckets
            )
        end, debug.traceback)

    if not success then
        spectre_common.log(ngx.ERR, { err=err, critical=false })
    end
end

-- Respond to requests for caching normal endpoints (non-bulk)
function caching_handlers._caching_handler(request_info, cacheability_info)
    local id = caching_handlers._get_cache_key(request_info, cacheability_info)[1]

    -- Check if datastore already has url cached
    local cached_value = spectre_common.fetch_from_cache(
        cassandra_helper,
        id,
        request_info.normalized_uri,
        request_info.destination,
        cacheability_info.cache_name,
        request_info.vary_headers,
        cacheability_info.cache_entry.num_buckets
    )

    -- Cache hit
    if cached_value['body'] then
        local headers = spectre_common.add_zipkin_headers_to_response_headers(request_info, cached_value['headers'])
        headers[spectre_common.HEADERS.CACHE_STATUS] = 'hit'
        return {
            status = ngx.HTTP_OK,
            body = cached_value['body'],
            headers = headers,
            post_request = nil
        }
    end

    -- Cache miss
    local response = spectre_common.get_response_from_remote_service(
        request_info.incoming_zipkin_headers,
        ngx.req.get_method(),
        ngx.var.request_uri,
        ngx.req.get_headers()
    )

    local headers = response.uncacheable_headers
    local post_request
    if response.status == ngx.HTTP_OK then
        headers[spectre_common.HEADERS.CACHE_STATUS] = 'miss'
        if not cached_value['cassandra_error'] then
            post_request = function()
                caching_handlers._post_request_callback(response, request_info, cacheability_info)
            end
        end
    else
        headers[spectre_common.HEADERS.CACHE_STATUS] = string.format(
            'non-cacheable-response: status code is %d',
            response.status
        )
    end
    for k, v in pairs(response.cacheable_headers) do headers[k] = v end

    return {
        status = response.status,
        body = response.body,
        headers = headers,
        post_request = post_request,
    }
end

-- Forward requests that can't be handled by spectre: either because they don't
-- fit the caching criteria OR because of some failures
-- @cache_status: The value of the header Spectre-Cache-Status
-- @incoming_zipkin_headers: Headers sent in from the request
function caching_handlers._forward_non_handleable_requests(cache_status, incoming_zipkin_headers)
    local response = spectre_common.get_response_from_remote_service(
        incoming_zipkin_headers,
        ngx.req.get_method(),
        ngx.var.request_uri,
        ngx.req.get_headers()
    )
    local headers = response.uncacheable_headers
    headers[spectre_common.HEADERS.CACHE_STATUS] = cache_status
    for k, v in pairs(response.cacheable_headers) do headers[k] = v end

    return {
        status = response.status,
        body = response.body,
        headers = headers,
        cacheable_headers = response.cacheable_headers,
    }
end

function caching_handlers._parse_request(incoming_zipkin_headers)
    -- Normalize the uri
    local normalized_uri = spectre_common.normalize_uri(ngx.var.request_uri)
    local request_info = {}
    local request_headers = ngx.req.get_headers()

    -- Check if endpoint is cacheable, use request URI as cache key
    local destination = spectre_common.get_smartstack_destination(request_headers)
    local cacheability_info = spectre_common.determine_if_cacheable(normalized_uri, destination, request_headers)

    -- Remove the gzip header because it's easier to work with text responses
    if ngx.re.match(ngx.req.get_headers()['accept-encoding'], 'gzip') then
        ngx.req.clear_header("accept-encoding")
        request_headers['accept-encoding'] = nil
    end

    if cacheability_info.is_cacheable or cacheability_info.refresh_cache then
        local vary_headers = spectre_common.get_vary_headers(request_headers, cacheability_info.vary_headers_list)
        request_info =  {
            incoming_zipkin_headers = incoming_zipkin_headers,
            normalized_uri = normalized_uri,
            vary_headers = vary_headers,
            destination = destination,
            request_method = ngx.req.get_method(),
            request_body = ngx.var.request_body
        }
    end
    return cacheability_info, request_info
end

-- Invoked when Spectre received a request and behaves as a caching proxy
function caching_handlers.caching_proxy(incoming_zipkin_headers)
    local cacheability_info, request_info = caching_handlers._parse_request(incoming_zipkin_headers)

    if not cacheability_info.is_cacheable then
        local response = caching_handlers._forward_non_handleable_requests(
            cacheability_info.reason,
            incoming_zipkin_headers
        )
        response.cacheability_info = cacheability_info
        if cacheability_info.refresh_cache then
            caching_handlers._post_request_callback(response, request_info, cacheability_info)
        end
        return response
    end

    local handler_response
    local handler_fn = cacheability_info.cache_entry.bulk_support
                           and bulk_endpoints.bulk_endpoint_caching_handler
                           or caching_handlers._caching_handler

    local success, err = pcall(function()
        handler_response = handler_fn(request_info, cacheability_info)
    end)

    -- When there's an error, just forward the request to the destination service
    if not success then
        spectre_common.log(ngx.ERR, { err=err, critical=true })
        local _, msg = string.match(err, "(.-:%d+): (.+)")
        local response = caching_handlers._forward_non_handleable_requests(msg, incoming_zipkin_headers)
        response.cacheability_info = cacheability_info
        return response
    end

    handler_response.cacheability_info = cacheability_info
    return handler_response
end

return caching_handlers

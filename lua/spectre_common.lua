local json = require "vendor.json"
local ngx_re = require 'ngx.re'
local socket = require 'socket'

local config_loader = require 'config_loader'
local http = require "http"
local metrics_helper = require 'metrics_helper'
local zipkin = require 'zipkin'

json.decodeNumbersAsObjects = true
json.strictTypes = true

local PLEASE_DO_NOT_CACHE_HEADERS = {
    ['x-strongly-consistent-read']={'1', 'true'},
    ['x-force-master-read']={'1', 'true'},
    ['cache-control']={'no-cache'},
    ['pragma']={'no-cache', 'spectre-no-cache'},
}

local HEADERS = {
    CACHE_STATUS = 'Spectre-Cache-Status',
    B3_TRACEID = 'X-B3-TraceId',
    ZIPKIN_ID = 'X-Zipkin-Id',
    ORIGINAL_STATUS = 'X-Original-Status'
}

local SUPPORTED_ENCODING_FOR_ID_EXTRACTION = {
    ['content-type']={'application/json'}
}

local DEFAULT_REQUEST_METHOD = 'GET'

-- JSON encode the message table provided and logs it
local function log(level, err)
    local formatted_err = json:encode(err)
    ngx.log(level, formatted_err)
    return formatted_err
end

-- Check configuration to determine if given URL is cacheable
-- Read list of Vary headers for this namespace / endpoint from srv-configs
local function get_vary_headers_list(namespace, cache_entry)
    if cache_entry['vary_headers'] ~= nil then
        return cache_entry['vary_headers']
    end

    local spectre_config = config_loader.get_spectre_config_for_namespace(namespace)
    if spectre_config['vary_headers'] ~= nil then
        return spectre_config['vary_headers']
    end

    return {}
end

-- Encodes the Vary headers as string
local function get_vary_headers(headers, vary_headers_list)
    local vary_headers = {}
    for _, key in ipairs(vary_headers_list) do
        table.insert(vary_headers, key .. ':' .. tostring(headers[key]))
    end
    return table.concat(vary_headers, ',')
end

-- Helper function to check if available header value
-- satisfies the comparison function for marker header lists.
local function _check_headers_helper(headers, marker_header_list, compare_fn)
    -- Header keys are case insensitive and you can mix - and _ at will
    -- So let's first normalize them to lowercase and with -
    -- We're also normalizing the header values here just to be sure, since
    -- anyway we only care about some very specific headers.
    local normalized_headers = {}
    for k, v in pairs(headers) do
        local norm_key = string.gsub(tostring(k):lower(), "_", "-")
        normalized_headers[norm_key] = tostring(v):lower()
    end
    for header, values in pairs(marker_header_list) do
        for _, v in pairs(values) do
            if compare_fn(normalized_headers[header], v) then
                return true
            end
        end
    end
    return false
end

local function has_marker_headers(headers, marker_header_list)
    return _check_headers_helper(
        headers,
        marker_header_list,
        function(actual, expected) return actual == expected end
    )
end

-- Compares if the string starts with given substring
local function string_starts_with(str, pattern)
    return string.sub(str,1,string.len(pattern)) == pattern
end

-- checks if supported content-type headers are available.
local function has_supported_content_type(headers)
    return _check_headers_helper(
        headers,
        SUPPORTED_ENCODING_FOR_ID_EXTRACTION,
        function(actual, expected) return string_starts_with(actual, expected) end
    )
end

-- Encodes the id fields in request body as single string
local function get_id_from_req_body(id_field, request_body)
    local body = json:decode(request_body)
    if body[id_field] ~= nil then
        return tostring(body[id_field])
    else
        error('Id field not available in request body:' .. id_field)
    end
end

-- @return (boolean indicating if cacheable, TTL in seconds, cache name from config,
--          reason for non-cacheability, vary headers list)
local function determine_if_cacheable(url, namespace, request_headers)
    local cacheability_info = {
        is_cacheable = false,
        cache_entry = {
            ttl = nil,
            pattern = nil,
            bulk_support = false,
            id_identifier = nil,
            dont_cache_missing_ids = false,
            enable_id_extraction = false,
            num_buckets = 0,
            post_body_id = nil,
            vary_body_field_list = nil,
            request_method = DEFAULT_REQUEST_METHOD,
        },
        cache_name = nil,
        reason = 'non-cacheable-uri (' .. namespace .. ')',
        vary_headers_list = nil,
        refresh_cache = false,
    }

    local spectre_config = config_loader.get_spectre_config_for_namespace(
        config_loader.CASPER_INTERNAL_NAMESPACE
    )
    if tostring(spectre_config['disable_caching']) == 'true' then
        cacheability_info.reason = 'caching disabled via configs'
        return cacheability_info
    end

    local namespace_config = config_loader.get_spectre_config_for_namespace(namespace)
    if namespace_config == nil then
        cacheability_info.reason = 'non-configured-namespace (' .. namespace .. ')'
        return cacheability_info
    end

    local http_method = ngx.req.get_method()
    for cache_name, cache_entry in pairs(namespace_config['cached_endpoints']) do
        if ngx.re.match(url, cache_entry['pattern']) and (
            -- Compute if http_method is same as in config or http method is GET
            cache_entry['request_method'] == http_method or DEFAULT_REQUEST_METHOD == http_method
        )
        then
            local vary_headers_list = get_vary_headers_list(namespace, cache_entry)
            cacheability_info = {
                is_cacheable = true,
                cache_entry = cache_entry,
                cache_name = cache_name,
                reason = nil,
                vary_headers_list = vary_headers_list,
                refresh_cache = false,
            }

            -- Check if client sent no-cache header
            if has_marker_headers(request_headers, PLEASE_DO_NOT_CACHE_HEADERS) then
                cacheability_info.is_cacheable = false
                cacheability_info.reason = 'no-cache-header'
                cacheability_info.refresh_cache = true
            end

            if http_method == 'POST' then
                if not has_supported_content_type(request_headers) then
                    -- For Post requests check the content type is application/json
                    cacheability_info.is_cacheable = false
                    cacheability_info.reason = 'non-cacheable-content-type'
                    cacheability_info.refresh_cache = false
                elseif cacheability_info.cache_entry.bulk_support then
                    -- Stop caching if bulk support is added for a POST endpoint.
                    cacheability_info.is_cacheable = false
                    cacheability_info.reason = 'no-bulk-support-for-post'
                    cacheability_info.refresh_cache = false
                else
                    -- Start reading the request body into ngx cache.
                    ngx.req.read_body()
                    -- For a POST method id extraction and vary fields are obtained from body.
                    if ngx.var.request_body == nil and (
                        cache_entry.enable_id_extraction or cache_entry.vary_body_field_list ~= nil
                    ) then
                        cacheability_info.is_cacheable = false
                        cacheability_info.reason = 'non-cacheable-missing-body'
                        cacheability_info.refresh_cache = false
                    end
                end
            end
            break
        end
    end
    return cacheability_info
end

-- This function tells if a header is a hop-by-hop header, as defined in
-- https://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.5.1
-- Returns true if `header_name` is hop-by-hop, and false otherwise
local function is_header_hop_by_hop(header_name)
    local HOP_BY_HOP_HEADERS = {
        ["connection"]          = true,
        ["keep-alive"]          = true,
        ["proxy-authenticate"]  = true,
        ["proxy-authorization"] = true,
        ["te"]                  = true,
        ["trailers"]            = true,
        ["transfer-encoding"]   = true,
        ["upgrade"]             = true,
        ["content-length"]      = true,
    }
    if HOP_BY_HOP_HEADERS[string.lower(header_name)] == true then
        return true
    else
        return false
    end
end

-- Check configuration to determine if a given response header is uncacheable
local function is_header_uncacheable(header_name, namespace)
    local spectre_config  = config_loader.get_spectre_config_for_namespace(namespace)
    if spectre_config == nil then
        return false
    end

    local uncacheable_headers = spectre_config['uncacheable_headers']
    if not uncacheable_headers then
        return false
    end

    for _, uncacheable_header in pairs(uncacheable_headers) do
        if string.lower(header_name) == string.lower(uncacheable_header) then
            return true
        end
    end

    return false
end

-- Gets the source of the request from headers
local function get_smartstack_source(headers)
    return headers['X-Smartstack-Source']
end

-- Gets the destination of the request from headers
local function get_smartstack_destination(headers)
    return headers['X-Smartstack-Destination']
end

local function get_target_uri(request_uri, request_headers)
    local casper_configs = config_loader.get_spectre_config_for_namespace(
        config_loader.CASPER_INTERNAL_NAMESPACE
    )
    local destination = get_smartstack_destination(request_headers)
    if casper_configs['route_through_envoy'] then
        local envoy_configs = config_loader.get_spectre_config_for_namespace(
            config_loader.ENVOY_NAMESPACE
        )
        request_headers['X-Yelp-Svc'] = destination
        -- in envoy_configs['url'], we have a '/' at the end of the url, so we need to remove it from request_url
        return envoy_configs['url'] .. string.sub(request_uri, 2), request_headers
    else
        local info = config_loader.get_smartstack_info_for_namespace(destination)
        local host = info['host']
        local port = info['port']
        if ngx.re.match(host, '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$') == nil then
            -- If host is not an IP, resolve it
            host = socket.dns.toip(host)
        end
        return 'http://' .. host .. ':' .. port .. request_uri, request_headers
    end

end

-- Utility function to perform request to underlying service and write response
-- @return (response status, response body, cacheable_headers, uncacheable_headers)
local function forward_to_destination(method, request_uri, request_headers)
    local target_uri, new_headers = get_target_uri(request_uri, request_headers)
    local destination = get_smartstack_destination(request_headers)

    local response, error_message = http.make_http_request(
        method,
        target_uri,
        new_headers
    )

    if not response then
        local body = "Error requesting " .. request_uri .. ": " .. error_message

        -- From http://w3.impa.br/~diego/software/luasocket/tcp.html#receive
        local status = ngx.HTTP_INTERNAL_SERVER_ERROR
        if error_message == 'closed' then
            -- If the error message is "closed" the connection dropped for some reason
            status = ngx.HTTP_BAD_GATEWAY
        elseif error_message == 'timeout' then
            status = ngx.HTTP_GATEWAY_TIMEOUT
        end

        metrics_helper.emit_counter('spectre.errors', {
            message = error_message,
            status_code = status,
        })

        -- Log any "no_response" errors
        local log_error_message = HEADERS.ORIGINAL_STATUS .. ": -1, message: " .. tostring(error_message)
        log(ngx.ERR, { err=log_error_message, critical=true })

        return {
            status = status,
            body = body,
            cacheable_headers = {},
            uncacheable_headers = {},
            -- This indicates that the origin didn't actually respond
            -- and that any status codes are inferred
            no_response = true
        }
    end

    -- Log any 5xx errors
    if string.match(response.status, '5%d%d') then
        local log_error_message = HEADERS.ORIGINAL_STATUS .. ": " .. response.status ..
              ", message: " .. tostring(error_message)
        log(ngx.ERR, { err=log_error_message, critical=false })
    end

    local cacheable_headers = {}
    local uncacheable_headers = {}
    for header_name, header_value in pairs(response.headers) do
        if not is_header_hop_by_hop(header_name) then
            -- Forward all non hop-by-hop headers to upstream callers
            uncacheable_headers[header_name] = header_value
            if not is_header_uncacheable(header_name, destination) then
                -- Keep track of the cacheable headers to cache them
                cacheable_headers[header_name] = header_value
            end
        end
    end

    return {
        status = response.status,
        body = response.body,
        cacheable_headers = cacheable_headers,
        uncacheable_headers = uncacheable_headers,
        no_response = false
    }
end

-- Validates a header. Returns nil if everything looks good, otherwise
-- returns a string containing an error message.
local function validate_smartstack_header(header_name, header_value)
    if type(header_value) == 'table' then
        local values = ''
        for _, val in pairs(header_value) do
            values = values .. ' ' .. val
        end
        return header_name .. ' has multiple values:' .. values .. ';'
    end
    return nil
end

-- Based on the request headers, determine whether this request is meant for a
-- proxied service (returns true) or for Spectre itself (returns an error message).
-- Returns an error message as a string if a malformed request is detected.
-- --
-- To be proxied through spectre, a request needs 2 headers
-- + X-SmartStack-Source: the nerve namespace of the client sending the
--   request. This header is set and used by HAProxy to proxy requests through
--   Spectre exactly once
-- + X-SmartStack-Destination: the nerve namespace of the service called. This
--   lets Spectre lookup the relevant set of configs and forward to the right
--   service.
-- --
-- Both of these headers are inserted by HAProxy for services configured with
-- the proxied_through directive:
-- (http://paasta.readthedocs.io/en/latest/yelpsoa_configs.html?highlight=proxied_through#basic-http-and-tcp-options)
local function is_request_for_proxied_service(http_method, headers)
    local source_error = validate_smartstack_header('X-Smartstack-Source', get_smartstack_source(headers))
    local destination_error = validate_smartstack_header(
        'X-Smartstack-Destination',
        get_smartstack_destination(headers)
    )

    if source_error and destination_error then
        return false, table.concat({source_error, destination_error}, ' ')
    elseif source_error then
        return false, source_error
    elseif destination_error then
        return false, destination_error
    end

    local source_is_set = get_smartstack_source(headers) ~= nil
    local destination_is_set = get_smartstack_destination(headers) ~= nil

    -- Delete after biz_claims is using the generated clientlib PERF-2453
    if http_method == 'PURGE' then
        return false, nil
    end

    return source_is_set and destination_is_set, nil
end

-- Normalizes the uri by sorting the query params in lexicographical order
local function normalize_uri(uri)
    -- Split the URI by ?
    local res, _ = ngx_re.split(uri, '\\?')

    local uri_path = res[1]
    local query_params = res[2]

    -- If there are no query parameters, we don't change anything about the uri
    if query_params == nil then
        return uri
    end

    -- Split the query params by &
    local query_param_table = ngx_re.split(query_params, '&')
    -- Sort the query params
    table.sort(query_param_table)
    local sorted_params = table.concat(query_param_table, '&')

    return uri_path .. '?' .. sorted_params
end

-- Normalizes the request body by addding only vary fields
-- and sorting the keys in lexicographical order
local function normalize_body(request_body, cache_entry)
    -- Normalize body only for POST methods.
    if cache_entry.request_method ~= 'POST' then
        return nil
    end

    -- If there is no body, nothing to normalize.
    if request_body == nil then
        return nil
    end

    local body = json:decode(request_body)
    -- Add all the required body fields and id field into keys for sorting.
    local keys = { cache_entry.post_body_id }
    if cache_entry.vary_body_field_list ~= nil then
        for _, vary_key in ipairs(cache_entry.vary_body_field_list) do
            table.insert(keys, vary_key)
        end
    end
    -- sort the list that holds the keys
    table.sort(keys)

    -- Create a map with only varying body fields and id field
    -- with sorted order of keys and convert to a json string.
    local vary_body = {}
    for _, key in ipairs(keys) do
        vary_body[key] = body[key]
    end
    return json:encode(vary_body)
end

-- Takes a single response (dictionary) and looks for request id in it
-- ie single_resp = {id=3, reviews='this is a review'}.
-- get_response_id(single_resp, 'id') returns '3'
local function get_response_id(single_resp, id_identifier)
    local res = single_resp[id_identifier]
    if res == nil then
        log(ngx.ERR, {
          err="Invalid id_identifier in config",
          id=id_identifier,
          response=single_resp,
          critical=true
        })
        error("Invalid spectre configuration")
    end
    return ngx.escape_uri(tostring(res))
end

local function remove_nils_from_array(arr, max_ind)
    local new_array = {}
    local arr_ind = 1
    for ind= 1, max_ind do
        local val = arr[ind]
        if val ~= nil then
            new_array[arr_ind] = val
            arr_ind = arr_ind + 1
        end
    end
    return new_array
end

-- Removes nil entries from table and json encodes that final result
local function format_into_json(final_responses, num_ids, options)
    local new_array = remove_nils_from_array(final_responses, num_ids)
    return json:encode(new_array, false, options)
end

-- Takes in a table of indiv_ids and the original request to create a new request
local function construct_uri(pattern, indiv_ids, original_request, separator, num_ids)
    local request_ids = remove_nils_from_array(indiv_ids, num_ids)

    -- Split the original request into 2: left of the ids and right of them
    -- This allows us to piece together the same request but with different ids
    local res, _ = ngx.re.match(original_request, pattern)
    return res[1] .. table.concat(request_ids, separator) .. res[3]
end

-- Extracts the ids from the input string by splitting on the separator characters
-- ids_string must only contain the ids, not the entire URL
local function extract_ids_from_string(ids_string)
    local separator = '%2C'
    -- First try splitting by %2C
    local individual_ids, _ = ngx_re.split(ids_string, separator)
    if table.getn(individual_ids) == 1 then
        -- Try splitting by ,
        separator = ','
        individual_ids, _ = ngx_re.split(ids_string, separator)
    end
    return individual_ids, separator
end

local function fetch_from_cache(cassandra_helper, id, uri, destination, cache_name, vary_headers, num_buckets)
    -- Check if datastore already has url cached
    -- Returns the response body. Fills out the the headers
    local start_time = socket.gettime()
    local cached_value = cassandra_helper.fetch_body_and_headers(
        cassandra_helper.get_connection(cassandra_helper.READ_CONN),
        id,
        uri,
        destination,
        cache_name,
        vary_headers,
        num_buckets
    )

    local cache_status = cached_value['body'] ~= nil and 'hit' or 'miss'
    local dims = {{'namespace', destination}, {'cache_name', cache_name}, {'cache_status', cache_status}}
    metrics_helper.emit_timing('spectre.fetch_body_and_headers', (socket.gettime() - start_time) * 1000, dims)
    metrics_helper.emit_counter('spectre.hit_rate', dims)

    return cached_value
end

local function cache_store(
    cassandra_helper,
    ids,
    uri,
    destination,
    cache_name,
    response_body,
    response_headers,
    vary_headers,
    ttl,
    num_buckets
)
    local start_time = socket.gettime()

    cassandra_helper.store_body_and_headers(
        cassandra_helper.get_connection(cassandra_helper.WRITE_CONN),
        ids,
        uri,
        destination,
        cache_name,
        response_body,
        response_headers,
        vary_headers,
        ttl,
        num_buckets
    )

    local dims = {{'namespace', destination}, {'cache_name', cache_name}}
    metrics_helper.emit_timing('spectre.store_body_and_headers', (socket.gettime() - start_time) * 1000, dims)
end

local function purge_cache(cassandra_helper, namespace, cache_name, id)
    local start_time = socket.gettime()
    local status, body = cassandra_helper.purge(
        cassandra_helper.get_connection(cassandra_helper.WRITE_CONN),
        namespace,
        cache_name,
        id
    )

    local dims = {{'namespace', namespace}, {'cache_name', cache_name}}
    metrics_helper.emit_timing('spectre.purge_cache', (socket.gettime() - start_time) * 1000, dims)
    return status, body
end

-- Get headers to be returned for a normal-endpoint cache hit
local function add_zipkin_headers_to_response_headers(request_info, headers)
    local zipkin_trace_id = request_info.incoming_zipkin_headers[HEADERS.B3_TRACEID]
    if zipkin_trace_id then
        headers[HEADERS.ZIPKIN_ID] = zipkin_trace_id
    end

    return headers
end

-- HTTP headers are not case sensitive, but lua table keys are. So we first unset
-- all version of cased and lower-cased keys from a headers table, before we set
-- it.
local function set_header(headers, key, value)
    headers[key:lower()] = nil
    headers[key] = value
end

-- Injects the request with zipkin headers and calls the remote service
local function get_response_from_remote_service(incoming_zipkin_headers, method, uri, request_headers)
    local zipkin_headers = zipkin.get_new_headers(incoming_zipkin_headers)
    for k, v in pairs(zipkin_headers) do set_header(request_headers, k, v) end

    local response = forward_to_destination(
        method,
        uri,
        request_headers
    )

    return response
end

return {
    get_id_from_req_body = get_id_from_req_body,
    determine_if_cacheable = determine_if_cacheable,
    forward_to_destination = forward_to_destination,
    add_zipkin_headers_to_response_headers = add_zipkin_headers_to_response_headers,
    get_smartstack_destination = get_smartstack_destination,
    get_response_from_remote_service = get_response_from_remote_service,
    get_target_uri = get_target_uri,
    is_header_hop_by_hop = is_header_hop_by_hop,
    is_header_uncacheable = is_header_uncacheable,
    is_request_for_proxied_service = is_request_for_proxied_service,
    get_vary_headers_list = get_vary_headers_list,
    get_vary_headers = get_vary_headers,
    normalize_uri = normalize_uri,
    normalize_body = normalize_body,
    get_response_id = get_response_id,
    format_into_json = format_into_json,
    construct_uri = construct_uri,
    extract_ids_from_string = extract_ids_from_string,
    fetch_from_cache = fetch_from_cache,
    has_marker_headers = has_marker_headers,
    has_supported_content_type = has_supported_content_type,
    cache_store = cache_store,
    log = log,
    HEADERS = HEADERS,
    purge_cache = purge_cache,
    string_starts_with = string_starts_with,
    SUPPORTED_ENCODING_FOR_ID_EXTRACTION = SUPPORTED_ENCODING_FOR_ID_EXTRACTION,
}

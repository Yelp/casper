local json = require 'vendor.json'
local Cluster = require 'resty.cassandra.cluster'
local dc_rr = require 'resty.cassandra.policies.lb.dc_rr'
local simple_retry = require "resty.cassandra.policies.retry.simple"
local cassandra = require 'cassandra'
local C32 = require 'crc32'
local metrics_helper = require 'metrics_helper'
local socket = require 'socket'
local spectre_common = require 'spectre_common'
local config_loader = require 'config_loader'

-- cassandra_helper provides capabilities to use Cassandra as a Spectre datastore
local cassandra_helper = {
    READ_CONN = 'read_connection',
    WRITE_CONN = 'write_connection',
}

json.decodeNumbersAsObjects = true
json.strictTypes = true

-- Logs cassandra errors with the appropriate message and log-level
function cassandra_helper.log_cassandra_error(err, stmt, args)
    local is_timeout_error = string.find(err, 'timeout') ~= nil
    local message, log_level, log_message

    if is_timeout_error then
        message = 'cassandra_timeout'
        log_level = ngx.WARN
    else
        message = 'cassandra_error'
        log_level = ngx.ERR
    end

    metrics_helper.emit_counter('spectre.errors', {
        {'message', message},
        {'status_code', '-1'},
    })

    log_message = 'Cassandra error: ' .. err .. ' Statement: ' .. stmt .. ' Args: ' .. json:encode(args)
    spectre_common.log(log_level, { err=log_message, critical=false })
    return log_level, log_message
end

-- Discovers cassandra cluster hosts for Spectre's Cassandra cluster
-- Ananlogous in function to yelp_cassandra.connection.get_cassandra_cluster_hosts
function cassandra_helper.get_cluster_hosts()
    local configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['cassandra']
    local contents = io.open(configs['seeds_file']):read()
    local hosts = {}

    for _,v in pairs(json:decode(contents)) do
        if string.match(v['name'], configs['local_region']) then
            -- We should only use local nodes to initialize the cluster. The driver picks
            -- a random node from this list and connects to it to get the updated topology.
            -- If it chooses a remote node it'll timeout since the cross-dc latency is very high.
            local host = v['host']
            -- string.match doesn't support the \d{1,3} syntax so the choice was between allowing any
            -- number of digits or writing something like "^%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?$"
            -- which looks awful and is super hard to read...
            if string.match(host, '%d+%.%d+%.%d+%.%d+') == nil then
                -- If host is not an ip, resolve it
                host = socket.dns.toip(host)
            end
            table.insert(hosts, string.format('%s:%s',
                host,
                v['port']
            ))
        end
    end

    return hosts
end

-- See https://github.com/thibaultcha/lua-cassandra/blob/master/lib/resty/cassandra/policies/retry/simple.lua
function cassandra_helper.retry_policy(num_retries)
    if num_retries > 0 then
        return simple_retry.new(num_retries)
    else
        return {
            on_unavailable = function(_) return false end,
            on_read_timeout = function(_) return false end,
            on_write_timeout = function(_) return false end,
        }
    end
end

-- Helper function used for dependency injection
function cassandra_helper.init()
    cassandra_helper.init_with_cluster(Cluster)
end

function cassandra_helper.create_cluster(cluster_module, shm, timeout)
    local configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['cassandra']
    local hosts = cassandra_helper.get_cluster_hosts()
    spectre_common.log(ngx.WARN, { err='init ' .. shm .. ' cluster with timeout ' .. timeout, critical=false })
    -- Use datacenter-aware load balancing policy
    local dc_name = configs['local_dc']
    local cluster, err = cluster_module.new {
        shm = shm,
        contact_points = hosts,
        keyspace = configs['keyspace'],
        lb_policy = dc_rr.new(dc_name),
        lock_timeout = timeout / 1000,  -- lock_timeout is in seconds, not millisecond
        timeout_connect = tonumber(configs['connect_timeout_ms']),
        timeout_read = timeout,
        retry_on_timeout = configs['retry_on_timeout'],
        retry_policy = cassandra_helper.retry_policy(configs['num_retries'])
    }

    if err ~= nil then
       spectre_common.log(ngx.ERR, { err='Cassandra connection error: ' .. err, critical=false })
       return nil
    end

    -- Retrieve the cluster's nodes informations early, to avoid
    -- slowing down our first incoming request, which would have triggered
    -- a refresh should this not be done already.
    cluster:refresh()

    return cluster
end

-- Creates the Cassandra cluster tables to serve as the clients for subsequent queries
function cassandra_helper.init_with_cluster(cluster_module)
    local configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['cassandra']
    ngx.shared.cassandra_read_cluster = cassandra_helper.create_cluster(
        cluster_module,
        'cassandra_read_conn',
        tonumber(configs['read_timeout_ms'])
    )
    ngx.shared.cassandra_write_cluster = cassandra_helper.create_cluster(
        cluster_module,
        'cassandra_write_conn',
        tonumber(configs['write_timeout_ms'])
    )
end

-- Get a shared Cassandra connection, initialized during nginx startup
function cassandra_helper.get_connection(conn_type)
    if conn_type == cassandra_helper.WRITE_CONN then
        return ngx.shared.cassandra_write_cluster
    elseif conn_type == cassandra_helper.READ_CONN then
        return ngx.shared.cassandra_read_cluster
    end
    return nil
end

-- Utility function to execute statements on the Cassandra cluster
function cassandra_helper.execute(cluster, stmt, args, passed_query_options)
    if cluster == nil then
        return nil
    end
    local query_options = {prepared = true}

    if passed_query_options then
        for k, v in pairs(passed_query_options) do query_options[k] = v end
    end

    local res, err = cluster:execute(stmt, args, query_options)

    if err ~= nil then
        cassandra_helper.log_cassandra_error(err, stmt, args)
        return nil, err
    end

    local ret = {}
    for i=1, #res, 1 do
        table.insert(ret, res[i])
    end

    return ret, nil
end

-- Checks to see if Cassandra is available and the required keyspace and table are present
-- used with the /status endpoint
function cassandra_helper.healthcheck(cluster)
    local configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['cassandra']
    local query = 'select * from ' .. configs['keyspace'] .. '.cache_store LIMIT 1'
    local db_check = cassandra_helper.execute(
        cluster,
        query,
        nil,
        {
            consistency = cassandra.consistencies.local_one
        }
    )

    return type(db_check) == 'table'
end

-- Determines the bucket to be used for storing the data associated with the id, cache_name, & namespace combination
function cassandra_helper.get_bucket(key, id, cache_name, namespace, num_buckets)
    local unique_id = key
    if id ~= 'null' and id ~= nil then
        unique_id = id
    end

    local hash = C32.crc32(
        0,
        table.concat({unique_id, cache_name, namespace}, '::')
    )
    local configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['cassandra']
    local max_buckets = num_buckets or configs['default_num_buckets']
    return hash % max_buckets
end

-- Stores the response body and headers into Cassandra
function cassandra_helper.store_body_and_headers(cluster, ids, cache_key, namespace, cache_name,
                                                 body, headers, vary_headers, ttl, num_buckets)

    local configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['cassandra']
    local bucket = cassandra_helper.get_bucket(cache_key, ids[1], cache_name, namespace, num_buckets)
    local consistency = cassandra.consistencies[configs['write_consistency']]
    local stmt = 'INSERT INTO cache_store (bucket, namespace, cache_name, id, key, vary_headers, body, headers)\
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?'
    local args = {
        bucket,
        namespace,
        cache_name,
        ids[1],
        cache_key,
        vary_headers,
        body,
        json:encode(headers),
        ttl
    }
    if consistency == nil then
        cassandra_helper.log_cassandra_error('Invalid consistency level', stmt, args)
        return
    end
    cassandra_helper.execute(cluster, stmt, args, {consistency = consistency})
end

-- Fetch a response body and headers from Cassandra for a cache key.
-- @return hash-like lua table OR nil if the cache key doesn't exist
function cassandra_helper.fetch_body_and_headers(
    cluster,
    id,
    cache_key,
    namespace,
    cache_name,
    vary_headers,
    num_buckets
)
    local result = {
        body = nil,
        headers = nil,
        cassandra_error = true,
    }
    if not cluster then return result end

    local bucket = cassandra_helper.get_bucket(cache_key, id, cache_name, namespace, num_buckets)
    local res, err = cassandra_helper.execute(
        cluster,
        'SELECT body, headers FROM cache_store WHERE bucket = ? AND namespace = ? AND \
            cache_name = ? AND id = ? AND key = ? AND vary_headers = ?',
        {
            bucket,
            namespace,
            cache_name,
            id,
            cache_key,
            vary_headers
        },
        {
            consistency = cassandra.consistencies.local_one
        }
    )

    result['cassandra_error'] = err ~= nil

    if res == nil
       or type(res) == string
       or (type(res) == 'table' and #res == 0)
       or res[1]['body'] == nil
       or res[1]['headers'] == nil then
        return result
    end

    return {
        body = res[1]['body'],
        headers = json:decode(res[1]['headers']),
        cassandra_error = false,
    }
end

-- Called when PURGE endpoint is hit. Remove all endpoints matching a cache_name from Cassandra
-- @return HTTP status (200/500) of the action performed, text identifying all keys purged OR error msg
function cassandra_helper.purge(cluster, namespace, cache_name, id)
    if not cluster then
        metrics_helper.emit_counter('spectre.errors', {
            {'message', 'no_cassandra_connection'},
            {'status_code', ngx.HTTP_INTERNAL_SERVER_ERROR},
        })
        return ngx.HTTP_INTERNAL_SERVER_ERROR, 'Could not establish Cassandra connection'
    end

    local delete_statement = 'DELETE FROM cache_store WHERE bucket = ? AND cache_name = ? AND namespace = ?'
    local statement_args = {nil, cache_name, namespace}

    local configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['cassandra']
    local bucket_min = 0
    local bucket_max = configs['default_num_buckets']-1

    if id then
        delete_statement = delete_statement .. ' AND id = ?'
        table.insert(statement_args,id)
        local bucket = cassandra_helper.get_bucket(nil, id, cache_name, namespace)
        bucket_min = bucket
        bucket_max = bucket
    end

    local failures = false
    for i = bucket_min, bucket_max  do
        statement_args[1] = i
        local res = cassandra_helper.execute(
            cluster,
            delete_statement,
            statement_args,
            {
                consistency = cassandra.consistencies.local_quorum
            }
        )

        if res == nil then
            failures = true
        end
    end

    if failures then
        metrics_helper.emit_counter('spectre.errors', {
            {'message', 'purge_failed'},
            {'status_code', ngx.HTTP_INTERNAL_SERVER_ERROR},
        })
        return ngx.HTTP_INTERNAL_SERVER_ERROR, 'Failed to purge some keys. Check spectre logs'
    end

    local response = string.format(
        'Purged namespace: %s & cache_name: %s',
         namespace,
         cache_name
    )
    if id then
        response = string.format('%s & id: %s', response, id)
    end
    return ngx.HTTP_OK, response
end

return {
    cassandra_helper = cassandra_helper,
}

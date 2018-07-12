require 'busted.runner'()

describe('cassandra_helper', function()
    local datastores
    local cassandra
    local cassandra_helper
    local metrics_helper
    local spectre_common
    local config_loader
    local configs

    setup(function()
        _G.package.loaded.socket = {
            dns = {
                toip = function(_)
                    return '127.1.2.3'
                end
            },
            udp = function(e)
                return {
                    setsockname = function() end,
                    setpeername = function() end,
                    send = function() end,
                }
            end
        }
        datastores = require 'datastores'
        cassandra = require 'cassandra'
        cassandra_helper = datastores.cassandra_helper
        metrics_helper = require 'metrics_helper'

        config_loader = require 'config_loader'
        config_loader.load_services_configs('/code/tests/data/srv-configs')
        configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['cassandra']

        stub(ngx, 'log')
    end)

    describe('get_cluster_hosts', function()
        it('returns the Cassandra connection string', function()
            local actual = cassandra_helper.get_cluster_hosts()
            local expected = {'10.56.6.22:31321','10.40.25.42:31725'}

            assert.are.same(expected, actual)
        end)

        it('only uses local nodes', function()
            local old_region = configs['local_region']
            configs['local_region'] = 'uswest1'
            local actual = cassandra_helper.get_cluster_hosts()
            local expected = {'10.40.25.42:31725' }
            configs['local_region'] = old_region

            assert.are.same(expected, actual)
        end)
    end)

    describe('no_retry_policy', function()
        it('returns false for any retry', function()
            local request = { retries = 0 }
            local policy = cassandra_helper.retry_policy(0)
            assert.falsy(policy:on_unavailable(request))
            assert.falsy(policy:on_read_timeout(request))
            assert.falsy(policy:on_write_timeout(request))
        end)
    end)

    describe('retry_policy', function()
        it('returns true for read/write timeouts and false for unavailability errors', function()
            local request = { retries = 0 }
            local policy = cassandra_helper.retry_policy(2)

            -- The "simple" retry policy only retries on read/write timeouts
            assert.falsy(policy:on_unavailable(request))
            assert.truthy(policy:on_read_timeout(request))
            assert.truthy(policy:on_write_timeout(request))
        end)
    end)

    it('create_cluster correctly initialize the cluster', function()
        local cluster = {
            refresh = function() end
        }
        local cluster_module = {
            new = function(opts)
                assert.are.equal('test_shm', opts['shm'])
                assert.are.same({'10.56.6.22:31321','10.40.25.42:31725'}, opts['contact_points'])
                assert.are.equal(configs['keyspace'], opts['keyspace'])
                assert.are.equal(10 / 1000, opts['lock_timeout'])
                assert.are.equal(configs['connect_timeout_ms'], opts['timeout_connect'])
                assert.are.equal('norcal-devc', opts['lb_policy'].local_dc)
                assert.are.equal(10, opts['timeout_read'])
                return cluster, nil
            end
        }
        local s = spy.on(cluster, 'refresh')
        local old_open = _G.io.open
        _G.io.open = function(name, mode)
            if name == '/nail/etc/superregion' then
                return { read = function(_) return 'norcal-devc' end }
            else
                return old_open(name, mode)
            end
        end
        cassandra_helper.create_cluster(cluster_module, 'test_shm', 10)

        _G.io.open = old_open
        assert.spy(s).was_called()
    end)

    it('init_with_cluster creates both the read and write clusters', function()
        local cluster_module = {}
        stub(cassandra_helper, 'create_cluster')
        cassandra_helper.init_with_cluster(cluster_module)

        assert.stub(cassandra_helper.create_cluster).was_called(2)
        assert.stub(cassandra_helper.create_cluster).was_called_with(
            cluster_module,
            'cassandra_read_conn',
            tonumber(configs['read_timeout_ms'])
        )
        assert.stub(cassandra_helper.create_cluster).was_called_with(
            cluster_module,
            'cassandra_write_conn',
            tonumber(configs['write_timeout_ms'])
        )
    end)

    describe('log_cassandra_error', function()
        it('logs timeouts as warnings', function()
            local log_level, msg
            log_level, msg = cassandra_helper.log_cassandra_error(
                '[Write timeout]',
                'select * from table',
                {})
            assert.are.equal(log_level, ngx.WARN)
        end)

        it('logs non-timeouts as errors', function()
            local log_level, msg
            log_level, msg = cassandra_helper.log_cassandra_error(
                '[Syntax error]',
                'invalid',
                {})
            assert.are.equal(log_level, ngx.ERR)
        end)
    end)

    describe('get_connection', function()
        it('returns the pre-initialized connections', function()
            ngx.shared.cassandra_write_cluster = {}
            ngx.shared.cassandra_read_cluster = {}
            -- assert.are.equal copares the pointers, so we're sure that it's the same object
            assert.are.equal(ngx.shared.cassandra_write_cluster, cassandra_helper.get_connection(cassandra_helper.WRITE_CONN))
            assert.are.equal(ngx.shared.cassandra_read_cluster, cassandra_helper.get_connection(cassandra_helper.READ_CONN))
        end)
    end)

    describe('execute', function()
        it('doesn\'t error if cluster is nil', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return {}, 'Test Error'
                end
            }
            local s = spy.on(conn, 'execute')
            assert.is_nil(cassandra_helper.execute(nil, '', {}, {}))
            assert.spy(s).was_not_called()
        end)

        it('adds prepared=true to the query options', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    assert.are.equal('SELECT * where namespace=?', stmt)
                    assert.are.same({namespace = 'main'}, args)
                    assert.are.same({k1 = 'v1', k2 = 'v2', prepared = true}, options)
                    return {}, nil
                end
            }
            cassandra_helper.execute(conn, 'SELECT * where namespace=?', {namespace = 'main'}, {k1 = 'v1', k2 = 'v2'})
        end)

        it('returns nil in case of error', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return {}, 'Test Error'
                end
            }
            local s = spy.on(conn, 'execute')
            stub(ngx, 'log')
            local spy_metric = spy.on(metrics_helper, 'emit_counter')
            assert.is_nil(cassandra_helper.execute(conn, 'SELECT * where namespace=?', {namespace = 'main'}, {k1 = 'v1', k2 = 'v2'}))
            assert.spy(s).was_called()
            assert.spy(spy_metric).was_called_with('spectre.errors', {{'message', 'cassandra_error'}, {'status_code', '-1'}})
        end)

        it('correctly format results', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return {'res1', 'res2'}, nil
                end
            }
            local s = spy.on(conn, 'execute')
            local expected = {'res1', 'res2'}
            assert.are.same(expected, cassandra_helper.execute(conn, 'SELECT * where namespace=?', {namespace = 'main'}, {k1 = 'v1', k2 = 'v2'}))
            assert.spy(s).was_called()
        end)
    end)

    describe('healthcheck', function()
        it('doesn\'t error if cluster is nil', function()
            local is_healthy = cassandra_helper.healthcheck(nil)
            assert.is_false(is_healthy)
        end)

        it('returns alive if query succeeds', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return {'res1'}, nil
                end
            }
            local is_healthy = cassandra_helper.healthcheck(conn)
            assert.is_true(is_healthy)
        end)

        it('returns unavailable if query fails', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return nil, 'Test Error'
                end
            }
            stub(ngx, 'log')
            local is_healthy = cassandra_helper.healthcheck(conn)
            assert.are.equal(false, is_healthy)
        end)
    end)

    describe('get_bucket', function()
        it('returns a consistent hash', function()
            assert.are.equal(718, cassandra_helper.get_bucket(
                '/biz/yelp-san-francisco',
                nil,
                'test_cache',
                'test_namespace',
                nil
            ))
            assert.are.equal(718, cassandra_helper.get_bucket(
                '/biz/yelp-san-francisco',
                'null',
                'test_cache',
                'test_namespace',
                nil
            ))
            -- let's also make sure it doesn't simply always return 718
            assert.are.equal(847, cassandra_helper.get_bucket(
                '/sf',
                'null',
                'test_cache',
                'test_namespace',
                nil
            ))
            -- check if we can override max_buckets
            assert.are.equal(847, cassandra_helper.get_bucket(
                '/sf',
                'null',
                'test_cache',
                'test_namespace',
               configs['default_num_buckets']
            ))
            -- check if we can override max_buckets
            assert.are.equal(42, cassandra_helper.get_bucket(
                '/sf',
                'null',
                'test_cache',
                'test_namespace',
               configs['default_num_buckets'] + 1
            ))
        end)
    end)

    describe('store_body_and_headers', function()
        it('doesn\'t error if cluster is nil', function()
            cassandra_helper.store_body_and_headers(nil, {'null'},  '', '', '', {}, '', 1)
        end)

        it("works with valid arguments", function()
            local conn = {
                execute = function(self, stmt, args, options)
                    assert.are.same({602, 'server', 'test_cache', 'null', 'key', '{}',
                                     'body', '{"Header1":"foobar"}', 10}, args)
                    assert.are.same({consistency = cassandra.consistencies.all, prepared = true}, options)
                    return {}, nil
                end
            }
            cassandra_helper.store_body_and_headers(conn, {'null'}, 'key', 'server', 'test_cache', 'body', {Header1 = 'foobar'}, '{}', 10)
        end)

        it("fails on invalid consistency level", function()
            local old_consistency = configs['write_consistency']
            configs['write_consistency'] = 'FOOBAR'
            local conn = {
                execute = function(self, stmt, args, options)
                    error('it should not call execute!')
                end
            }
            cassandra_helper.store_body_and_headers(conn, {'null'}, 'key', 'server', 'test_cache', 'body', {Header1 = 'foobar'}, '{}', 10)
            configs['write_consistency'] = old_consistency
        end)

        it('can configure consistency', function()
            local old_consistency = configs['write_consistency']
            configs['write_consistency'] = 'local_quorum'
            local conn = {
                execute = function(self, stmt, args, options)
                    assert.are.same({602, 'server', 'test_cache', 'null', 'key', '{}',
                                     'body', '{"Header1":"foobar"}', 10}, args)
                    assert.are.same({consistency = cassandra.consistencies.local_quorum, prepared = true}, options)
                    return {}, nil
                end
            }
            cassandra_helper.store_body_and_headers(conn, {'null'}, 'key', 'server', 'test_cache', 'body', {Header1 = 'foobar'}, '{}', 10)
            configs['write_consistency'] = old_consistency
        end)

        it('write the correct ids', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    -- NOTE: This bucket should change to 2 once we update get_bucket
                    assert.are.same({469, 'server', 'test_cache', '1', 'key', '{}', 'body', '{"Header1":"foobar"}', 10}, args)
                    return {}, nil
                end
            }
            cassandra_helper.store_body_and_headers(conn, {'1', '3', '2'}, 'key', 'server', 'test_cache', 'body', {Header1 = 'foobar'}, '{}', 10)
        end)


        it('forwards num_buckets', function()
            local s = spy.on(cassandra_helper, 'get_bucket')
            local conn = {
                execute = function(self, stmt, args, options)
                    -- NOTE: This bucket should change to 2 once we update get_bucket
                    assert.are.same({509, 'server', 'test_cache', '1', 'key', '{}', 'body', '{"Header1":"foobar"}', 10}, args)
                    return {}, nil
                end
            }
            cassandra_helper.store_body_and_headers(conn, {'1', '3', '2'}, 'key', 'server', 'test_cache', 'body', {Header1 = 'foobar'}, '{}', 10, 666)
            assert.spy(s).was_called_with('key', '1', 'test_cache', 'server', 666)
            s:revert()
        end)
    end)

    describe('fetch_body_and_headers', function()
        it('doesn\'t error if cluster is nil', function()
            cassandra_helper.fetch_body_and_headers(nil, 'null', '', '', '', '')
        end)

        it('works with valid arguments', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    assert.are.same({602, 'server', 'test_cache', 'null', 'key', ''}, args)
                    assert.are.same({consistency = cassandra.consistencies.local_one, prepared = true}, options)
                    return {{ body = '{"foo":"bar"}', headers = '{"Header1":"foobar"}'}}, nil
                end
            }
            local res = cassandra_helper.fetch_body_and_headers(conn, 'null', 'key', 'server', 'test_cache', '')
            -- we don't decode the body
            assert.are.equal('{"foo":"bar"}', res['body'])
            -- we decode the headers table
            assert.are.same({Header1 = 'foobar'}, res['headers'])
        end)

        it('returns nil if the result is not a table', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return 'Test Error', nil
                end
            }
            local res = cassandra_helper.fetch_body_and_headers(conn, 'null', 'key', 'server', 'test_cache', '')
            assert.is_nil(res['body'])
            assert.is_nil(res['headers'])
            assert.is_false(res['cassandra_error'])
        end)

        it('returns cassandra_error=true on timeout', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return 'Test Error', 'write timeout'
                end
            }
            local res = cassandra_helper.fetch_body_and_headers(conn, 'null', 'key', 'server', 'test_cache', '')
            assert.is_nil(res['body'])
            assert.is_nil(res['headers'])
            assert.is_true(res['cassandra_error'])
        end)

        it('forwards num_buckets', function()
            local s = spy.on(cassandra_helper, 'get_bucket')
            local conn = {
                execute = function(self, stmt, args, options)
                    assert.are.same({196, 'server', 'test_cache', 'null', 'key', ''}, args)
                    assert.are.same({consistency = cassandra.consistencies.local_one, prepared = true}, options)
                    return {{ body = '{"foo":"bar"}', headers = '{"Header1":"foobar"}'}}, nil
                end
            }
            local res = cassandra_helper.fetch_body_and_headers(conn, 'null', 'key', 'server', 'test_cache', '', 666)
            -- we don't decode the body
            assert.are.equal('{"foo":"bar"}', res['body'])
            -- we decode the headers table
            assert.are.same({Header1 = 'foobar'}, res['headers'])
            assert.spy(s).was_called_with('key', 'null', 'test_cache', 'server', 666)
            s:revert()
        end)
    end)

    describe('purge', function()
        it('returns 500 if cluster is nil', function()
            local spy_metric = spy.on(metrics_helper, 'emit_counter')
            local err, msg = cassandra_helper.purge(nil, '', '')
            assert.are.equal(500, err)
            assert.are.equal('Could not establish Cassandra connection', msg)
            assert.spy(spy_metric).was_called_with('spectre.errors', {{'message', 'no_cassandra_connection'}, {'status_code', 500}})
        end)

        it('returns 200 if everything worked', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return {}, nil
                end
            }
            local err, msg = cassandra_helper.purge(conn, 'main', 'test_cache')
            assert.are.equal(200, err)
            assert.are.equal('Purged namespace: main & cache_name: test_cache', msg)
        end)

        it('iterates over all buckets', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return {}, nil
                end
            }
            local s = spy.on(conn, 'execute')
            local err, msg = cassandra_helper.purge(conn, 'main', 'test_cache')
            assert.spy(s).was_called(configs['default_num_buckets'])
        end)

        it('returns 500 if any delete returns an error', function()
            local conn = {
                execute = function(self, stmt, args, options)
                    return {}, 'Test Cassandra Error'
                end
            }
            local spy_metric = spy.on(metrics_helper, 'emit_counter')
            local err, msg = cassandra_helper.purge(conn, 'main', 'test_cache')
            assert.are.equal(500, err)
            assert.are.equal('Failed to purge some keys. Check spectre logs', msg)
            assert.spy(spy_metric).was_called_with('spectre.errors', {{'message', 'purge_failed'}, {'status_code', 500}})
        end)
    end)
end)

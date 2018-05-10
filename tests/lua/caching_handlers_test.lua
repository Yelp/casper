require 'busted.runner'()

insulate('caching_handlers', function()
    local bulk_endpoints
    local caching_handlers
    local old_cache_store
    local old_cacheable
    local old_fetch
    local old_get
    local old_get_id_from_req_body
    local old_ch_extract_ids_from_uri
    local spectre_common

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

        bulk_endpoints = require 'bulk_endpoints'
        caching_handlers = require 'caching_handlers'
        spectre_common = require 'spectre_common'

        stub(ngx, 'log')
    end)

    before_each(function()
        old_cache_store = spectre_common.cache_store
        old_cacheable = spectre_common.determine_if_cacheable
        old_fetch = spectre_common.fetch_from_cache
        old_get = spectre_common.get_response_from_remote_service
        old_get_id_from_req_body = spectre_common.get_id_from_req_body
        old_ch_extract_ids_from_uri = caching_handlers._extract_ids_from_uri
        local headers = {
            ['myheader'] = 'foo',
            ['accept-encoding'] = 'gzip, deflate',
            ['X-Smartstack-Destination'] = 'backend.main',
        }
        ngx.req.get_method = function() return 'GET' end
        ngx.var = {request_uri = '/test/endpoint?ids=1&biz=2&key=3'}
        ngx.req.get_headers = function()
            return headers
        end
        ngx.req.clear_header = function(name) headers[name] = nil end
    end)

    after_each(function()
        spectre_common.cache_store = old_cache_store
        spectre_common.determine_if_cacheable = old_cacheable
        spectre_common.fetch_from_cache = old_fetch
        spectre_common.get_response_from_remote_service = old_get
        spectre_common.get_id_from_req_body = old_get_id_from_req_body
        caching_handlers._extract_ids_from_uri = old_ch_extract_ids_from_uri
    end)

    describe('post_request_callback', function()
        it('stores data in the cache', function()
            stub(spectre_common, 'log')
            stub(spectre_common, 'cache_store')
            caching_handlers._post_request_callback(
                {
                    body = 'test body',
                    cacheable_headers = {Header1 = 'cacheable'},
                    uncacheable_headers = {Header2 = 'uncacheable'},
                },
                {
                    normalized_uri = '/uri',
                    destination = 'backend.main',
                    vary_headers = {Header3 = 'vary'},
                },
                {
                    cache_name = 'test_cache',
                    cache_entry = {
                        ttl = 10,
                        num_buckets = 500,
                    },
                }
            )

            assert.stub(spectre_common.cache_store).was_called()
            assert.stub(spectre_common.cache_store).was_called_with(
                match.is_table(),
                {'null'},
                '/uri',
                'backend.main',
                'test_cache',
                'test body',
                {Header1 = 'cacheable'},
                {Header3 = 'vary'},
                10,
                500
            )
            assert.stub(spectre_common.log).was_not_called()
            -- revert the stubs
            spectre_common.log:revert()
            spectre_common.cache_store:revert()
        end)

        it('logs on error', function()
            stub(spectre_common, 'log')
            spectre_common.cache_store = function(_) error('test error') end
            caching_handlers._post_request_callback(
                {
                    body = 'test body',
                    cacheable_headers = {Header1 = 'cacheable'},
                    uncacheable_headers = {Header2 = 'uncacheable'},
                },
                {
                    normalized_uri = '/uri',
                    destination = 'backend.main',
                    vary_headers = {Header3 = 'vary'},
                },
                {
                    cache_name = 'test_cache',
                    cache_entry = {},
                    ttl = 10,
                }
            )

            assert.stub(spectre_common.log).was_called()
            assert.stub(spectre_common.log).was_called_with(ngx.ERR, match.is_table())
            spectre_common.log:revert()
        end)

        it('correctly gets all ids from url', function()
            stub(spectre_common, 'log')
            stub(spectre_common, 'cache_store')
            caching_handlers._post_request_callback(
                {
                    body = 'test body',
                    cacheable_headers = {Header1 = 'cacheable'},
                    uncacheable_headers = {Header2 = 'uncacheable'},
                },
                {
                    destination = 'backend.main',
                    normalized_uri = '/uri?ids=1%2C2%2C3&foo=bar',
                    vary_headers = {Header3 = 'vary'},
                    request_method = 'GET'
                },
                {
                    cache_name = 'test_cache',
                    cache_entry = {
                        enable_id_extraction = true,
                        pattern = '^/uri\\?ids=((?:\\d|%2C)+)&.*$',
                        ttl = 10,
                        num_buckets = 500,
                    },
                }
            )

            assert.stub(spectre_common.cache_store).was_called()
            assert.stub(spectre_common.cache_store).was_called_with(
                match.is_table(),
                {'1', '2', '3'},
                '/uri?ids=1%2C2%2C3&foo=bar',
                'backend.main',
                'test_cache',
                'test body',
                {Header1 = 'cacheable'},
                {Header3 = 'vary'},
                10,
                500
            )
            assert.stub(spectre_common.log).was_not_called()
            -- revert the stubs
            spectre_common.log:revert()
            spectre_common.cache_store:revert()
        end)

        it('correctly gets all ids from body', function()
            stub(spectre_common, 'log')
            stub(spectre_common, 'cache_store')
            caching_handlers._post_request_callback(
                {
                    body = 'test body',
                    cacheable_headers = {Header1 = 'cacheable'},
                    uncacheable_headers = {Header2 = 'uncacheable'},
                },
                {
                    destination = 'backend.main',
                    normalized_uri = '/uri',
                    request_body = '{"id":123}',
                    request_method = 'POST',
                    vary_headers = {Header3 = 'vary'},
                },
                {
                    cache_name = 'test_cache',
                    cache_entry = {
                        ttl = 10,
                        enable_id_extraction = true,
                        post_body_id = 'id',
                        num_buckets = 500,
                    },
                }
            )

            assert.stub(spectre_common.cache_store).was_called()
            assert.stub(spectre_common.cache_store).was_called_with(
                match.is_table(),
                {'123'},
                '/uri',
                'backend.main',
                'test_cache',
                'test body',
                {Header1 = 'cacheable'},
                {Header3 = 'vary'},
                10,
                500
            )
            assert.stub(spectre_common.log).was_not_called()
            -- revert the stubs
            spectre_common.log:revert()
            spectre_common.cache_store:revert()
        end)

    end)

    describe('get_cache_key', function()
        it('returns default for no extract id config', function()
            local res = caching_handlers._get_cache_key(
                {},
                {cache_entry = {enable_id_extraction = false}}
            )
            assert.are.same({'null'}, res)
        end)

        it('extracts id from uri for get calls', function()
            caching_handlers._extract_ids_from_uri = function(_, _)
                return { 'id1', 'id2' }
            end

            local res = caching_handlers._get_cache_key(
                {
                    request_method = 'GET',
                    normalized_uri = 'testuri/id1'
                },
                {
                    cache_entry = {
                        enable_id_extraction = true,
                        pattern = 'regex_for_extracting'
                    }
                }
            )

            assert.are.same({'id1', 'id2' }, res)
        end)

        it('fails on cached post endpoint with no body', function()
            local status, _ = pcall(
                caching_handlers._get_cache_key,
                {
                    request_method = 'POST',
                    request_body = nil
                },
                {
                    enable_id_extraction = true,
                    post_body_id = 'id_field'
                }
            )
            assert.are.same(false, status)
        end)

        it('extracts id from body for post calls', function()
            spectre_common.get_id_from_req_body = function(_, _)
            return 'id1'
        end

        local res = caching_handlers._get_cache_key(
            {
                request_method = 'POST',
                request_body = 'sample_body'
            },
            {
                cache_entry = {
                    enable_id_extraction = true,
                    post_body_id = 'id_field'
                }
            }
        )

        assert.are.same({'id1'}, res)
    end)

    end)

    describe('caching_handler', function()
        it('returns cached result', function()
            spectre_common.fetch_from_cache = function(_, _, _, _, _, _)
                return {
                    body = 'cached body',
                    headers = {Header1 = 'foobar'},
                    cassandra_error = false,
                }
            end
            local res = caching_handlers._caching_handler({incoming_zipkin_headers = {}},{cache_entry = {}})
            assert.are.equal(ngx.HTTP_OK, res.status)
            assert.are.equal('cached body', res.body)
            assert.are.equal('hit', res.headers[spectre_common.HEADERS.CACHE_STATUS])
            assert.is_nil(res.post_request)
        end)

        it('returns correct data on cache miss and service 200', function()
            spectre_common.fetch_from_cache = function()
                return {
                    body = nil,
                    headers = nil,
                    cassandra_error = false,
                }
            end
            spectre_common.get_response_from_remote_service = function(_, _, _, _)
                return {
                    status = 200,
                    body = 'new body',
                    cacheable_headers = {Header1 = 'cacheable'},
                    uncacheable_headers = {Header2 = 'uncacheable'},
                }
            end
            local res = caching_handlers._caching_handler(
                {incoming_zipkin_headers = {}},
                {cache_name = 'test_cache', cache_entry = {}}
            )

            assert.are.equal(ngx.HTTP_OK, res.status)
            assert.are.equal('new body', res.body)
            assert.are.same({
                ['Header1'] = 'cacheable',
                ['Header2'] = 'uncacheable',
                ['Spectre-Cache-Status'] = 'miss'
            }, res.headers)
            assert.is_not_nil(res.post_request)
        end)

        it('returns the error if the service call fails', function()
            spectre_common.fetch_from_cache = function()
                return {
                    body = nil,
                    headers = nil,
                    cassandra_error = false,
                }
            end
            spectre_common.get_response_from_remote_service = function(_, _, _, _)
                return {
                    status = ngx.HTTP_METHOD_NOT_IMPLEMENTED,
                    body = 'error message',
                    cacheable_headers = {},
                    uncacheable_headers = {},
                }
            end
            local res = caching_handlers._caching_handler(
                {incoming_zipkin_headers = {}},
                {cache_name = 'test_cache', cache_entry = {}}
            )

            assert.are.equal(ngx.HTTP_METHOD_NOT_IMPLEMENTED, res.status)
            assert.are.equal('error message', res.body)
            assert.are.same({
                ['Spectre-Cache-Status'] = 'non-cacheable-response: status code is 501'
            }, res.headers)
            assert.is_nil(res.post_request)
        end)

        it('doesn\'t write to cache if the read timed out', function()
            spectre_common.fetch_from_cache = function()
                return {
                    body = nil,
                    headers = nil,
                    cassandra_error = true,
                }
            end
            spectre_common.get_response_from_remote_service = function(_, _, _, _)
                return {
                    status = 200,
                    body = 'new body',
                    cacheable_headers = {Header1 = 'cacheable'},
                    uncacheable_headers = {Header2 = 'uncacheable'},
                }
            end
            local res = caching_handlers._caching_handler(
                {incoming_zipkin_headers = {}},
                {cache_name = 'test_cache', cache_entry = {}}
            )

            assert.are.equal(ngx.HTTP_OK, res.status)
            assert.are.equal('new body', res.body)
            assert.are.same({
                ['Header1'] = 'cacheable',
                ['Header2'] = 'uncacheable',
                ['Spectre-Cache-Status'] = 'miss'
            }, res.headers)
            assert.is_nil(res.post_request)
        end)
    end)

    describe('forward_non_handleable_requests', function()
        it('correctly forwards the request', function()
            spectre_common.get_response_from_remote_service = function(zipkin_headers, method, uri, headers)
                assert.are.same({['X-Trace-Id'] = '123'}, zipkin_headers)
                assert.are.equal('GET', method)
                assert.are.equal('/test/endpoint?ids=1&biz=2&key=3', uri)
                assert.are.same({
                    ['myheader'] = 'foo',
                    ['accept-encoding'] = 'gzip, deflate',
                    ['X-Smartstack-Destination'] = 'backend.main',
                }, headers)
                return {
                    status = ngx.HTTP_OK,
                    body = 'resp body',
                    cacheable_headers = {Header1 = 'cacheable'},
                    uncacheable_headers = {Header2 = 'uncacheable'},
                }
            end
            local res = caching_handlers._forward_non_handleable_requests(
                'some reason',
                {['X-Trace-Id'] = '123' }
            )

            assert.are.equal(ngx.HTTP_OK, res.status)
            assert.are.equal('resp body', res.body)
            assert.are.same({
                ['Header1'] = 'cacheable',
                ['Header2'] = 'uncacheable',
                ['Spectre-Cache-Status'] = 'some reason',
            }, res.headers)
        end)
    end)

    describe('_parse_request', function()
        it('correctly parses the request', function()
            spectre_common.determine_if_cacheable = function()
                return {
                    is_cacheable = true,
                    cache_entry = {
                        bulk_support = true,
                    },
                    cache_name = 'test_cache',
                    vary_headers_list = {'accept-encoding'},
                    other_fields = true,  -- they're not important here
                    refresh_cache = false,
                }
            end
            local cacheability_info, request_info = caching_handlers._parse_request({['X-Trace-Id'] = '123'})

            assert.are.same({
                is_cacheable = true,
                cache_entry = {
                    bulk_support = true,
                },
                cache_name = 'test_cache',
                vary_headers_list = {'accept-encoding'},
                other_fields = true,  -- they're not important here
                refresh_cache = false,
            }, cacheability_info)
            assert.are.same({
                incoming_zipkin_headers = {['X-Trace-Id'] = '123'},
                normalized_uri = '/test/endpoint?biz=2&ids=1&key=3',
                vary_headers = 'accept-encoding:nil',  -- drops gzip encoding
                destination = 'backend.main',  -- correctly got it from the request headers
                request_method = 'GET',  -- correctly got it from ngx module.
            }, request_info)
        end)

        it('drops accept-encoding gzip header for bulk endpoints', function()
            spectre_common.determine_if_cacheable = function()
                return {
                    is_cacheable = true,
                    cache_name = 'test_cache',
                    vary_headers_list = {'accept-encoding'},
                    cache_entry = {
                        bulk_support = true,
                    },
                    other_fields = true,  -- they're not important here
                }
            end
            local _, request_info = caching_handlers._parse_request({['X-Trace-Id'] = '123'})

            assert.are.equal('accept-encoding:nil', request_info['vary_headers'])
        end)

        it('drops accept-encoding gzip header for normal endpoints', function()
            spectre_common.determine_if_cacheable = function()
                return {
                    is_cacheable = true,
                    cache_name = 'test_cache',
                    vary_headers_list = {'accept-encoding'},
                    cache_entry = {
                        bulk_support = false,
                    },
                    other_fields = true,  -- they're not important here
                }
            end
            local _, request_info = caching_handlers._parse_request({['X-Trace-Id'] = '123'})

            assert.are.equal('accept-encoding:nil', request_info['vary_headers'])
        end)
        it('doesn\'t get vary headers if not cacheable', function()
            -- spectre_common.get_vary_headers will fail if this request is not cacheable
            spectre_common.determine_if_cacheable = function()
                return {
                    is_cacheable = false,
                    reason = 'uncacheable',
                }
            end
            spy.on(spectre_common, 'get_vary_headers')
            local cacheability_info, request_info = caching_handlers._parse_request({['X-Trace-Id'] = '123'})

            assert.are.same({
                is_cacheable = false,
                reason = 'uncacheable',
            }, cacheability_info)
            assert.are.same({}, request_info)
            assert.spy(spectre_common.get_vary_headers).was_not_called()
            spectre_common.get_vary_headers:revert()
        end)
    end)

    describe('caching_proxy', function()
        local old_bulk, old_caching_handler, old_forward, old_parse
        before_each(function()
            old_bulk = bulk_endpoints.bulk_endpoint_caching_handler
            old_caching_handler = caching_handlers._caching_handler
            old_forward = caching_handlers._forward_non_handleable_requests
            old_parse = caching_handlers._parse_request
        end)
        after_each(function()
            bulk_endpoints.bulk_endpoint_caching_handler = old_bulk
            caching_handlers._caching_handler = old_caching_handler
            caching_handlers._forward_non_handleable_requests = old_forward
            caching_handlers._parse_request = old_parse
        end)

        it('forwards request if not cacheable', function()
            caching_handlers._parse_request = function()
                return {
                    is_cacheable = false,
                    cache_entry = {
                        bulk_support = false,
                    },
                    reason = 'uncacheable'
                }, {}
            end
            caching_handlers._forward_non_handleable_requests = function(reason, zipkin_headers)
                assert.are.equal('uncacheable', reason)
                assert.are.same({['X-Trace-Id'] = '123'}, zipkin_headers)
                return {status = 200, headers = {}, body = 'body'}
            end
            stub(caching_handlers, '_caching_handler')
            local res = caching_handlers.caching_proxy({['X-Trace-Id'] = '123'})
            assert.are.same({
                body = 'body',
                headers = {},
                status = 200,
                cacheability_info = {
                    is_cacheable = false,
                    cache_entry = {
                        bulk_support = false,
                    },
                    reason = 'uncacheable',
                },
            }, res)
            assert.stub(caching_handlers._caching_handler).was_not_called()
            caching_handlers._caching_handler:revert() -- reverts the stub
        end)

        it('Save new response if force read', function()
            caching_handlers._parse_request = function()
                return {
                    is_cacheable = false,
                    bulk_support = false,
                    reason = 'uncacheable',
                    refresh_cache = true,
                }, {}
            end
            caching_handlers._forward_non_handleable_requests = function(reason, zipkin_headers)
                assert.are.equal('uncacheable', reason)
                -- zipkin headers should be same as what was passed to caching_proxy function
                -- this just ensures parameters passed to this function were as expected by this test.
                assert.are.same({['X-Force-Master-Read'] = '1'}, zipkin_headers)
                return {status = 200, headers = {}, body = 'body'}
            end
            -- checking function '_post_request_callback' was called ensures that response was stored in the cache_store
            stub(caching_handlers, '_post_request_callback')
            local res = caching_handlers.caching_proxy({['X-Force-Master-Read'] = '1'})
            assert.stub(caching_handlers._post_request_callback).was_called_with(
                res,
                {},
                {
                    is_cacheable = false,
                    bulk_support = false,
                    reason = 'uncacheable',
                    refresh_cache = true,
                }
            )
            caching_handlers._post_request_callback:revert()
        end)

        it('Dont save response if uncacheable but not refresh cache header', function()
            caching_handlers._parse_request = function()
                return {
                    is_cacheable = false,
                    bulk_support = false,
                    reason = 'uncacheable',
                    refresh_cache = false,
                }, {}
            end
            caching_handlers._forward_non_handleable_requests = function(reason, zipkin_headers)
                assert.are.equal('uncacheable', reason)
                assert.are.same({['Cache-Control'] = 'no-cache'}, zipkin_headers)
                return {status = 200, headers = {}, body = 'body'}
            end
            stub(caching_handlers, '_post_request_callback')
            local res = caching_handlers.caching_proxy({['Cache-Control'] = 'no-cache'})
            assert.stub(caching_handlers._post_request_callback).was_not_called()
            caching_handlers._post_request_callback:revert()
        end)

        it('calls basic handler if cacheable', function()
            caching_handlers._parse_request = function()
                return {
                    is_cacheable = true,
                    cache_entry = {
                        bulk_support = false,
                    }
                }, { req_info = true }
            end
            caching_handlers._caching_handler = function(request_info, cacheability_info)
                assert.are.same({req_info = true}, request_info)
                assert.are.same({
                    is_cacheable = true,
                    cache_entry = {
                        bulk_support = false,
                    },
                }, cacheability_info)
                return {status = 200, headers = {}, body = 'body'}
            end
            stub(caching_handlers, '_forward_non_handleable_requests')
            stub(bulk_endpoints, 'bulk_endpoint_caching_handler')
            local res = caching_handlers.caching_proxy({['X-Trace-Id'] = '123'})

            assert.are.same({
                body = 'body',
                headers = {},
                status = 200,
                cacheability_info = {
                    is_cacheable = true,
                    cache_entry = {
                        bulk_support = false,
                    },
                }
            }, res)
            assert.stub(caching_handlers._forward_non_handleable_requests).was_not_called()
            assert.stub(bulk_endpoints.bulk_endpoint_caching_handler).was_not_called()
            caching_handlers._forward_non_handleable_requests:revert()
            bulk_endpoints.bulk_endpoint_caching_handler:revert()
        end)

        it('calls bulk handler if cacheable and bulk_support is true', function()
            caching_handlers._parse_request = function()
                return {
                    is_cacheable = true,
                    cache_entry = {
                        bulk_support = true,
                    },
                }, {req_info = true}
            end
            bulk_endpoints.bulk_endpoint_caching_handler = function(request_info, cacheability_info)
                assert.are.same({req_info = true}, request_info)
                assert.are.same({
                    is_cacheable = true, cache_entry = {
                        bulk_support = true,
                    }, 
                }, cacheability_info)
                return {status = 200, headers = {}, body = 'body'}
            end
            stub(caching_handlers, '_forward_non_handleable_requests')
            stub(caching_handlers, '_caching_handler')
            local res = caching_handlers.caching_proxy({['X-Trace-Id'] = '123'})

            assert.are.same({
                body = 'body',
                headers = {},
                status = 200,
                cacheability_info = {
                    is_cacheable = true,
                    cache_entry = {
                        bulk_support = true,
                    },
                },
            }, res)
            assert.stub(caching_handlers._forward_non_handleable_requests).was_not_called()
            assert.stub(caching_handlers._caching_handler).was_not_called()
            caching_handlers._forward_non_handleable_requests:revert()
            caching_handlers._caching_handler:revert()
        end)

        it('forwards request if caching handler fails', function()
            caching_handlers._parse_request = function()
                return {
                    is_cacheable = true,
                    cache_entry = {
                        bulk_support = true,
                    },
                }, {req_info = true}
            end
            caching_handlers._forward_non_handleable_requests = function(reason, zipkin_headers)
                assert.are.equal('test error', reason)
                assert.are.same({['X-Trace-Id'] = '123'}, zipkin_headers)
                return {status = 200, headers = {}, body = 'body'}
            end
            bulk_endpoints.bulk_endpoint_caching_handler = function(request_info, cacheability_info)
                error("test error")
            end
            spy.on(bulk_endpoints, 'bulk_endpoint_caching_handler')
            spy.on(caching_handlers, '_forward_non_handleable_requests')
            stub(caching_handlers, '_caching_handler')
            local res = caching_handlers.caching_proxy({['X-Trace-Id'] = '123'})

            assert.are.same({
                body = 'body',
                headers = {},
                status = 200,
                cacheability_info = {
                    is_cacheable = true,
                    cache_entry = {
                        bulk_support = true,
                    },
                },
            }, res)
            assert.spy(caching_handlers._forward_non_handleable_requests).was_called()
            assert.spy(bulk_endpoints.bulk_endpoint_caching_handler).was_called()
            assert.stub(caching_handlers._caching_handler).was_not_called()
            bulk_endpoints.bulk_endpoint_caching_handler:revert()
            caching_handlers._forward_non_handleable_requests:revert()
            caching_handlers._caching_handler:revert()
        end)
    end)
end)

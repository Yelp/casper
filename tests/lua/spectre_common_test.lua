require 'busted.runner'()

describe("spectre_common", function()
    local config_loader, spectre_common

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

        config_loader = require 'config_loader'
        config_loader.load_services_configs('/code/tests/data/srv-configs')
        spectre_common = require 'spectre_common'

        stub(ngx, 'log')
    end)

    describe("spectre_common", function()
        it("caches simple URLs", function()
            config_loader.set_spectre_config_for_namespace('srv.main', {
                cached_endpoints = {
                    test_cache = { pattern = "^/cached$", ttl = 1234, vary_headers = {'X-Mode', 'Accept-Encoding'}},
                    test_cache_2 = { pattern = "^/also_cached$", ttl = 1234, vary_headers = {'X-Mode', 'Accept-Encoding'}, dont_cache_missing_ids = true}
                }
            })
            ngx.req.get_method = function() return 'GET' end

            local cacheability_info = spectre_common.determine_if_cacheable('/cached', 'srv.main', {})
            assert.is_true(cacheability_info.is_cacheable)
            assert.are.equal(1234, cacheability_info.ttl)
            assert.are.equal(nil, cacheability_info.cache_status)
            assert.are.same({'X-Mode', 'Accept-Encoding'}, cacheability_info.vary_headers_list)
            assert.is_nil(cacheability_info.dont_cache_missing_ids)

            local cacheability_info = spectre_common.determine_if_cacheable('/also_cached', 'srv.main', {})
            assert.is_true(cacheability_info.dont_cache_missing_ids)

            cacheability_info = spectre_common.determine_if_cacheable('/notcached', 'srv.main', {})
            assert.is_false(cacheability_info.is_cacheable)
            assert.is_nil(cacheability_info.ttl)
            assert.are.equal('non-cacheable-uri (srv.main)', cacheability_info.reason)
            assert.is_nil(cacheability_info.vary_headers_list)
        end)

        it("caches wildcard URLs", function()
            config_loader.set_spectre_config_for_namespace('srv.main', {
                cached_endpoints = {
                    test_cache = { pattern = "^/yelp/.*$", ttl = 1234}
                }
            })
            ngx.req.get_method = function() return 'GET' end

            local cacheability_info = spectre_common.determine_if_cacheable('/yelp/business/info', 'srv.main', {})
            assert.is_true(cacheability_info.is_cacheable)
            assert.are.equal(1234, cacheability_info.ttl)
            assert.are.equal('test_cache', cacheability_info.cache_name)
            assert.is_nil(cacheability_info.reason)
            assert.are.same({}, cacheability_info.vary_headers_list)

            cacheability_info = spectre_common.determine_if_cacheable('/not/yelp/', 'srv.main', {})
            assert.is_false(cacheability_info.is_cacheable)
            assert.is_nil(cacheability_info.ttl)
            assert.are.equal('non-cacheable-uri (srv.main)', cacheability_info.reason)
            assert.is_nil(cacheability_info.vary_headers_list)
        end)

        it("does not cache POST requests with non-json body", function()
            ngx.req.get_method = function() return 'POST' end

            local headers = {['Content-type'] = 'non-json'}
            local cacheability_info = spectre_common.determine_if_cacheable('/yelp/business/info', 'srv.main', headers)
            assert.is_false(cacheability_info.is_cacheable)
            assert.are.equal('non-cacheable-content-type', cacheability_info.reason)
        end)

        it("respects no-cache headers", function()
            config_loader.set_spectre_config_for_namespace('srv.main', {
                cached_endpoints = {
                    test_cache = { pattern = "^/yelp/.*$", ttl = 1234}
                }
            })
            ngx.req.get_method = function() return 'GET' end

            local headers = {}
            local cacheability_info = spectre_common.determine_if_cacheable('/yelp/business/info', 'srv.main', headers)
            assert.is_true(cacheability_info.is_cacheable)

            headers = {Pragma = 'spectre-no-cache'}
            cacheability_info = spectre_common.determine_if_cacheable('/yelp/business/info', 'srv.main', headers)
            assert.is_false(cacheability_info.is_cacheable)

            headers = {['X-Strongly-Consistent-Read'] = '1'}
            cacheability_info = spectre_common.determine_if_cacheable('/yelp/business/info', 'srv.main', headers)
            assert.is_false(cacheability_info.is_cacheable)
        end)

        it("respects cache refresh headers", function()
            config_loader.set_spectre_config_for_namespace('srv.main', {
                cached_endpoints = {
                    test_cache = { pattern = "^/yelp/.*$", ttl = 1234}
                }
            })
            ngx.req.get_method = function() return 'GET' end

            local headers = {}
            local cacheability_info = spectre_common.determine_if_cacheable('/yelp/business/info', 'srv.main', headers)
            assert.is_true(cacheability_info.is_cacheable)

            headers = {['X-Strongly-Consistent-Read'] = '1'}
            cacheability_info = spectre_common.determine_if_cacheable('/yelp/business/info', 'srv.main', headers)
            assert.is_true(cacheability_info.refresh_cache)

            headers = {['X-Force-Master-Read'] = 'True'}
            cacheability_info = spectre_common.determine_if_cacheable('/yelp/business/info', 'srv.main', headers)
            assert.is_true(cacheability_info.refresh_cache)
        end)

        it("caches different orders of query params", function()
            assert.are_equal('/happy/?k1=v6&k2=v1%2Cv20&k3=v2', spectre_common.normalize_uri('/happy/?k2=v1%2Cv20&k1=v6&k3=v2'))
            assert.are_equal(spectre_common.normalize_uri('/happy/?k2=v1%2Cv20&k1=v6&k3=v2'), spectre_common.normalize_uri('/happy/?k3=v2&k1=v6&k2=v1%2Cv20'))
        end)

        it("doesn't error when normalizing requests without params", function()
            assert.are_equal('/happy/v1', spectre_common.normalize_uri('/happy/v1'))
        end)

        it("regex library supports urls with commas in them", function()
            local is_cacheable, ttl, cache_status, vary_list, bulk_endpoint_support

            local function assert_cacheable(url, expected)
                local cacheability_info = spectre_common.determine_if_cacheable(url, 'srv.main', {})
                assert.are.equal(expected, cacheability_info.is_cacheable)
            end

            config_loader.set_spectre_config_for_namespace('srv.main', {
                cached_endpoints = {
                    get_user_info = { pattern = "^/get_user_info/v1(\\?|\\?.*&)ids=[0-9]+(&.*$|$)", ttl = 1234 }
                }
            })

            assert_cacheable('/get_user_info/v1?ids=1', true)
            assert_cacheable('/get_user_info/v1?k1=v1&ids=1', true)
            assert_cacheable('/get_user_info/v1?ids=1&k1=v1', true)
            assert_cacheable('/get_user_info/v1?ids=1&k1=v1,v2,v3', true)
            assert_cacheable('/get_user_info/v1?ids=1,2,3', false)
            assert_cacheable('/get_user_info/v1?user_ids=1,2,3', false)
            assert_cacheable('/get_user_info/v1?ids=1,2,3&k1=v1', false)
            assert_cacheable('/get_user_info/v1?k1=v1&ids=1,2,3', false)
        end)

        it("does not cache anything if config file is missing", function()
            config_loader.set_spectre_config_for_namespace('srv.main', nil)

            local cacheability_info = spectre_common.determine_if_cacheable('/baz', 'srv.main', {})
            assert.is_false(cacheability_info.is_cacheable)
            assert.is_nil(cacheability_info.ttl)
            assert.are.equals('non-configured-namespace (srv.main)', cacheability_info.reason)
            assert.is_nil(cacheability_info.vary_headers_list)
        end)

        it("returns pattern specific vary headers first", function()
            local cache_entry = { pattern = "^/yelp/.*$", ttl = 1234, vary_headers = {'X-Mode', 'Accept-Encoding'} }
            config_loader.set_spectre_config_for_namespace('srv.main', {
                cached_endpoint = { test_cache = cached_entry },
                vary_headers = { 'Accept-Encoding' }
            })

            local vary_list = spectre_common.get_vary_headers_list('srv.main', cache_entry)
            assert.are.same({ 'X-Mode', 'Accept-Encoding' }, vary_list)
        end)

        it("returns namespace vary headers if there are no pattern specific ones", function()
            local cache_entry = { pattern = "^/yelp/.*$", ttl = 1234 }
            config_loader.set_spectre_config_for_namespace('srv.main', {
                cached_endpoints = { test_cache = cache_entry },
                vary_headers = { 'Accept-Encoding' }
            })

            local vary_list = spectre_common.get_vary_headers_list('srv.main', cache_entry)
            assert.are.same({ 'Accept-Encoding' }, vary_list)
        end)

        it("returns {} if there are no vary_headers directives in the configs", function()
            local cache_entry = { pattern = "^/yelp/.*$", ttl = 1234 }
            config_loader.set_spectre_config_for_namespace('srv.main', {
                cached_endpoints = { test_cache = cache_entry },
            })

            local vary_list = spectre_common.get_vary_headers_list('srv.main', cache_entry)
            assert.are.same({}, vary_list)
        end)

        it("correctly concatenates vary headers", function()
            local headers = {
                ['X-Mode'] = "ro",
                ['X-Smartstack-Destination'] = 'yelp-main',
                ['Accept-Encoding'] = 'gzip, deflate'
            }

            local vary_headers = spectre_common.get_vary_headers(headers, {'X-Mode', 'Accept-Encoding'})
            assert.are.same('X-Mode:ro,Accept-Encoding:gzip, deflate', vary_headers)
        end)

        it("correctly handles missing vary headers", function()
            local headers = {
                ['X-Smartstack-Destination'] = 'yelp-main',
                ['Accept-Encoding'] = 'gzip, deflate'
            }

            local vary_headers = spectre_common.get_vary_headers(headers, {'X-Mode', 'Accept-Encoding'})
            assert.are.same('X-Mode:nil,Accept-Encoding:gzip, deflate', vary_headers)
        end)

        it("handles uncacheable headers", function()
            config_loader.set_spectre_config_for_namespace('srv.main', {
                uncacheable_headers = {'X', 'Y'}
            })

            assert.is_true(spectre_common.is_header_uncacheable('X', 'srv.main'))
            assert.is_true(spectre_common.is_header_uncacheable('x', 'srv.main'))
            assert.is_true(spectre_common.is_header_uncacheable('Y', 'srv.main'))
            assert.is_false(spectre_common.is_header_uncacheable('Z', 'srv.main'))
            assert.is_false(spectre_common.is_header_uncacheable('Z', 'other-service.main'))
        end)

        it("Recognizes hop-by-hop headers", function()
            assert.is_true(spectre_common.is_header_hop_by_hop('connection'))
            assert.is_true(spectre_common.is_header_hop_by_hop('Transfer-Encoding'))
            assert.is_true(spectre_common.is_header_hop_by_hop('Content-Length'))
            assert.is_false(spectre_common.is_header_hop_by_hop('X-Custom'))
            assert.is_false(spectre_common.is_header_hop_by_hop('Cache-Control'))
        end)

        it("does not mark header as uncacheable if no uncacheable list in config file", function()
            config_loader.set_spectre_config_for_namespace('srv.main', {
                uncacheable_headers = {}
            })
            assert.is_false(spectre_common.is_header_uncacheable('X', 'srv.main'))
        end)

        it("does not mark header as uncacheable if config file is missing", function()
            config_loader.set_spectre_config_for_namespace('srv.main', {})
            assert.is_false(spectre_common.is_header_uncacheable('X', 'srv.main'))
        end)

        it("does not cache anything if config file is missing", function()
            config_loader.set_spectre_config_for_namespace('srv.main', nil)
            local cacheability_info = spectre_common.determine_if_cacheable('/baz', 'srv.main', {})
            assert.is_false(cacheability_info.is_cacheable)
            assert.is_nil(cacheability_info.ttl)
            assert.is_nil(cacheability_info.cache_name)
            assert.are.equals('non-configured-namespace (srv.main)', cacheability_info.reason)
        end)

        it("formats errors and collapses stack traces into one line", function()
            local _, err = xpcall(function() error('An error occurred') end, debug.traceback)
            assert.is_false(err == nil)
            local formatted_err = spectre_common.log(ngx.ERR, { err=err })
            assert.are.equal(nil, string.find(formatted_err, '\n'))
        end)

        describe("is_request_for_proxied_service", function()
            it("Returns true and no error for proxied requests", function()
                local should_proxy, err = spectre_common.is_request_for_proxied_service('GET', {
                    ['X-Smartstack-Source'] = 'src',
                    ['X-Smartstack-Destination'] = 'dst',
                })
                assert.are.equal(true, should_proxy)
                assert.are.equal(nil, err)
            end)
            it("Returns false and no error for direct requests", function()
                local should_proxy, err = spectre_common.is_request_for_proxied_service('GET', {})
                assert.are.equal(false, should_proxy)
                assert.are.equal(nil, err)
            end)
            it("Returns true for PURGE requests", function()
                local should_proxy, err = spectre_common.is_request_for_proxied_service('PURGE', {})
                assert.are.equal(false, should_proxy)
                assert.are.equal(nil, err)
            end)
            it("Errors out when multiple destination values are provided", function()
                local should_proxy, err = spectre_common.is_request_for_proxied_service('GET', {
                    ['X-Smartstack-Source'] = 'src',
                    ['X-Smartstack-Destination'] = {'dst1', 'dst2'},
                })
                assert.are.equal(false, should_proxy)
                assert.are.equal('X-Smartstack-Destination has multiple values: dst1 dst2;', err)
            end)
            it("Combines error messages when multiple sources AND destinations are provided", function()
                local should_proxy, err = spectre_common.is_request_for_proxied_service('GET', {
                    ['X-Smartstack-Source'] = {'src1', 'src2'},
                    ['X-Smartstack-Destination'] = {'dst1', 'dst2'},
                })
                assert.are.equal(false, should_proxy)
                assert.are.equal('X-Smartstack-Source has multiple values: src1 src2; X-Smartstack-Destination has multiple values: dst1 dst2;', err)
            end)
        end)

        describe("get_response_id", function()
            it("Returns the id as a string given the right parameters", function()
                local status, request_id = pcall(spectre_common.get_response_id, {test_id = 9, key1 = 'val1'}, 'test_id')
                assert.are.equal(true, status)
                assert.are.equal('string', type(request_id))
                assert.are.equal('9',request_id)
            end)
            it("Throws an error when id_identifier isn't in table", function()
                local status, _ = pcall(spectre_common.get_response_id, {test_id = 9, key1 = 'val1'}, 'uid')
                assert.are.equal(false, status)
            end)
            it("Returns nil when id_identifier is nil", function()
                local status, _ = pcall(spectre_common.get_response_id, {test_id = 9, key1 = 'val1'}, nil)
                assert.are.equal(false, status)
            end)
        end)

        describe("format_into_json", function()
            it("Removes nil entries from the final result", function()
                local my_table = {}
                my_table[3] = '1'
                my_table[7] = '4'
                my_table[2] = '2'
                assert.are.equal('["2","1","4"]', spectre_common.format_into_json(my_table, 7))
            end)
        end)

        describe("construct_uri", function()
            local pattern = '(choco/pies\\?my_bulk_ids=)((?:\\d|%2C)+)(&k1=v1&k3=v1)'
            it("correctly constructs a valid request when given valid params", function()
                local ind_ids = {3,4,0}
                local orig_request = 'choco/pies?my_bulk_ids=1%2C3%2C4%2C2%2C0&k1=v1&k3=v1'
                local new_request = spectre_common.construct_uri(pattern, ind_ids, orig_request,'%2C',5)
                assert.are.equal('choco/pies?my_bulk_ids=3%2C4%2C0&k1=v1&k3=v1', new_request)
            end)
            it("works when indiv ids has nil entries and is out of order", function()
                local ind_ids = {[3] = '3', [2] = '2', [5] = '5'}
                local orig_request = 'choco/pies?my_bulk_ids=1%2C3%2C4%2C2%2C5&k1=v1&k3=v1'
                local new_request = spectre_common.construct_uri(pattern, ind_ids, orig_request,'%2C',5)
                assert.are.equal('choco/pies?my_bulk_ids=2%2C3%2C5&k1=v1&k3=v1', new_request)
            end)
            it("works with commas in the path", function()
                pattern = '(choco/)((?:\\d|,)+)(/pies.*)'
                local ind_ids = {1,3,4}
                local orig_request = 'choco/1,3,4/pies'
                local new_request = spectre_common.construct_uri(pattern, ind_ids, orig_request,',',3)
                assert.are.equal(orig_request, new_request)
            end)
        end)

        describe("extract_ids_from_string", function()
            it("works with %2C in the query params", function()
                local ids_string = '1%2C5%2C0'
                local individual_ids, separator = spectre_common.extract_ids_from_string(ids_string)
                assert.are.same({'1','5','0'}, individual_ids)
                assert.are_equal('%2C', separator)
            end)
            it("works with , in the path", function()
                local ids_string = '1,5,0'
                local individual_ids, separator = spectre_common.extract_ids_from_string(ids_string)
                assert.are_equal(',', separator)
                assert.are.same({'1','5','0'}, individual_ids)
            end)
            it("works with a single id", function()
                local ids_string = '1'
                local individual_ids, _ = spectre_common.extract_ids_from_string(ids_string)
                assert.are.same({'1'}, individual_ids)
            end)
        end)
    end)

    describe("get_target_uri", function()
        it("Contructs a full URI based on service host/port", function()
            config_loader.set_smartstack_info_for_namespace('srv.main', {
                host = '169.254.255.254',
                port = 12345,
            })
            local uri = spectre_common.get_target_uri(
                '/quux',
                {['X-Smartstack-Destination'] = 'srv.main'}
            )
            assert.are.equal('http://169.254.255.254:12345/quux', uri)
        end)

        it("Resolves hostnames to ip", function()
            config_loader.set_smartstack_info_for_namespace('srv.main', {
                host = 'localhost',
                port = 12345,
            })
            local uri = spectre_common.get_target_uri(
                '/quux',
                {['X-Smartstack-Destination'] = 'srv.main'}
            )
            assert.are.equal('http://127.1.2.3:12345/quux', uri)
        end)
    end)

    describe("forward_to_destination", function()
        setup(function()
            config_loader.set_smartstack_info_for_namespace('srv.main', {host = 'srvhost', port = 666 })
        end)

        before_each(function()
            _G.package.loaded.spectre_common = nil
            _G.package.loaded.http = nil
        end)

        it("Errors out when HTTP request fails", function()
            _G.package.loaded.http = {
                make_http_request = function(method, uri, body, headers)
                    return nil, 'there was an error. Whoops.'
                end
            }
            local spectre_common = require 'spectre_common'

            local resp = spectre_common.forward_to_destination(
                'GET', -- method
                '/bar', -- request_uri
                {['X-Smartstack-Destination'] = 'srv.main'} -- request_headers
            )

            assert.are.equal(500, resp.status)
            assert.are.equal('Error requesting /bar: there was an error. Whoops.', resp.body)
        end)

        it("Forwards body when HTTP request succeeds", function()
            _G.package.loaded.http = {
                make_http_request = function(method, uri, body, headers)
                    return {
                        status=200,
                        body='RESULT',
                        headers={},
                    }, nil
                end
            }
            local spectre_common = require 'spectre_common'

            local resp = spectre_common.forward_to_destination(
                'GET', -- method
                '/bar', -- request_uri
                {['X-Smartstack-Destination'] = 'srv.main'} -- request_headers
            )

            assert.are.equal(200, resp.status)
            assert.are.equal('RESULT', resp.body)
        end)
    end)
end)

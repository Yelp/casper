require 'busted.runner'()

describe('internal_handlers', function()
    local internal_handlers
    local config_loader
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

        internal_handlers = require 'internal_handlers'
        config_loader = require 'config_loader'
        spectre_common = require 'spectre_common'

        stub(ngx, 'log')
    end)

    describe('purge_handler', function()
        local old_get_uri_args

        before_each(function()
            old_get_uri_args = ngx.req.get_uri_args
        end)
        after_each(function()
            ngx.req.get_uri_args = old_get_uri_args
            config_loader.set_smartstack_info_for_namespace('backend.main', nil)
            config_loader.set_spectre_config_for_namespace('backend.main', nil)
        end)

        it('returns 400 if missing namespace or cache_name', function()
            local status, body

            ngx.req.get_uri_args = function() return {namespace = 'backend.main'} end
            status, body, _ = internal_handlers._purge_handler(nil)
            assert.are.equal(ngx.HTTP_BAD_REQUEST, status)
            assert.are.equal('namespace and cache_name are required arguments', body)

            ngx.req.get_uri_args = function() return {cache_name = 'test_cache'} end
            status, body, _ = internal_handlers._purge_handler(nil)
            assert.are.equal(ngx.HTTP_BAD_REQUEST, status)
            assert.are.equal('namespace and cache_name are required arguments', body)
        end)

        it('returns 400 if namespace is unknown', function()
            local status, body

            ngx.req.get_uri_args = function() return {namespace = 'foo.bar', cache_name = 'test'} end
            status, body, _ = internal_handlers._purge_handler(nil)
            assert.are.equal(ngx.HTTP_BAD_REQUEST, status)
            assert.are.equal('Unknown namespace foo.bar', body)
        end)

        it('returns 400 if cache_name is unknown', function()
            local status, body
            config_loader.set_smartstack_info_for_namespace('backend.main', {host='1.2.3.4', port='1234'})

            ngx.req.get_uri_args = function() return {namespace = 'backend.main', cache_name = 'test'} end
            status, body, _ = internal_handlers._purge_handler(nil)
            assert.are.equal(ngx.HTTP_BAD_REQUEST, status)
            assert.are.equal('Unknown cache_name test for namespace backend.main', body)
        end)

        it('calls datastore if validation succeded', function()
            local status, body
            config_loader.set_smartstack_info_for_namespace('backend.main', {host='1.2.3.4', port='1234'})
            config_loader.set_spectre_config_for_namespace('backend.main', {cached_endpoints= {test_cache = {}}})
            local old_purge = spectre_common.purge_cache
            spectre_common.purge_cache = function(_) return ngx.HTTP_OK, 'OK' end

            ngx.req.get_uri_args = function() return {namespace = 'backend.main', cache_name = 'test_cache'} end
            status, body, _ = internal_handlers._purge_handler(nil)
            assert.are.equal(ngx.HTTP_OK, status)
            assert.are.equal('OK', body)
            spectre_common.purge_cache = old_purge
            end)

    end)
end)

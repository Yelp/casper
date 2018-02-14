require 'busted.runner'()

local lfs = require 'lfs'
local SRV_CONFIG_FOLDER = './tests/data/srv-configs'
local SMARTSTACK_CONFIG = './tests/data/services.yaml'
local SRV1_CONFIG_PATH = './tests/data/srv-configs/service1.main.yaml'

describe("config_loader", function()
    local config_loader
    setup(function()
        _G.os.getenv = function(e)
            if e == 'SRV_CONFIGS_PATH' then
                return SRV_CONFIG_FOLDER
            elseif e == 'SERVICES_YAML_PATH' then
                return SMARTSTACK_CONFIG
            end
            return e
        end

        config_loader = require 'config_loader'

        stub(ngx, 'log')
    end)

    after_each(function()
        config_loader.clear_mod_time_table()
    end)

    it("loads a valid config file", function()
        local config = config_loader.parse_configs(SRV1_CONFIG_PATH)
        assert.are.equal(123, config['cached_endpoints']['test_cache']['ttl'])
        assert.are.equal('^abc$', config['cached_endpoints']['test_cache']['pattern'])
        assert.are.equal('XY1', config['uncacheable_headers'][1])
    end)

    it("returns nil if file_path is nil", function()
        stub(ngx, 'log')
        local config = config_loader.parse_configs(nil)
        assert.is_nil(config)
        assert.stub(ngx.log).was.called.with(ngx.ERR, 'No file provided. Cannot parse configs.')
    end)

    it("returns nil and log if config file is missing", function()
        stub(ngx, 'log')
        local config = config_loader.parse_configs('/code/tests/data/srv-configs/missing_config.yaml')
        assert.is_nil(config)
        assert.stub(ngx.log).was.called.with(ngx.ERR, 'File missing, cannot parse: /code/tests/data/srv-configs/missing_config.yaml')
    end)

    it("file has not changed, returns nil", function()
        local old_att = lfs.attributes
        lfs.attributes = function(a, b) return 12346 end
        config_loader.set_mod_time(SRV1_CONFIG_PATH, 12346)
        local config = config_loader.parse_configs(SRV1_CONFIG_PATH)
        lfs.attributes = old_att

        assert.is_nil(config)
    end)

    it("file has changed, reload it", function()
        local old_att = lfs.attributes
        lfs.attributes = function(a, b) return 12347 end
        config_loader.set_mod_time(SRV1_CONFIG_PATH, 12346)
        local config = config_loader.parse_configs(SRV1_CONFIG_PATH)
        lfs.attributes = old_att

        assert.are.equal(123, config['cached_endpoints']['test_cache']['ttl'])
        assert.are.equal('^abc$', config['cached_endpoints']['test_cache']['pattern'])
        assert.are.equal('XY1', config['uncacheable_headers'][1])
    end)

    it("can load multiple config files", function()
        config_loader.load_services_configs(SRV_CONFIG_FOLDER)

        assert.are.equal('XY1', config_loader.get_spectre_config_for_namespace('service1.main')['uncacheable_headers'][1])
        assert.are.equal('XY2', config_loader.get_spectre_config_for_namespace('service2.main')['uncacheable_headers'][1])
        assert.are.equal('XY2', config_loader.get_spectre_config_for_namespace('service2.canary')['uncacheable_headers'][1])
        assert.are.equal('XY3', config_loader.get_spectre_config_for_namespace('service3.main')['uncacheable_headers'][1])

        --assert.is_true(ngx.shared.services_configs['service2.main']['cached_endpoints'])
        assert._is_true(config_loader.get_spectre_config_for_namespace('service2.main')['cached_endpoints']['test_cache']['dont_cache_missing_ids'])
    end)

    it("can load services.yaml", function()
        config_loader.load_smartstack_info(SMARTSTACK_CONFIG)

        assert.are.equal('169.254.255.254', config_loader.get_smartstack_info_for_namespace('service1.main')['host'])
        assert.are.equal(20001, config_loader.get_smartstack_info_for_namespace('service1.main')['port'])
        assert.are.equal(20003, config_loader.get_smartstack_info_for_namespace('service2.canary')['port'])
    end)

    it("setup reload timer", function()
        local mock_timer = function(a, b) return true, '' end
        ngx.timer.at = mock_timer
        local spy_timer = spy.on(ngx.timer, 'at')

        config_loader.reload_configs(false)
        assert.is_not_nil(config_loader.get_spectre_config_for_namespace('service1.main'))
        assert.is_not_nil(config_loader.get_smartstack_info_for_namespace('service1.main'))
        assert.spy(spy_timer).was.called()
    end)

    it("don't setup reload timer if premature = true", function()
        local mock_timer = function(a, b) return true, '' end
        ngx.timer.at = mock_timer
        local spy_timer = spy.on(ngx.timer, 'at')

        config_loader.reload_configs(true)
        assert.is_not_nil(config_loader.get_spectre_config_for_namespace('service1.main'))
        assert.is_not_nil(config_loader.get_smartstack_info_for_namespace('service1.main'))
        assert.spy(spy_timer).was_not_called()
    end)
end)

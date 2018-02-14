require 'busted.runner'()

insulate("overriding io.open and os.getenv", function()

    describe("metrics_helper", function()
        setup(function()
            _G.os.getenv = function(e)
                return e
            end

            local old_open = _G.io.open
            _G.io.open = function(name, mode)
                if string.match(name, '/nail/etc/') then
                    return {
                        read = function(_)
                            return string.match(name, ".+/(.*)")
                        end
                    }
                else
                    return old_open(name, mode)
                end
            end

            stub(ngx, 'log')
        end)

        before_each(function()
            -- unload metrics_helper before every test to begin clean
            _G.package.loaded.metrics_helper = nil

            -- override socket lib so that we don't actually transmit data
            _G.package.loaded.socket = {
                udp = function(e)
                    return {
                        setsockname = function() end,
                        setpeername = function() end,
                        send = function() end,
                    }
                end
            }
        end)

        it("calls and initializes the UDP socket only once", function()
            local spy_setsockname = spy(function() end)
            local spy_setpeername = spy(function() end)

            _G.package.loaded.socket = {
                udp = function(e)
                    return {
                        setsockname = spy_setsockname,
                        setpeername = spy_setpeername,
                    }
                end
            }
            spy.on(_G.package.loaded.socket, 'udp')

             local metrics_helper = require 'metrics_helper'

            assert.spy(_G.package.loaded.socket.udp).was.called(1)
            assert.spy(spy_setsockname).was.called_with(match._, "*", 0)
            assert.spy(spy_setpeername).was.called_with(match._, "169.254.255.254", "METEORITE_WORKER_PORT")
        end)

        it("emit_request_timing sends data to metrics_relay via UDP", function()
            local spy_send = spy(function() end)

            _G.package.loaded.socket = {
                udp = function(e)
                    return {
                        setsockname = function() end,
                        setpeername = function() end,
                        send = spy_send,
                    }
                end
            }

            local metrics_helper = require 'metrics_helper'
            metrics_helper.emit_request_timing(1, 'some.namespace', 'test_cache', 200)

           assert.are_equal(
               '[["hostname", "MARATHON_HOST"],["habitat", "habitat"],["service_name", "PAASTA_SERVICE"],["instance_name", "PAASTA_INSTANCE"],["namespace", "some.namespace"],["cache_name", "test_cache"],["status", "200"],["metric_name", "spectre.request_timing"]]:1|ms',
               spy_send.calls[1]['vals'][2] -- 2nd argument of the first call
           )
           assert.are_equal(
               '[["hostname", "MARATHON_HOST"],["habitat", "habitat"],["service_name", "PAASTA_SERVICE"],["instance_name", "PAASTA_INSTANCE"],["namespace", "__ALL__"],["cache_name", "test_cache"],["status", "200"],["metric_name", "spectre.request_timing"]]:1|ms',
               spy_send.calls[2]['vals'][2] -- 2nd argument of the second call
           )
           assert.are_equal(
               '[["hostname", "MARATHON_HOST"],["habitat", "habitat"],["service_name", "PAASTA_SERVICE"],["instance_name", "PAASTA_INSTANCE"],["namespace", "some.namespace"],["cache_name", "__ALL__"],["status", "200"],["metric_name", "spectre.request_timing"]]:1|ms',
               spy_send.calls[3]['vals'][2] -- 2nd argument of the third call
           )
           assert.are_equal(
               '[["hostname", "MARATHON_HOST"],["habitat", "habitat"],["service_name", "PAASTA_SERVICE"],["instance_name", "PAASTA_INSTANCE"],["namespace", "__ALL__"],["cache_name", "__ALL__"],["status", "200"],["metric_name", "spectre.request_timing"]]:1|ms',
               spy_send.calls[4]['vals'][2] -- 2nd argument of the fourth call
           )
        end)
    end)
end)

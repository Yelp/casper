require 'busted.runner'()

insulate("override os.getenv", function()
    describe("zipkin", function()
        local zipkin

        setup(function()
            _G.os.getenv = function(e)
                return e
            end

            zipkin = require 'zipkin'

            stub(ngx, 'log')
        end)

        describe("random_string", function()
            it("generates a random string of 16 characters", function()
                local random_string = zipkin.random_string()
                assert.equals(string.len(random_string), 16)
            end)
        end)

        describe("get_new_headers", function()
            it("correctly modifies zipkin headers when span ID exists", function()
                local headers = {
                    ['X-B3-SpanId'] = 'abc'
                }
                local new_headers = zipkin.get_new_headers(headers)

                assert.equals(string.len(new_headers['X-B3-SpanId']), 16)
                assert.equals(new_headers['X-B3-ParentSpanId'], 'abc')
            end)

            it("does no-op when zipkin headers not present", function()
                local headers = {}
                local new_headers = zipkin.get_new_headers(headers)

                assert.is_nil(new_headers['X-B3-SpanId'])
                assert.is_nil(new_headers['X-B3-ParentSpanId'])
            end)
        end)

        describe("extract_zipkin_headers", function()
            it("gets incoming zipkin headers", function()
                -- Checks that extract_zipkin_headers creates a headers table with only
                -- Zipkin-related headers.
                local request_headers = {
                    ['X-B3-TraceId'] = 'abc',
                    ['X-B3-SpanId'] = 'bce',
                    ['X-B3-ParentSpanId'] = 'ced',
                    ['X-B3-Flags'] = '0',
                    ['X-B3-Sampled'] = '1',
                    ['Accept'] = 'text/plain',
                    ['X-B3-Cookie-Monster'] = 'yum'
                }
                local headers = zipkin.extract_zipkin_headers(request_headers)
                -- Lua doesn't have an easy way to get the # of entries in a table if
                -- the table doesn't have consecutive integer keys (i.e. is an array).
                local count = 0
                for _ in pairs(headers) do
                    count = count + 1
                end
                assert.equals(count, #zipkin['ZIPKIN_HEADERS'])
                for index=1, #zipkin['ZIPKIN_HEADERS'] do
                    local header = zipkin.ZIPKIN_HEADERS[index]
                    assert.equals(headers[header], request_headers[header])
                end
            end)

            it("doesn't add non-Zipkin headers", function()
                local request_headers = {
                    ['Accept'] = 'text/plain',
                    ['X-B3-Cookie-Monster'] = 'yum'
                }
                local headers = zipkin.extract_zipkin_headers(request_headers)
                assert.equals(next(headers), nil)
            end)
        end)

    end)
end)

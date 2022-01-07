require 'busted.runner'()

describe("nginx", function()
    it("whitelists all environment variables", function()
        local NGINX_CONFIG_FILE = 'config/nginx.conf'
        local LUA_DIRECTORY = 'lua'

        local env_variables = {}
        local captures

        -- Extract nginx.conf env variables
        for line in io.lines(NGINX_CONFIG_FILE) do
            captures = ngx.re.match(line, 'env (.*);')
            if captures ~= nil then
                env_variables[captures[1]] = false
            end
        end

        -- Search for `getenv` references in lua scripts and check to see
        -- if they've been whitelisted.
        local pfile = io.popen('find ' .. LUA_DIRECTORY .. ' -name "*.lua"')
        for filename in pfile:lines() do
            for line in io.lines(filename) do
                captures = ngx.re.match(line, "getenv\\(['|\"](.*?)['|\"]\\)")
                if captures ~= nil then
                    assert.is_false(
                        env_variables[captures[1]] == nil,
                        string.format(
                            "`%s` in `%s` is not whitelisted in nginx.conf",
                            captures[1], filename))
                        env_variables[captures[1]] = true
                end
            end
        end
        pfile:close()

        -- Ensure that all whitelisted variables are used.
        for key, v in pairs(env_variables) do
            if not string.match(key, "^REDIS_") then
                assert.is_true(v, string.format(
                    "`%s` is whitelisted in nginx.conf but never used", key))
            end
        end
    end)
end)

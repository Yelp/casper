local config_loader = require 'config_loader'
local http = require "resty.http"


local function make_http_request(method, uri, headers)
    local configs = config_loader.get_spectre_config_for_namespace(config_loader.CASPER_INTERNAL_NAMESPACE)['http']
    local httpc = http.new()
    httpc:set_timeout(configs['timeout_ms'])

    -- If body data is already read by some module use it.
    local body = ngx.var.request_body
    if body == nil then
        -- Load client body reader if body is not loaded already.
        local _
        body, _= httpc:get_client_body_reader()
    end

    local response, error_message = httpc:request_uri(uri, {
        method = method,
        body = body,
        headers = headers,
    })

    return response, error_message
end

return {
    make_http_request = make_http_request
}

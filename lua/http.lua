local config_loader = require 'config_loader'
local http = require "resty.http"


local function make_http_request(method, uri, headers)
    local configs = config_loader.get_spectre_config_for_namespace('casper.internal')['http']
    local httpc = http.new()
    httpc:set_timeout(configs['timeout_ms'])

    local client_body_reader, _ = httpc:get_client_body_reader()

    -- If body data is already read by some module use it.
    local body = ngx.var.request_body

    local response, error_message = httpc:request_uri(uri, {
        method = method,
        body = body or client_body_reader,
        headers = headers,
    })

    return response, error_message
end

return {
    make_http_request = make_http_request
}

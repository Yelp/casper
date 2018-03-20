local config_loader = require 'config_loader'
local http = require "resty.http"


local function make_http_request(method, uri, headers)
    local configs = config_loader.get_spectre_config_for_namespace('casper.internal')['http']
    local httpc = http.new()
    httpc:set_timeout(configs['timeout_ms'])

    local client_body_reader, _ = httpc:get_client_body_reader()

    local response, error_message = httpc:request_uri(uri, {
        method = method,
        body = client_body_reader,
        headers = headers,
    })

    return response, error_message
end

return {
    make_http_request = make_http_request
}

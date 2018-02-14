local http = require "resty.http"

-- This timeout should match the one in yelpsoa-configs:spectre/smartstack.yaml
local HTTP_TIMEOUT = os.getenv('HTTP_TIMEOUT_MS') or 60000 -- ms

local function make_http_request(method, uri, headers)
    local httpc = http.new()
    httpc:set_timeout(HTTP_TIMEOUT)

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

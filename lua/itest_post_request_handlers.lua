-- Slow down Spectre for 10s after the response is sent.
local function dogslow_handler()
   os.execute("sleep " .. tostring(10))
end

-- Make Spectre encounter an internal error after the response is sent.
local function crash_handler()
    error('Intentional post request error. Please ignore.')
end

-- Router to take care of itest actions once the response is sent.
local function get_handler(request_uri)
    local handlers = {
       ['/internal_error/dogslow'] = dogslow_handler,
       ['/internal_error/crash'] = crash_handler,
    }

    return handlers[request_uri]
end

return {
    get_handler = get_handler,
}

local _all_hooks = {
    'defaults'
}

-- Import all the custom hook modules and turn them into a [key] => [custom_hook] mapping.
local custom_hooks = {}
for _, key in pairs(_all_hooks) do
    local custom_hook = require('./custom_hooks/' .. key)
    custom_hook.key = key
    custom_hooks[key] = custom_hook
end

return custom_hooks


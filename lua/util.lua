-- Handy function to transform (almost) any lua type in str, for debugging
-- or observability purposes.
-- To use it:
--     local util = require 'util'
--     util.to_string(some_mystery_value)
local function to_string(v)
    if type(v) == "nil" then
        return "nil"
    end
    if type(v) == "boolean" then
        return tostring(v)
    end
    if type(v) == "number" then
        if v > 1500000000 then
            return tostring(v) .. '(' .. os.date('%Y-%m-%d %H:%M:%S', v) .. ')'
        end
        return tostring(v)
    end
    if type(v) == "string" then
        return v
    end
    if type(v) == "table" then
        local str = ''
        for key, val in pairs(v) do
            str = str .. key .. ': ' .. to_string(val) .. ';'
        end
        return '[' .. str .. ']'
    end
    return '<' .. type(v) .. '>'
end

local function is_non_empty_table(table)
    if type(table) ~= "table" then
        return false
    end
    local num_entries = 0
    for _ in pairs(table) do
        num_entries = num_entries + 1
    end
    return num_entries > 0
end

return {
    to_string = to_string,
    is_non_empty_table = is_non_empty_table
}

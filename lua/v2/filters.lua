local FILTERS = {
    dummy = true,
}

for name in pairs(FILTERS) do
    FILTERS[name] = require("lua.v2.filters."..name)
end

return FILTERS

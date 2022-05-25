local FILTERS = {
    dummy = true,
    force_db_read = true,
}

for name in pairs(FILTERS) do
    FILTERS[name] = require("lua.v2.filters."..name)
end

return FILTERS

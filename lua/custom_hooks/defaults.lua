-- Default hooks, these do not modify any values

local function cache_key(uri)
    return uri
end

return {
    cache_key = cache_key,
}


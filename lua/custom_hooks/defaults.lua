-- Default hooks, these do not modify any values

local function custom_cache_key_fn(uri)
    return uri
end

return {
    custom_cache_key_fn = custom_cache_key_fn,
}


local function on_request(_, ctx)
    if string.find(ctx.normalized_uri, "force_db_read=true", 1, true) ~= nil then
        -- Don't load data from cache (always "miss")
        return false
    end
end

return {
    on_request = on_request,
}

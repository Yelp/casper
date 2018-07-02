-- Formats traceback/error information in a table used with `ngx.log`
local function format_critical(traceback, error_message)
    return {
        err=error_message .. "\n\n\t" .. traceback,
        critical=true,
    }
end

return {
    format_critical=format_critical,
}

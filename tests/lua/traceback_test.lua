require 'busted.runner'()

local traceback = require 'traceback'

describe('traceback', function()
    it("Formats traceback and error information into a table", function()
        local tb = "stack traceback:\n\t[C]: in function 'error'\n\t/code/lua/itest_post_request_handlers.lua:8:\n\t...in function </code/lua/entry_point.lua:1>"
        local error_message = 'Something critical happened.'

        local expected_err = [[Something critical happened.

	stack traceback:
	[C]: in function 'error'
	/code/lua/itest_post_request_handlers.lua:8:
	...in function </code/lua/entry_point.lua:1>]]

        formatted_traceback = traceback.format_critical(tb, error_message)
        assert.are.equal(expected_err, formatted_traceback['err'])
        assert.are.equal(true, formatted_traceback['critical'])
    end)
end)

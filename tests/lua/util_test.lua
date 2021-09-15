-- original path: casper/tests/lua

require 'busted.runner'()

describe('util', function()
    local util

    setup(function()
        util = require 'util'
    end)

    it('string function should convert types', function()
        assert.are.same(util.to_string(nil), 'nil')
        assert.are.same(util.to_string(true), 'true')
        assert.are.same(util.to_string(5), '5')
        assert.are.same(util.to_string(1500000001), '1500000001(2017-07-13 19:40:01)')
        assert.are.same(util.to_string('string'), 'string')
        assert.are.same(util.to_string({'a', 'b'}), '[1: a;2: b;]')
        assert.are.same(util.to_string(print), '<function>')
    end)
    it('non empty table function should return the correct value', function()
        assert.are.same(util.is_non_empty_table('a'), false)
        assert.are.same(util.is_non_empty_table({'a', 'b'}), true)
    end)
end)

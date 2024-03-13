use std::ops::Deref;
use std::sync::Arc;

use mini_moka::sync::Cache;
use mlua::{
    Lua, MetaMethod, Result as LuaResult, String as LuaString, Table, UserData, UserDataMethods,
    Value, Variadic,
};
use once_cell::sync::Lazy;
use ouroboros::self_referencing;

/*
--- @class module
--- @tag module
---
--- Built-in module for working with regular expressions.
local module = {}

--- @class Regex
--- Represents a compiled Regex object in Lua.
local Regex = {}
Regex.__index = Regex

export type Regex = typeof(setmetatable({}, Regex))
*/

// TODO: Move to config
const REGEX_CACHE_SIZE: u64 = 512;

#[derive(Clone, Debug)]
pub struct Regex(Arc<regex::bytes::Regex>);

impl Deref for Regex {
    type Target = regex::bytes::Regex;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Global cache for regexes shared across all Lua states.
static CACHE: Lazy<Cache<String, Regex>> = Lazy::new(|| Cache::new(REGEX_CACHE_SIZE));

impl Regex {
    pub fn new(_: &Lua, re: String) -> Result<Self, regex::Error> {
        match CACHE.get(&re) {
            Some(regex) => Ok(regex),
            None => {
                let regex = Self(Arc::new(regex::bytes::Regex::new(&re)?));
                CACHE.insert(re, regex.clone());
                Ok(regex)
            }
        }
    }
}

impl UserData for Regex {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        /*
        --- @within Regex
        --- Returns true if there is a match for the regex anywhere in the given text.
        ---
        --- @param text haystack to search in
        function Regex:is_match(text: string): boolean
            return nil :: any
        end
        */
        methods.add_method("is_match", |_, this, text: LuaString| {
            Ok(this.0.is_match(text.as_bytes()))
        });

        /*
        --- @within Regex
        --- Returns the first match of the regex in the given text.
        --- Returns `nil` if there is no match.
        ---
        --- @param text haystack to search in
        function Regex:match(text: string): Captures?
            return nil :: any
        end
        */
        methods.add_method("match", |lua, this, text: LuaString| {
            let text = text.as_bytes().into();
            let caps = Captures::try_new(text, |text| this.0.captures(text).ok_or(()));
            match caps {
                Ok(caps) => Ok(Value::UserData(lua.create_userdata(caps)?)),
                Err(_) => Ok(Value::Nil),
            }
        });

        // Returns low level information about raw offsets of each submatch.
        methods.add_method("captures_read", |lua, this, text: LuaString| {
            let mut locs = this.capture_locations();
            match this.captures_read(&mut locs, text.as_bytes()) {
                Some(_) => Ok(Value::UserData(
                    lua.create_userdata(CaptureLocations(locs))?,
                )),
                None => Ok(Value::Nil),
            }
        });

        /*
        --- @within Regex
        --- Returns a table substrings of the text given, delimited by a
        --- match of the regex.
        ---
        --- @param text The text to split
        function Regex:split(text: string): {string}
            return nil :: any
        end
        */
        methods.add_method("split", |lua, this, text: LuaString| {
            lua.create_sequence_from(
                this.split(text.as_bytes())
                    .map(|s| lua.create_string(s).unwrap()),
            )
        });

        /*
        --- @within Regex
        --- Returns a table substrings of the text given, delimited by a
        --- match of the regex. The number of substrings is limited by the
        --- `limit` parameter.
        ---
        --- @param text The text to split
        --- @param limit The maximum number of substrings to return
        function Regex:splitn(text: string, limit: number): {string}
            return nil :: any
        end
        */
        methods.add_method("splitn", |lua, this, (text, limit): (LuaString, usize)| {
            lua.create_sequence_from(
                this.splitn(text.as_bytes(), limit)
                    .map(|s| lua.create_string(s).unwrap()),
            )
        });

        methods.add_method(
            "replace",
            |lua, this, (text, rep): (LuaString, LuaString)| {
                lua.create_string(this.replace(text.as_bytes(), rep.as_bytes()))
            },
        );
    }
}

/*
type CapturesMetatable = {
    __index: (Regex, string|number) -> string?,
}

--- @type Captures Captures
--- @within module
--- Represents a set of captures from a regex match.
export type Captures = typeof(setmetatable({}, {} :: CapturesMetatable))
*/
#[self_referencing]
struct Captures {
    text: Box<[u8]>,

    #[borrows(text)]
    #[covariant]
    caps: regex::bytes::Captures<'this>,
}

impl UserData for Captures {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: Value| match key {
            Value::String(s) => {
                let name = s.to_string_lossy();
                this.borrow_caps()
                    .name(&name)
                    .map(|v| lua.create_string(v.as_bytes()))
                    .transpose()
            }
            Value::Integer(i) => this
                .borrow_caps()
                .get(i as usize)
                .map(|v| lua.create_string(v.as_bytes()))
                .transpose(),
            _ => Ok(None),
        })
    }
}

struct CaptureLocations(regex::bytes::CaptureLocations);

impl UserData for CaptureLocations {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        // Returns the total number of capture groups.
        methods.add_method("len", |_, this, ()| Ok(this.0.len()));

        // Returns the start and end positions of the Nth capture group.
        methods.add_method("get", |_, this, i: usize| match this.0.get(i) {
            // We add 1 to the start position because Lua is 1-indexed.
            // End position is non-inclusive, so we don't need to add 1.
            Some((start, end)) => Ok(Variadic::from_iter([start + 1, end])),
            None => Ok(Variadic::new()),
        });
    }
}

struct RegexSet(regex::bytes::RegexSet);

impl Deref for RegexSet {
    type Target = regex::bytes::RegexSet;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl UserData for RegexSet {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_function("new", |_, patterns: Vec<String>| {
            let set = lua_try!(regex::bytes::RegexSet::new(patterns).map(RegexSet));
            Ok(Ok(set))
        });

        methods.add_method("is_match", |_, this, text: LuaString| {
            Ok(this.is_match(text.as_bytes()))
        });

        methods.add_method("len", |_, this, ()| Ok(this.len()));

        methods.add_method("matches", |_, this, text: LuaString| {
            Ok(this
                .matches(text.as_bytes())
                .iter()
                .map(|i| i + 1)
                .collect::<Vec<_>>())
        });
    }
}

/*
--- @within module
--- Compiles a regular expression. Once compiled, it can be used repeatedly
--- to search, split or replace substrings in a text.
--- Returns `nil` and an error message if the input is not a valid regular expression.
---
--- @param re The regular expression to compile
function module.new(re: string): (Regex?, string?)
    return nil :: any
end
*/
fn regex_new(lua: &Lua, re: String) -> LuaResult<Result<Regex, String>> {
    // TODO: Support flag to use global/local/no cache
    Ok(Ok(lua_try!(Regex::new(lua, re))))
}

/*
--- @within module
--- Escapes a string so that it can be used as a literal in a regular expression.
---
--- @param text The string to escape
function module.escape(text: string): string
    return nil :: any
end
*/
fn regex_escape(_: &Lua, text: LuaString) -> LuaResult<String> {
    Ok(regex::escape(text.to_str()?))
}

/*
--- @within module
--- Returns true if there is a match for the regex anywhere in the given text.
---
--- @param re The regular expression to match
--- @param text The text to search in
function module.is_match(re: string, text: string): (boolean?, string?)
    return nil :: any
end
*/
fn regex_is_match(lua: &Lua, (re, text): (String, LuaString)) -> LuaResult<Result<bool, String>> {
    let re = lua_try!(Regex::new(lua, re));
    Ok(Ok(re.is_match(text.as_bytes())))
}

/*
--- @within module
--- Returns all matches of the regex in the given text or nil if there is no match.
---
--- @param re The regular expression to match
--- @param text The text to search in
function module.match(re: string, text: string): ({string}?, string?)
    return nil :: any
end
*/
fn regex_match<'lua>(
    lua: &'lua Lua,
    (re, text): (String, LuaString),
) -> LuaResult<Result<Value<'lua>, String>> {
    let re = lua_try!(Regex::new(lua, re));
    match re.captures(text.as_bytes()) {
        Some(caps) => {
            let mut it = caps
                .iter()
                .map(|om| om.map(|m| lua.create_string(m.as_bytes()).unwrap()));
            let first = it.next().unwrap();
            let table = lua.create_sequence_from(it)?;
            table.raw_set(0, first)?;
            Ok(Ok(Value::Table(table)))
        }
        None => Ok(Ok(Value::Nil)),
    }
}

pub fn create_module(lua: &Lua) -> LuaResult<Table> {
    lua.create_table_from([
        ("new", Value::Function(lua.create_function(regex_new)?)),
        (
            "escape",
            Value::Function(lua.create_function(regex_escape)?),
        ),
        (
            "is_match",
            Value::Function(lua.create_function(regex_is_match)?),
        ),
        ("match", Value::Function(lua.create_function(regex_match)?)),
        ("RegexSet", Value::UserData(lua.create_proxy::<RegexSet>()?)),
    ])
}

/*
return module
*/

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_regex() -> Result<()> {
        let lua = Lua::new();

        let regex = super::create_module(&lua)?;
        lua.load(chunk! {
            local re = $regex.new(".*(?P<gr1>abc)")

            assert(re:is_match("123abc321"), "is_match() should have matches")
            assert(not re:is_match("123"), "is_match() should not have matches")

            local matches = re:match("123abc321")
            assert(matches[0] == "123abc", "zero capture group should match the whole text")
            assert(matches[1] == "abc", "first capture group should match `abc`")
            assert(matches["gr1"] == "abc", "named capture group should match `abc`")
            assert(matches[true] == nil, "bad key should have no match") // Bad key

            // Test split
            local re = $regex.new("[,.]")
            local vec = re:split("abc.qwe,rty.asd")
            assert(#vec == 4, "vec len should be 4")
            assert(vec[1] == "abc" and vec[2] == "qwe" and vec[3] == "rty" and vec[4] == "asd", "vec must be 'abc', 'qwe', 'rty', 'asd'")
            vec = re:splitn("abc,bcd,cde", 2)
            assert(#vec == 2, "vec len should be 2")
            assert(vec[1] == "abc" and vec[2] == "bcd,cde", "vec must be 'abc', 'bcd,cde'")

            // Test invalid regex
            local re, err = $regex.new("(")
            assert(re == nil, "re is not nil")
            assert(string.find(err, "regex parse error") ~= nil, "err must contain 'regex parse error'")

            // Test replace
            local re = $regex.new("(?P<last>[^,\\s]+),\\s+(?P<first>\\S+)")
            local str = re:replace("Smith, John", "$first $last")
            assert(str == "John Smith", "str must be 'John Smith'")
        })
        .exec()
    }

    #[test]
    fn test_regex_shortcuts() -> Result<()> {
        let lua = Lua::new();

        let regex = super::create_module(&lua)?;
        lua.load(chunk! {
            // Test escape
            assert($regex.escape("a*b") == "a\\*b", "escaped regex must be 'a\\*b'")

            // Test "is_match"
            assert($regex.is_match("\\b\\w{13}\\b", "I categorically deny having ..."), "is_match should have matches")
            assert(not $regex.is_match("abc", "bca"), "is_match should not have matches")
            local is_match, err = $regex.is_match("(", "")
            assert(is_match == nil and string.find(err, "regex parse error") ~= nil, "is_match should return error")

            // Test "match"
            local matches = $regex.match("^(\\d{4})-(\\d{2})-(\\d{2})$", "2014-05-01")
            assert(matches[0] == "2014-05-01", "zero capture group should match the whole text")
            assert(matches[1] == "2014", "first capture group should match year")
            assert(matches[2] == "05", "second capture group should match month")
            assert(matches[3] == "01", "third capture group should match day")
            matches, err = $regex.match("(", "")
            assert(matches == nil and string.find(err, "regex parse error") ~= nil, "match should return error")
        })
        .exec()
    }

    #[test]
    fn test_regex_set() -> Result<()> {
        let lua = Lua::new();

        let regex = super::create_module(&lua)?;
        lua.load(chunk! {
            local set = $regex.RegexSet.new({"\\w+", "\\d+", "\\pL+", "foo", "bar", "barfoo", "foobar"})
            assert(set:len() == 7, "len should be 7")
            assert(set:is_match("foobar"), "is_match should have matches")
            assert(table.concat(set:matches("foobar"), ",") == "1,3,4,5,7", "matches should return 1,3,4,5,7")
        })
        .exec()
    }

    #[test]
    fn test_capture_locations() -> Result<()> {
        let lua = Lua::new();

        let regex = super::create_module(&lua)?;
        lua.load(chunk! {
            local re = $regex.new("\\d+(abc)\\d+")

            local str = "123abc321"
            local locs = re:captures_read(str)
            assert(locs ~= nil, "locs is nil")
            assert(locs:len() == 2, "locs len is not 2")
            local i, j = locs:get(0)
            assert(i == 1 and j == 9, "locs:get(0) is not 1, 9")
            i, j = locs:get(1)
            assert(i == 4 and j == 6, "locs:get(1) is not 4, 6")
            assert(str:sub(i, j) == "abc", "str:sub(i, j) is not 'abc'")
            assert(locs:get(2) == nil, "locs:get(2) is nil")

            // Test no match
            locs = re:captures_read("123")
            assert(locs == nil, "locs is not nil")
        })
        .exec()
    }
}

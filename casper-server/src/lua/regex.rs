use std::ops::Deref;
use std::rc::Rc;

use mini_moka::unsync::Cache;
use mlua::{
    Lua, MetaMethod, Result as LuaResult, String as LuaString, Table, UserData, UserDataMethods,
    Value, Variadic,
};
use self_cell::self_cell;

// TODO: Move to config
const REGEX_CACHE_SIZE: u64 = 512;

#[derive(Clone, Debug)]
pub struct Regex(Rc<regex::Regex>);

impl Deref for Regex {
    type Target = regex::Regex;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

type RegexCache = Cache<String, Regex>;

impl Regex {
    pub fn new(lua: &Lua, mut re: String) -> Result<Self, regex::Error> {
        match lua.app_data_mut::<RegexCache>() {
            Some(mut cache) => {
                if let Some(regex) = cache.get(&re) {
                    return Ok(regex.clone());
                }
                let regex = regex::Regex::new(&re).map(|r| Self(Rc::new(r)))?;
                re.shrink_to_fit();
                cache.insert(re, regex.clone());
                Ok(regex)
            }
            None => {
                let mut cache = Cache::new(REGEX_CACHE_SIZE);
                let regex = regex::Regex::new(&re).map(|r| Self(Rc::new(r)))?;
                re.shrink_to_fit();
                cache.insert(re, regex.clone());
                lua.set_app_data::<RegexCache>(cache);
                Ok(regex)
            }
        }
    }
}

type RegexCaptures<'a> = regex::Captures<'a>;

self_cell!(
    struct Captures {
        owner: Box<str>,

        #[covariant]
        dependent: RegexCaptures,
    }
);

struct CaptureLocations(regex::CaptureLocations);

impl UserData for Regex {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("is_match", |_, this, text: String| {
            Ok(this.0.is_match(&text))
        });

        methods.add_method("match", |lua, this, text: Box<str>| {
            let caps = Captures::try_new(text, |text| this.0.captures(text).ok_or(()));
            match caps {
                Ok(caps) => Ok(Value::UserData(lua.create_userdata(caps)?)),
                Err(_) => Ok(Value::Nil),
            }
        });

        // Returns low level information about raw offsets of each submatch.
        methods.add_method("captures_read", |lua, this, text: Box<str>| {
            let mut locs = this.capture_locations();
            match this.captures_read(&mut locs, &text) {
                Some(_) => Ok(Value::UserData(
                    lua.create_userdata(CaptureLocations(locs))?,
                )),
                None => Ok(Value::Nil),
            }
        });

        methods.add_method("split", |_, this, text: String| {
            Ok(this.split(&text).map(String::from).collect::<Vec<_>>())
        });

        methods.add_method("splitn", |_, this, (text, limit): (String, usize)| {
            Ok(this
                .splitn(&text, limit)
                .map(String::from)
                .collect::<Vec<_>>())
        });

        methods.add_method("replace", |lua, this, (text, rep): (String, String)| {
            lua.create_string(this.replace(&text, &rep).as_bytes())
        });
    }
}

impl UserData for Captures {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |_, this, key: Value| match key {
            Value::String(s) => {
                let name = std::str::from_utf8(s.as_bytes())?;
                let caps = this.borrow_dependent();
                let res = caps.name(name).map(|v| v.as_str().to_string());
                Ok(res)
            }
            Value::Integer(i) => Ok(this
                .borrow_dependent()
                .get(i as usize)
                .map(|v| v.as_str().to_string())),
            _ => Ok(None),
        })
    }
}

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

struct RegexSet(regex::RegexSet);

impl Deref for RegexSet {
    type Target = regex::RegexSet;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl UserData for RegexSet {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_function("new", |_, patterns: Vec<String>| {
            Ok(Ok(lua_try!(regex::RegexSet::new(patterns).map(RegexSet))))
        });

        methods.add_method("is_match", |_, this, text: String| Ok(this.is_match(&text)));

        methods.add_method("len", |_, this, ()| Ok(this.len()));

        methods.add_method("matches", |_, this, text: String| {
            Ok(this
                .matches(&text)
                .iter()
                .map(|i| i + 1)
                .collect::<Vec<_>>())
        });
    }
}

fn regex_new(lua: &Lua, re: String) -> LuaResult<Result<Regex, String>> {
    Ok(Ok(lua_try!(Regex::new(lua, re))))
}

fn regex_escape(_: &Lua, text: LuaString) -> LuaResult<String> {
    Ok(regex::escape(text.to_str()?))
}

// A shortcut for testing a match for the regex in the string given.
fn regex_is_match(lua: &Lua, (re, text): (String, String)) -> LuaResult<Result<bool, String>> {
    let re = lua_try!(Regex::new(lua, re));
    Ok(Ok(re.is_match(&text)))
}

// A shortcut for matching text against a regex and collecting all capture groups
fn regex_match<'lua>(
    lua: &'lua Lua,
    (re, text): (String, LuaString),
) -> LuaResult<Result<Value<'lua>, String>> {
    let re = lua_try!(Regex::new(lua, re));
    let caps = re.captures(lua_try!(text.to_str()));
    match caps {
        Some(caps) => match caps
            .iter()
            .map(|om| om.map(|m| m.as_str()))
            .collect::<Option<Vec<_>>>()
        {
            Some(matches) => {
                let table = lua.create_sequence_from(matches.iter().skip(1).copied())?;
                table.raw_set(0, matches[0])?;
                Ok(Ok(Value::Table(table)))
            }
            None => Ok(Ok(Value::Nil)),
        },
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

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_regex() -> Result<()> {
        let lua = Lua::new();

        let regex = super::create_module(&lua)?;
        lua.load(chunk! {
            local re = $regex.new(".*(?P<gr1>abc)")

            assert(re:is_match("123abc321"))
            assert(not re:is_match("123"))

            local matches = re:match("123abc321")
            assert(matches[0] == "123abc")
            assert(matches[1] == "abc")
            assert(matches["gr1"] == "abc")
            assert(matches[true] == nil) // Bad key

            // Test split
            local re = $regex.new("[,.]")
            local vec = re:split("abc.qwe,rty.asd")
            assert(#vec == 4)
            vec = re:splitn("abc,bcd,cde", 2)
            assert(#vec == 2 and vec[1] == "abc" and vec[2] == "bcd,cde")

            // Test invalid regex
            local re, err = $regex.new("(")
            assert(re == nil)
            assert(string.find(err, "regex parse error") ~= nil)

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
            assert(not $regex.is_match("abc", "bca"), "is_match should have not matches")
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
            assert(set:len() == 7)
            assert(set:is_match("foobar"))
            assert(table.concat(set:matches("foobar"), ",") == "1,3,4,5,7")
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

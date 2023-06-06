use std::ops::Deref;

use mini_moka::unsync::Cache;
use mlua::{Lua, MetaMethod, Result as LuaResult, Table, UserData, UserDataMethods, Value};
use ouroboros::self_referencing;

const REGEX_CACHE_SIZE: u64 = 512;

#[derive(Clone, Debug)]
pub struct Regex(regex::Regex);

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Regex {
    pub fn new(lua: &Lua, re: String) -> Result<Self, regex::Error> {
        match lua.app_data_mut::<Cache<String, Regex>>() {
            Some(mut cache) => {
                if let Some(regex) = cache.get(&re) {
                    return Ok(regex.clone());
                }
                let regex = regex::Regex::new(&re).map(Self)?;
                cache.insert(re, regex.clone());
                Ok(regex)
            }
            None => {
                let mut cache = Cache::new(REGEX_CACHE_SIZE);
                let regex = regex::Regex::new(&re).map(Self)?;
                cache.insert(re, regex.clone());
                lua.set_app_data::<Cache<String, Regex>>(cache);
                Ok(regex)
            }
        }
    }
}

#[self_referencing]
struct RegexCaptures {
    text: Box<str>,
    #[borrows(text)]
    #[covariant]
    caps: regex::Captures<'this>,
}

impl UserData for Regex {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("is_match", |_, this, text: String| {
            Ok(this.0.is_match(&text))
        });

        methods.add_method("match", |lua, this, text: Box<str>| {
            let caps = RegexCapturesTryBuilder {
                text,
                caps_builder: |text| this.0.captures(text).ok_or(()),
            }
            .try_build();
            match caps {
                Ok(caps) => Ok(Value::UserData(lua.create_userdata(caps)?)),
                Err(_) => Ok(Value::Nil),
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
    }
}

impl UserData for RegexCaptures {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |_, this, key: Value| match key {
            Value::String(s) => {
                let name = std::str::from_utf8(s.as_bytes())?;
                let res = this.with_caps(|caps| caps.name(name).map(|v| v.as_str().to_string()));
                Ok(res)
            }
            Value::Integer(i) => {
                Ok(this.with_caps(|caps| caps.get(i as usize).map(|v| v.as_str().to_string())))
            }
            _ => Ok(None),
        })
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
            Ok(Ok(lua_try!(regex::RegexSet::new(&patterns).map(RegexSet))))
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

pub fn create_module(lua: &Lua) -> LuaResult<Table> {
    let regex_new = lua.create_function(|lua, re| Ok(Ok(lua_try!(Regex::new(lua, re)))))?;

    lua.create_table_from([
        ("new", Value::Function(regex_new)),
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
}

use std::ops::Deref;

use mlua::{ExternalResult, Lua, MetaMethod, Result, Table, UserData, UserDataMethods, Value};
use moka::sync::Cache;
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
    pub fn new(lua: &Lua, re: String) -> Result<Self> {
        match lua.app_data_ref::<Cache<String, Regex>>() {
            Some(cache) => {
                if let Some(regex) = cache.get(&re) {
                    return Ok(regex);
                }
                let regex = regex::Regex::new(&re).map(Self).to_lua_err()?;
                cache.insert(re, regex.clone());
                Ok(regex)
            }
            None => {
                let cache = Cache::new(REGEX_CACHE_SIZE);
                let regex = regex::Regex::new(&re).map(Self).to_lua_err()?;
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

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([("new", lua.create_function(Regex::new)?)])
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    #[test]
    fn test_module() -> Result<()> {
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

            local re = $regex.new("[,.]")
            local vec = re:split("abc.qwe,rty.asd")
            assert(#vec == 4)
        })
        .exec()?;

        Ok(())
    }
}

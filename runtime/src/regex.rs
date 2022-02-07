use std::ops::Deref;

use linked_hash_map::LinkedHashMap;
use mlua::{ExternalResult, Lua, MetaMethod, UserData, UserDataMethods, Value};
use ouroboros::self_referencing;

const REGEX_CACHE_SIZE: usize = 256;

#[derive(Clone)]
pub struct Regex(regex::Regex);

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &Self::Target {
        &self.0
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
            _ => unreachable!(),
        })
    }
}

pub fn regex_new(lua: &Lua, pattern: String) -> mlua::Result<Regex> {
    // Check cache
    match lua.app_data_mut::<LinkedHashMap<String, Regex>>() {
        Some(mut cache) => {
            if let Some(regex) = cache.get_refresh(&pattern) {
                return Ok(regex.clone());
            }
        }
        None => {
            lua.set_app_data::<LinkedHashMap<String, Regex>>(LinkedHashMap::new());
        }
    }

    let regex = Regex(regex::Regex::new(&pattern).to_lua_err()?);
    let mut cache = lua
        .app_data_mut::<LinkedHashMap<String, Regex>>()
        .expect("Regex cache must exist");
    if cache.len() >= REGEX_CACHE_SIZE {
        cache.pop_front();
    }
    cache.insert(pattern, regex.clone());

    Ok(regex)
}

use std::ops::Deref;
use std::pin::Pin;

use linked_hash_map::LinkedHashMap;
use mlua::{ExternalResult, Lua, MetaMethod, UserData, UserDataMethods, Value};

const REGEX_CACHE_SIZE: usize = 256;

#[derive(Clone)]
pub struct Regex(regex::Regex);

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct RegexCaptures {
    _text: Pin<Box<str>>,
    caps: regex::Captures<'static>,
}

impl UserData for Regex {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("is_match", |_, this, text: String| {
            Ok(this.0.is_match(&text))
        });

        methods.add_method("match", |lua, this, text: Box<str>| {
            let text = Pin::new(text);
            if let Some(caps) = this.0.captures(unsafe { &*(&*text as *const _) }) {
                let caps = RegexCaptures { _text: text, caps };
                return Ok(Value::UserData(lua.create_userdata(caps)?));
            }
            Ok(Value::Nil)
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
                Ok(this.caps.name(name).map(|v| v.as_str()))
            }
            Value::Integer(i) => Ok(this.caps.get(i as usize).map(|v| v.as_str())),
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

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::mem;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use bytes::BufMut;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{body::HttpBody, Body, Method, Request, Uri};
use mlua::{
    AnyUserData, ExternalResult, FromLua, Result as LuaResult, String as LuaString, Table, ToLua,
    UserData, UserDataFields, UserDataMethods, Value,
};

use crate::http::set_headers_metatable;
use crate::lua::regex::Regex;

pub struct LuaRequest {
    req: Request<Body>,
    remote_addr: SocketAddr,
    destination: Option<Uri>,
    timeout: Option<Duration>,
}

impl LuaRequest {
    pub fn new(request: Request<Body>, remote_addr: SocketAddr) -> Self {
        LuaRequest {
            req: request,
            remote_addr,
            destination: None,
            timeout: None,
        }
    }

    pub fn into_parts(self) -> (Request<Body>, Option<Uri>) {
        (self.req, self.destination)
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}

impl Deref for LuaRequest {
    type Target = Request<Body>;

    fn deref(&self) -> &Self::Target {
        &self.req
    }
}

impl DerefMut for LuaRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.req
    }
}

impl UserData for LuaRequest {
    fn add_fields<'lua, F: UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("method", |_, this| Ok(this.method().to_string()));
        fields.add_field_method_set("method", |_, this, method: String| {
            *this.method_mut() = Method::from_bytes(method.as_bytes()).to_lua_err()?;
            Ok(())
        });

        fields.add_field_method_get("uri", |_, this| Ok(this.uri().to_string()));
        fields.add_field_method_set("uri", |_, this, uri: String| {
            *this.uri_mut() = Uri::try_from(uri.as_bytes()).to_lua_err()?;
            Ok(())
        });

        fields.add_field_method_get("uri_path", |_, this| Ok(this.uri().path().to_string()));

        fields.add_field_method_get("remote_addr", |_, this| Ok(this.remote_addr.to_string()));
    }

    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("set_destination", |_, this, dst: String| {
            this.destination = Some(dst.parse().to_lua_err()?);
            Ok(())
        });

        methods.add_method_mut("set_timeout", |_, this, timeout: Option<f64>| {
            match timeout {
                Some(t) if t > 0.0 => this.timeout = Some(Duration::from_secs_f64(t)),
                Some(_) | None => this.timeout = None,
            };
            Ok(())
        });

        methods.add_method("header", |lua, this, name: String| {
            if let Some(val) = this.headers().get(name) {
                return lua.create_string(val.as_bytes()).map(Value::String);
            }
            Ok(Value::Nil)
        });

        methods.add_method("header_all", |lua, this, name: String| {
            let vals = this.headers().get_all(name);
            let vals = vals
                .iter()
                .map(|val| lua.create_string(val.as_bytes()))
                .collect::<LuaResult<Vec<_>>>()?;
            if vals.is_empty() {
                return Ok(Value::Nil);
            }
            vals.to_lua(lua)
        });

        methods.add_method("header_cnt", |_, this, name: String| {
            Ok(this.headers().get_all(name).iter().count())
        });

        methods.add_method(
            "header_match",
            |lua, this, (name, pattern): (String, String)| {
                let regex = Regex::new(lua, pattern)?;
                for hdr_val in this.headers().get_all(name) {
                    if let Ok(val) = hdr_val.to_str() {
                        if regex.is_match(val) {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            },
        );

        methods.add_method_mut("del_header", |_, this, name: String| {
            this.headers_mut().remove(name);
            Ok(())
        });

        methods.add_method_mut(
            "add_header",
            |_, this, (name, value): (String, LuaString)| {
                let name = HeaderName::from_bytes(name.as_bytes()).to_lua_err()?;
                let value = HeaderValue::from_bytes(value.as_bytes()).to_lua_err()?;
                this.headers_mut().append(name, value);
                Ok(())
            },
        );

        methods.add_method_mut(
            "set_header",
            |_, this, (name, value): (String, LuaString)| {
                let name = HeaderName::from_bytes(name.as_bytes()).to_lua_err()?;
                let value = HeaderValue::from_bytes(value.as_bytes()).to_lua_err()?;
                this.headers_mut().insert(name, value);
                Ok(())
            },
        );

        methods.add_method("headers", |lua, this, names: Option<HashSet<String>>| {
            let mut headers = HashMap::new();
            for (name, value) in this.headers() {
                if let Some(ref names) = names {
                    if !names.contains(name.as_str()) {
                        continue;
                    }
                }

                headers
                    .entry(name.to_string())
                    .or_insert_with(Vec::new)
                    .push(lua.create_string(value.as_bytes())?);
            }

            let lua_headers = Table::from_lua(headers.to_lua(lua)?, lua)?;
            set_headers_metatable(lua, lua_headers.clone())?;

            Ok(lua_headers)
        });

        methods.add_method("uri_args", |lua, this, ()| {
            let table = lua.create_table()?;
            if let Some(query) = this.uri().query() {
                for (k, v) in form_urlencoded::parse(query.as_bytes()) {
                    match table.raw_get::<_, Option<Value>>(&*k)? {
                        None => table.raw_set(k, v)?,
                        Some(Value::Table(t)) => {
                            t.raw_insert(t.raw_len() + 1, v)?;
                        }
                        Some(val) => {
                            let inner_table = lua.create_sequence_from([val, v.to_lua(lua)?])?;
                            table.raw_set(k, inner_table)?;
                        }
                    }
                }
            }
            Ok(table)
        });

        methods.add_method_mut("set_uri_args", |_lua, _this, _args: Table| {
            // TODO
            Ok(())
        });

        methods.add_async_function("body", |lua, this: AnyUserData| async move {
            // Check if body cached
            if let Some(body) = this.get_user_value::<Option<LuaString>>()? {
                return Ok(Value::String(body));
            }

            let body = {
                let mut this = this.borrow_mut::<Self>()?;

                let mut body = Body::empty();
                mem::swap(this.body_mut(), &mut body);

                let mut vec = Vec::new();
                while let Some(buf) = body.data().await {
                    vec.put(buf.to_lua_err()?);
                }

                let lua_body = lua.create_string(&vec)?;
                // Restore request body
                *this.body_mut() = Body::from(vec);

                lua_body
            };

            // Cache it
            this.set_user_value(body.clone())?;

            Ok(Value::String(body))
        });
    }
}

// #[cfg(test)]
// mod tests {
//     use mlua::{ExternalResult, Lua, LuaSerdeExt, Result, ToLua, Value};

//     #[test]
//     fn test_uri() -> Result<()> {
//         let lua = Lua::new();
//         // lua.globals().set("Url", LuaUrl("http:/".parse().unwrap()))?;

//         let query = "a=b&a=c&c=d&d";
//         let table = lua.create_table()?;
//         for (k, v) in form_urlencoded::parse(query.as_bytes()) {
//             match table.raw_get::<_, Option<Value>>(&*k)? {
//                 None => table.raw_set(k, v)?,
//                 Some(Value::Table(t)) => {
//                     t.raw_insert(t.raw_len() + 1, v)?;
//                 }
//                 Some(val) => {
//                     let inner_table = lua.create_sequence_from([val, v.to_lua(&lua)?])?;
//                     table.raw_set(k, inner_table)?;
//                 }
//             }
//         }

//         let x = serde_json::to_value(table).unwrap();
//         println!("{}", x);

//         Ok(())
//     }
// }

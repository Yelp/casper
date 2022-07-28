use std::convert::TryFrom;
use std::mem;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use bytes::BufMut;
use hyper::{body::HttpBody, Body, Method, Request, Uri};
use mlua::{
    AnyUserData, ExternalResult, String as LuaString, Table, ToLua, UserData, UserDataFields,
    UserDataMethods, Value,
};

use super::{LuaHttpHeaders, LuaHttpHeadersExt};

pub struct LuaRequest {
    req: Request<Body>,
    remote_addr: Option<SocketAddr>,
    destination: Option<Uri>,
    timeout: Option<Duration>,
}

impl LuaRequest {
    pub fn new(request: Request<Body>) -> Self {
        LuaRequest {
            req: request,
            remote_addr: None,
            destination: None,
            timeout: None,
        }
    }

    pub fn set_remote_addr(&mut self, addr: SocketAddr) {
        self.remote_addr = Some(addr);
    }

    pub fn into_inner(self) -> Request<Body> {
        self.req
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn destination(&self) -> Option<Uri> {
        self.destination.clone()
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

        fields.add_field_method_get("remote_addr", |_, this| {
            Ok(this.remote_addr.map(|s| s.to_string()))
        });
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
            LuaHttpHeadersExt::get(this.headers(), lua, &name)
        });

        methods.add_method("header_all", |lua, this, name: String| {
            LuaHttpHeadersExt::get_all(this.headers(), lua, &name)
        });

        methods.add_method("header_cnt", |lua, this, name: String| {
            LuaHttpHeadersExt::get_cnt(this.headers(), lua, &name)
        });

        methods.add_method(
            "header_match",
            |lua, this, (name, pattern): (String, String)| {
                LuaHttpHeadersExt::is_match(this.headers(), lua, &name, pattern)
            },
        );

        methods.add_method_mut("del_header", |_, this, name: String| {
            this.headers_mut().del(&name)
        });

        methods.add_method_mut(
            "add_header",
            |_, this, (name, value): (String, LuaString)| {
                this.headers_mut().add(&name, value.as_bytes())
            },
        );

        methods.add_method_mut(
            "set_header",
            |_, this, (name, value): (String, LuaString)| {
                this.headers_mut().set(&name, value.as_bytes())
            },
        );

        methods.add_method("headers", |_, this, ()| {
            Ok(LuaHttpHeaders::from(this.headers().clone()))
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

        #[allow(clippy::await_holding_refcell_ref)]
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

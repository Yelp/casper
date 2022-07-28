use std::ops::{Deref, DerefMut};

use hyper::{Body, Response, StatusCode};
use mlua::{
    ExternalError, ExternalResult, String as LuaString, Table, UserData, UserDataFields,
    UserDataMethods, Value,
};

use super::{LuaHttpHeaders, LuaHttpHeadersExt};

pub struct LuaResponse {
    response: Response<Body>,
    pub use_after_response: bool,
    pub is_proxied: bool,
    pub is_stored: bool,
}

impl LuaResponse {
    #[inline]
    pub fn new(response: Response<Body>) -> Self {
        LuaResponse {
            response,
            use_after_response: false,
            is_proxied: false,
            is_stored: false,
        }
    }

    #[inline]
    pub fn into_inner(self) -> Response<Body> {
        self.response
    }

    pub async fn clone_async(&mut self) -> hyper::Result<Self> {
        let bytes = hyper::body::to_bytes(self.body_mut()).await?;
        *self.body_mut() = Body::from(bytes.clone());

        let mut resp_builder = Response::builder().status(self.status());
        *resp_builder.headers_mut().expect("invalid response") = self.headers().clone();

        let mut resp = resp_builder
            .body(Body::from(bytes))
            .map(LuaResponse::new)
            .expect("cannot build response");
        resp.is_stored = self.is_stored;
        resp.is_proxied = self.is_proxied;

        Ok(resp)
    }
}

impl Deref for LuaResponse {
    type Target = Response<Body>;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

impl DerefMut for LuaResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.response
    }
}

impl UserData for LuaResponse {
    fn add_fields<'lua, F: UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("is_proxied", |_, this| Ok(this.is_proxied));
        fields.add_field_method_get("is_stored", |_, this| Ok(this.is_stored));

        fields.add_field_method_get("status", |_, this| Ok(this.status().as_u16()));
        fields.add_field_method_set("status", |_, this, status: u16| {
            *this.status_mut() = StatusCode::from_u16(status).to_lua_err()?;
            Ok(())
        });
    }

    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        // Static constructor
        methods.add_function("new", |lua, (arg, body): (Value, Value)| {
            let resp = match arg {
                Value::Integer(_) => {
                    let status: u16 = lua.unpack(arg)?;
                    let res = Response::builder().status(status);
                    match body {
                        Value::Nil => res.body(Body::empty()),
                        Value::String(b) => res.body(Body::from(b.as_bytes().to_vec())),
                        _ => {
                            let err = format!("invalid body type: {}", body.type_name());
                            return Err(err.to_lua_err());
                        }
                    }
                }
                Value::Table(params) => {
                    let mut res = Response::builder();

                    // Set status
                    if let Some(status) = params.raw_get::<_, Option<u16>>("status")? {
                        res = res.status(status);
                    }

                    // Append headers
                    if let Some(headers) = params.raw_get::<_, Option<Table>>("headers")? {
                        for kv in headers.pairs::<String, Value>() {
                            let (name, value) = kv?;
                            // Maybe `value` is a list of header values
                            if let Value::Table(values) = value {
                                for value in values.raw_sequence_values::<LuaString>() {
                                    res = res.header(name.clone(), value?.as_bytes());
                                }
                            } else {
                                let value = lua.unpack::<LuaString>(value)?;
                                res = res.header(name, value.as_bytes());
                            }
                        }
                    }

                    // Set body
                    if let Some(body) = params.raw_get::<_, Option<LuaString>>("body")? {
                        res.body(Body::from(body.as_bytes().to_vec()))
                    } else {
                        res.body(Body::empty())
                    }
                }
                _ => {
                    let err = format!("invalid arg type: {}", arg.type_name());
                    return Err(err.to_lua_err());
                }
            }
            .to_lua_err()?;
            Ok(LuaResponse::new(resp))
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

        methods.add_method_mut("set_headers", |lua, this, headers: Table| {
            this.headers_mut().replace_all(lua, headers)
        });

        methods.add_method_mut("use_after_response", |_, this, ()| {
            this.use_after_response = true;
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};

    // TODO: More tests

    #[test]
    fn test_response() -> Result<()> {
        let lua = Lua::new();
        lua.globals()
            .set("Response", lua.create_proxy::<super::LuaResponse>()?)?;

        lua.load(chunk! {
            local resp = Response.new(201, "test body")
            assert(resp.is_proxied == false)
            assert(resp.is_stored == false)

            assert(resp.status == 201)
            resp.status = 202
            assert(resp.status == 202)
        })
        .exec()?;

        lua.load(chunk! {
            local resp = Response.new({
                status = 200,
                headers = {
                    ["x-test"] = {"test1","test2"},
                    ["x-test-2"] = "test2",
                },
                body = "test body",
            })
            assert(resp:header("X-Test") == "test1")
            assert(resp:headers()["X-Test-2"][1] == "test2")
        })
        .exec()?;

        Ok(())
    }
}

use std::collections::HashMap;
use std::mem;
use std::ops::{Deref, DerefMut};

use http::{Response, StatusCode};
use hyper::Body;
use mlua::{
    AnyUserData, ExternalError, ExternalResult, FromLua, Lua, Result as LuaResult,
    String as LuaString, Table, UserData, UserDataFields, UserDataMethods, Value,
};
use opentelemetry::{Key as OTKey, Value as OTValue};

use super::{EitherBody, LuaBody, LuaHttpHeaders, LuaHttpHeadersExt};

pub struct LuaResponse {
    response: Response<EitherBody>,
    labels: Option<HashMap<OTKey, OTValue>>, // For metrics
    pub is_proxied: bool,
    pub is_stored: bool,
}

impl LuaResponse {
    #[inline]
    pub fn new(body: LuaBody) -> Self {
        LuaResponse {
            response: Response::new(EitherBody::Body(body)),
            labels: None,
            is_proxied: false,
            is_stored: false,
        }
    }

    /// Returns labels attached to this request
    #[inline]
    pub fn labels(&self) -> Option<&HashMap<OTKey, OTValue>> {
        self.labels.as_ref()
    }

    /// Removes labels attached to this request and returns them
    #[inline]
    pub fn take_labels(&mut self) -> Option<HashMap<OTKey, OTValue>> {
        self.labels.take()
    }

    /// Clones the response including buffering body
    async fn clone(&mut self) -> LuaResult<Self> {
        // Try to buffer body first
        let body = self.body_mut().buffer().await?;
        let body = body.map(LuaBody::Bytes).unwrap_or(LuaBody::Empty);

        let mut response = LuaResponse::from({
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = self.status();
            *resp.version_mut() = self.version();
            *resp.headers_mut() = self.headers().clone();
            resp
        });
        *response.body_mut() = EitherBody::Body(body);
        response.is_proxied = self.is_proxied;
        response.is_stored = self.is_stored;

        Ok(response)
    }

    /// Consumes the response, returning the wrapped hyper Response
    #[inline]
    pub fn into_inner(self) -> Response<Body> {
        self.response.map(|body| body.into())
    }
}

impl Deref for LuaResponse {
    type Target = Response<EitherBody>;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

impl DerefMut for LuaResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.response
    }
}

impl From<Response<Body>> for LuaResponse {
    #[inline]
    fn from(response: Response<Body>) -> Self {
        LuaResponse {
            response: response.map(|body| {
                EitherBody::Body(LuaBody::Hyper {
                    timeout: None,
                    body,
                })
            }),
            labels: None,
            is_proxied: false,
            is_stored: false,
        }
    }
}

impl From<reqwest::Response> for LuaResponse {
    #[inline]
    fn from(mut response: reqwest::Response) -> Self {
        let status = response.status();
        let headers = mem::take(response.headers_mut());
        let mut response = Response::new(Body::wrap_stream(response.bytes_stream()));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        Self::from(response)
    }
}

impl From<LuaResponse> for Response<Body> {
    #[inline]
    fn from(response: LuaResponse) -> Self {
        response.into_inner()
    }
}

impl<'lua> FromLua<'lua> for LuaResponse {
    fn from_lua(value: Value<'lua>, lua: &'lua Lua) -> LuaResult<Self> {
        let mut response = LuaResponse::new(LuaBody::Empty);

        let params = match lua.unpack::<Option<Table>>(value)? {
            Some(params) => params,
            None => return Ok(response),
        };

        if let Ok(Some(status)) = params.raw_get::<_, Option<u16>>("status") {
            *response.status_mut() = StatusCode::from_u16(status)
                .map_err(|err| err.to_string())
                .to_lua_err()?;
        }

        let headers = params
            .raw_get::<_, LuaHttpHeaders>("headers")
            .map_err(|err| format!("invalid headers: {err}"))
            .to_lua_err()?;
        *response.headers_mut() = headers.into();

        let body = params
            .raw_get::<_, LuaBody>("body")
            .map_err(|err| format!("invalid body: {err}"))
            .to_lua_err()?;
        *response.body_mut() = EitherBody::Body(body);

        Ok(response)
    }
}

#[allow(clippy::await_holding_refcell_ref)]
impl UserData for LuaResponse {
    fn add_fields<'lua, F: UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("is_proxied", |_, this| Ok(this.is_proxied));
        fields.add_field_method_get("is_stored", |_, this| Ok(this.is_stored));

        fields.add_field_method_get("status", |_, this| Ok(this.status().as_u16()));
        fields.add_field_method_set("status", |_, this, status: u16| {
            *this.status_mut() = StatusCode::from_u16(status)
                .map_err(|err| err.to_string())
                .to_lua_err()?;
            Ok(())
        });

        fields.add_field_function_get("body", |lua, this| {
            let mut this = this.borrow_mut::<Self>()?;
            this.body_mut().as_userdata(lua)
        });
    }

    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        // Static constructor
        methods.add_function("new", |lua, (arg, body): (Value, Value)| {
            let params = match arg {
                Value::Integer(status) => Value::Table(
                    lua.create_table_from([("status", Value::Integer(status)), ("body", body)])?,
                ),
                val => val,
            };
            LuaResponse::from_lua(params, lua)
        });

        methods.add_async_function("clone", |_, this: AnyUserData| async move {
            let mut this = this.borrow_mut::<Self>()?;
            this.clone().await
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

        methods.add_method_mut("set_headers", |lua, this, value: Value| {
            let headers = match value {
                Value::Nil => Err("headers must be non-nil".to_lua_err()),
                _ => LuaHttpHeaders::from_lua(value, lua)
                    .map_err(|err| format!("invalid headers: {err}"))
                    .to_lua_err(),
            };
            *this.headers_mut() = headers?.into();
            Ok(())
        });

        methods.add_method_mut("set_body", |_, this, new_body| {
            *this.body_mut() = EitherBody::Body(new_body);
            Ok(())
        });

        // Metric labels manipulation
        methods.add_method_mut("set_label", |lua, this, (key, value): (String, Value)| {
            let labels = this.labels.get_or_insert_with(HashMap::new);
            let key = OTKey::new(key);
            match value {
                Value::Nil => labels.remove(&key),
                Value::Boolean(b) => labels.insert(key, OTValue::Bool(b)),
                Value::Integer(i) => labels.insert(key, OTValue::I64(i as i64)),
                Value::Number(n) => labels.insert(key, OTValue::F64(n)),
                v => match lua.coerce_string(v) {
                    Ok(Some(s)) => {
                        let s = s.to_string_lossy().into_owned();
                        labels.insert(key, OTValue::String(s.into()))
                    }
                    _ => None,
                },
            };
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use mlua::{chunk, Lua, Result};

    use super::*;

    #[tokio::test]
    async fn test_response() -> Result<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals()
            .set("Response", lua.create_proxy::<LuaResponse>()?)?;

        // Check default response params
        lua.load(chunk! {
            local resp = Response.new()
            assert(resp.is_proxied == false)
            assert(resp.is_stored == false)
            assert(resp.status == 200)
            assert(resp.body:read() == nil)
        })
        .exec()
        .unwrap();

        // Construct simple response
        lua.load(chunk! {
            local resp = Response.new(400, "bad response")
            assert(resp.status == 400)
            assert(resp.body:read() == "bad response")
        })
        .exec()
        .unwrap();

        // Construct complex response
        lua.load(chunk! {
            local resp = Response.new({
                status = 201,
                body = "hello, world",
                headers = {
                    ["content-type"] = "text/plain",
                },
            })
            assert(resp.status == 201)
            assert(resp.body:read() == "hello, world")
        })
        .exec()
        .unwrap();

        // Check headers manipulation
        lua.load(chunk! {
            local resp = Response.new({
                headers = {
                    ["server"] = "test server",
                    foo = {"bar", "baz"},
                },
            })
            assert(resp:header("Server") == "test server")
            assert(resp:header("foo") == "bar")
            assert(table.concat(resp:header_all("foo"), ",") == "bar,baz")
            assert(resp:header_cnt("foo") == 2)
            assert(resp:header_cnt("none") == 0)
            assert(resp:header_match("foo", "ba"))
            assert(not resp:header_match("foo", "abc"))

            resp:add_header("foo", "test")
            assert(resp:header_cnt("foo") == 3)

            resp:set_header("abc", "cde")
            assert(resp:header("abc") == "cde")

            resp:del_header("foo")
            assert(resp:header("foo") == nil)

            resp:set_headers({
                ["x-new"] = "new"
            })
            assert(resp:header("x-new") == "new")
            assert(resp:header("abc") == nil)

            assert(type(resp:headers()) == "userdata")
        })
        .exec()
        .unwrap();

        let local = tokio::task::LocalSet::new();
        local
            .run_until(
                // Check cloning Response
                lua.load(chunk! {
                    local i = 0
                    local resp = Response.new({
                        status = 202,
                        headers = {
                            foo = "bar",
                        },
                        body = function()
                            if i == 0 then
                                i += 1
                                return "hello, world"
                            end
                        end,
                    })
                    local resp2 = resp:clone()
                    assert(resp2.status == 202)
                    assert(resp2:header("foo") == "bar")
                    assert(resp2.body:data() == "hello, world")
                })
                .exec_async(),
            )
            .await
            .unwrap();

        // Check rewriting body
        lua.load(chunk! {
            local resp = Response.new(200, "hello")
            resp:set_body("world")
            assert(resp.body:read() == "world")
        })
        .exec()
        .unwrap();

        // Check setting labels
        {
            let resp: AnyUserData = lua
                .load(chunk! {
                    local resp = Response.new(200)
                    resp:set_label("hello", "world")
                    return resp
                })
                .eval()
                .unwrap();
            let resp = resp.take::<LuaResponse>()?;
            assert_eq!(resp.labels().unwrap()[&"hello".into()], "world".into());
        }

        Ok(())
    }
}

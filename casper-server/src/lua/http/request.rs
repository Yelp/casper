use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use http::uri::PathAndQuery;
use http::{Method, Request, Uri, Version};
use hyper::Body;
use mlua::{
    AnyUserData, ExternalError, ExternalResult, FromLua, Lua, LuaSerdeExt, Result as LuaResult,
    String as LuaString, Table, ToLua, UserData, UserDataFields, UserDataMethods, Value,
};
use serde_json::Value as JsonValue;

use super::{EitherBody, LuaBody, LuaHttpHeaders, LuaHttpHeadersExt};
use crate::http::proxy_to_upstream;
use crate::types::SimpleHttpClient;

pub struct LuaRequest {
    request: Request<EitherBody>,
    remote_addr: Option<SocketAddr>,
    timeout: Option<Duration>,
}

impl LuaRequest {
    #[inline]
    pub fn new(body: LuaBody) -> Self {
        LuaRequest {
            request: Request::new(EitherBody::Body(body)),
            remote_addr: None,
            timeout: None,
        }
    }

    /// Returns timeout for outgoing request
    #[inline]
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Sets remote address for incoming request
    #[inline]
    pub fn set_remote_addr(&mut self, addr: SocketAddr) {
        self.remote_addr = Some(addr);
    }

    /// Rewrites request's uri path
    fn set_uri_path(&mut self, path: &str) -> LuaResult<()> {
        // Skip everything after `?`
        let mut path = path.split_once('?').unwrap_or((path, "/")).0;
        if path.is_empty() {
            path = "/";
        }
        let mut parts = self.uri().clone().into_parts();
        let path_and_query =
            if let Some(query) = parts.path_and_query.as_ref().and_then(|x| x.query()) {
                PathAndQuery::try_from(format!("{path}?{query}"))
            } else {
                PathAndQuery::try_from(path)
            };
        parts.path_and_query = Some(path_and_query.to_lua_err()?);
        *self.uri_mut() = Uri::from_parts(parts).to_lua_err()?;
        Ok(())
    }

    /// Rewrites request's uri query (can be empty)
    fn set_uri_query(&mut self, query: &str) -> LuaResult<()> {
        let mut parts = self.uri().clone().into_parts();
        let path = parts.path_and_query.as_ref().map(|x| x.path());
        let path = path.unwrap_or("/");
        let path_and_query = if query.is_empty() {
            Some(PathAndQuery::try_from(path)).transpose()
        } else {
            Some(PathAndQuery::try_from(format!("{path}?{query}"))).transpose()
        };
        parts.path_and_query = path_and_query.to_lua_err()?;
        *self.uri_mut() = Uri::from_parts(parts).to_lua_err()?;
        Ok(())
    }

    /// Clones the request including buffering body
    async fn clone(&mut self) -> LuaResult<Self> {
        // Try to buffer body first
        let body = self.body_mut().buffer().await?;
        let body = body.map(LuaBody::Bytes).unwrap_or(LuaBody::Empty);

        let mut request = LuaRequest::from({
            let mut req = Request::new(Body::empty());
            *req.uri_mut() = self.uri().clone();
            *req.version_mut() = self.version();
            *req.method_mut() = self.method().clone();
            *req.headers_mut() = self.headers().clone();
            req
        });
        *request.body_mut() = EitherBody::Body(body);
        request.remote_addr = self.remote_addr;
        request.timeout = self.timeout;

        Ok(request)
    }

    /// Consumes the request, returning the wrapped hyper Request
    #[inline]
    pub fn into_inner(self) -> Request<Body> {
        self.request.map(|body| body.into())
    }
}

impl Deref for LuaRequest {
    type Target = Request<EitherBody>;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

impl DerefMut for LuaRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.request
    }
}

impl From<Request<Body>> for LuaRequest {
    #[inline]
    fn from(request: Request<Body>) -> Self {
        LuaRequest {
            request: request.map(|body| {
                EitherBody::Body(LuaBody::Hyper {
                    timeout: None,
                    body,
                })
            }),
            remote_addr: None,
            timeout: None,
        }
    }
}

impl From<LuaRequest> for Request<Body> {
    #[inline]
    fn from(request: LuaRequest) -> Self {
        request.into_inner()
    }
}

impl TryFrom<LuaRequest> for reqwest::Request {
    type Error = reqwest::Error;

    #[inline]
    fn try_from(request: LuaRequest) -> Result<Self, Self::Error> {
        let timeout = request.timeout;
        let mut request: Self = request.into_inner().try_into()?;
        *request.timeout_mut() = timeout;
        Ok(request)
    }
}

impl<'lua> FromLua<'lua> for LuaRequest {
    fn from_lua(value: Value<'lua>, lua: &'lua Lua) -> LuaResult<Self> {
        let mut request = LuaRequest::new(LuaBody::Empty);
        let params = match lua.unpack::<Option<Table>>(value)? {
            Some(params) => params,
            None => return Ok(request),
        };

        if let Ok(Some(method)) = params.raw_get::<_, Option<LuaString>>("method") {
            *request.method_mut() = Method::from_bytes(method.as_bytes()).to_lua_err()?;
        }

        if let Ok(Some(uri)) = params.raw_get::<_, Option<LuaString>>("uri") {
            *request.uri_mut() = Uri::try_from(uri.as_bytes())
                .map_err(|err| format!("invalid uri: {err}"))
                .to_lua_err()?;
        }

        if let Ok(Some(version)) = params.raw_get::<_, Option<LuaString>>("version") {
            *request.version_mut() = match version.as_bytes() {
                b"1.0" => Version::HTTP_10,
                b"1.1" => Version::HTTP_11,
                b"2" | b"2.0" => Version::HTTP_2,
                _ => return Err("invalid HTTP version").to_lua_err(),
            };
        }

        if let Ok(Some(timeout)) = params.raw_get::<_, Option<f64>>("timeout") {
            if timeout > 0. {
                request.timeout = Some(Duration::from_secs_f64(timeout));
            }
        }

        let headers = params
            .raw_get::<_, LuaHttpHeaders>("headers")
            .map_err(|err| format!("invalid headers: {err}"))
            .to_lua_err()?;
        *request.headers_mut() = headers.into();

        let body = params
            .raw_get::<_, LuaBody>("body")
            .map_err(|err| format!("invalid body: {err}"))
            .to_lua_err()?;
        *request.body_mut() = EitherBody::Body(body);

        Ok(request)
    }
}

#[allow(clippy::await_holding_refcell_ref)]
impl UserData for LuaRequest {
    fn add_fields<'lua, F: UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("method", |lua, this| this.method().as_str().to_lua(lua));
        fields.add_field_method_set("method", |_, this, method: String| {
            *this.method_mut() = Method::from_bytes(method.as_bytes()).to_lua_err()?;
            Ok(())
        });

        fields.add_field_method_get("version", |lua, this| {
            format!("{:?}", this.version())[5..].to_lua(lua)
        });

        fields.add_field_method_get("uri", |_, this| Ok(this.uri().to_string()));
        fields.add_field_method_set("uri", |_, this, uri: LuaString| {
            *this.uri_mut() = Uri::try_from(uri.as_bytes()).to_lua_err()?;
            Ok(())
        });

        fields.add_field_method_get("remote_addr", |_, this| {
            Ok(this.remote_addr.map(|s| s.to_string()))
        });

        fields.add_field_function_get("body", |lua, this| {
            let mut this = this.borrow_mut::<Self>()?;
            this.body_mut().as_userdata(lua)
        });
    }

    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        // Static constructor
        methods.add_function("new", |lua, params: Value| {
            LuaRequest::from_lua(params, lua)
        });

        methods.add_async_function("clone", |_, this: AnyUserData| async move {
            let mut this = this.borrow_mut::<Self>()?;
            this.clone().await
        });

        methods.add_method("timeout", |_, this, ()| {
            Ok(this.timeout.map(|d| d.as_secs_f64()))
        });
        methods.add_method_mut("set_timeout", |_, this, timeout: Option<f64>| {
            match timeout {
                Some(t) if t > 0.0 => this.timeout = Some(Duration::from_secs_f64(t)),
                Some(_) | None => this.timeout = None,
            };
            Ok(())
        });

        methods.add_method("uri_path", |lua, this, ()| this.uri().path().to_lua(lua));
        methods.add_method_mut("set_uri_path", |_, this, path: String| {
            this.set_uri_path(&path)
        });

        methods.add_method("uri_query", |lua, this, ()| this.uri().query().to_lua(lua));
        methods.add_method_mut("set_uri_query", |_, this, query: String| {
            this.set_uri_query(&query)
        });

        methods.add_method("uri_args", |lua, this, ()| {
            let query = this.uri().query().unwrap_or("");
            let args =
                lua_try!(serde_qs::from_str::<HashMap<String, JsonValue>>(query).to_lua_err());
            Ok(Ok(lua.to_value(&args)?))
        });

        methods.add_method_mut("set_uri_args", |_, this, args: Table| {
            let query = serde_qs::to_string(&args).to_lua_err()?;
            this.set_uri_query(&query)
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

        methods.add_async_function(
            "proxy_to_upstream",
            |lua, (this, upstream): (AnyUserData, Option<String>)| async move {
                // Merge request uri with the upstream uri
                let req = this.take::<LuaRequest>()?;
                let client = lua
                    .app_data_ref::<SimpleHttpClient>()
                    .expect("Failed to get http client");
                proxy_to_upstream(&client, req, upstream.as_deref()).await
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use mlua::{chunk, Lua, Result};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[tokio::test]
    async fn test_request() -> Result<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals()
            .set("Request", lua.create_proxy::<LuaRequest>()?)?;

        // Check default request params
        lua.load(chunk! {
            local req = Request.new()
            assert(req.method == "GET")
            assert(req.uri == "/")
            assert(req.remote_addr == nil)
            assert(req.body:read() == nil)
        })
        .exec()
        .unwrap();

        // Construct complex request
        lua.load(chunk! {
            local req = Request.new({
                method = "PUT",
                uri = "http://127.0.0.1/path/a?param1=a&param2=b&param3[]=c",
                version = "2",
                timeout = 3.5,
                headers = {
                    ["user-agent"] = "test ua",
                    foo = {"bar", "baz"},
                },
                body = "hello, world",
            })
            assert(req.method == "PUT")
            assert(req.version == "2.0")
            assert(req:uri_path() == "/path/a")
            assert(req:uri_query() == "param1=a&param2=b&param3[]=c")
            assert(req:uri_args()["param1"] == "a")
            assert(type(req:uri_args()["param3"]) == "table")
            req:set_uri_args({p = "q"})
            assert(req:uri_query() == "p=q")
        })
        .exec()
        .unwrap();

        // Check headers manipulation
        lua.load(chunk! {
            local req = Request.new({
                headers = {
                    ["user-agent"] = "test ua",
                    foo = {"bar", "baz"},
                },
            })
            assert(req:header("User-Agent") == "test ua")
            assert(req:header("foo") == "bar")
            assert(table.concat(req:header_all("foo"), ",") == "bar,baz")
            assert(req:header_cnt("foo") == 2)
            assert(req:header_cnt("none") == 0)
            assert(req:header_match("foo", "ba"))
            assert(not req:header_match("foo", "abc"))

            req:add_header("foo", "test")
            assert(req:header_cnt("foo") == 3)

            req:set_header("abc", "cde")
            assert(req:header("abc") == "cde")

            req:del_header("foo")
            assert(req:header("foo") == nil)

            req:set_headers({
                ["x-new"] = "new"
            })
            assert(req:header("x-new") == "new")
            assert(req:header("abc") == nil)

            assert(type(req:headers()) == "userdata")
        })
        .exec()
        .unwrap();

        let local = tokio::task::LocalSet::new();
        local
            .run_until(
                // Check cloning Request
                lua.load(chunk! {
                    local i = 0
                    local req = Request.new({
                        uri = "http://0.1.2.3/",
                        headers = {
                            ["user-agent"] = "test ua",
                        },
                        body = function()
                            if i == 0 then
                                i += 1
                                return "hello, world"
                            end
                        end,
                    })
                    local req2 = req:clone()
                    assert(req2.uri == "http://0.1.2.3/")
                    assert(req2:header("user-agent") == "test ua")
                    assert(req2.body:data() == "hello, world")
                })
                .exec_async(),
            )
            .await
            .unwrap();

        // Check rewriting body
        lua.load(chunk! {
            local req = Request.new({body = "hello"})
            req:set_body("world")
            assert(req.body:read() == "world")
        })
        .exec()
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_proxy_to_upstream() -> Result<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals()
            .set("Request", lua.create_proxy::<LuaRequest>()?)?;

        // Attach HTTP client
        lua.set_app_data(SimpleHttpClient::new());

        let mock_server = MockServer::start().await;
        let upstream = mock_server.uri();
        Mock::given(method("GET"))
            .and(path("/status"))
            .respond_with(
                ResponseTemplate::new(200)
                    .append_header("x-test", "abc")
                    .set_body_string("hello, world!"),
            )
            .mount(&mock_server)
            .await;

        lua.load(chunk! {
            local req = Request.new({uri = "/status"})
            local resp = req:proxy_to_upstream($upstream)
            assert(resp.status == 200)
            assert(resp:header("x-test") == "abc")
            assert(resp.body:data() == "hello, world!")
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }
}

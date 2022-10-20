use std::mem;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::{Body, HttpBody};
use mlua::{
    AnyUserData, ExternalError, FromLua, Function, Lua, RegistryKey, Result as LuaResult,
    String as LuaString, UserData, Value,
};
use tokio::time;
use tracing::error;

use super::super::{LuaExt, WeakLuaExt};

// TODO: Limit number of fetched bytes
// TODO: Trailers support

pub enum LuaBody {
    Empty,
    Bytes(Bytes),
    Hyper {
        timeout: Option<Duration>,
        body: Body,
    },
}

impl LuaBody {
    /// Sets timeout to fetch whole body
    pub fn set_timeout(&mut self, dur: Option<Duration>) {
        if let LuaBody::Hyper { timeout, .. } = self {
            *timeout = dur;
        }
    }

    /// Buffers the body into memory and returns the buffered data.
    pub async fn buffer(&mut self) -> LuaResult<Option<Bytes>> {
        match self {
            LuaBody::Empty => Ok(None),
            LuaBody::Bytes(bytes) => Ok(Some(bytes.clone())),
            LuaBody::Hyper { timeout, body } => {
                let res = match *timeout {
                    Some(timeout) => time::timeout(timeout, hyper::body::to_bytes(body)).await,
                    None => Ok(hyper::body::to_bytes(body).await),
                };
                match res {
                    Ok(Ok(bytes)) => {
                        *self = LuaBody::Bytes(bytes.clone());
                        Ok(Some(bytes))
                    }
                    Ok(Err(err)) => {
                        *self = LuaBody::Empty;
                        Err(err.to_lua_err())
                    }
                    Err(err) => {
                        *self = LuaBody::Empty;
                        Err(err.to_lua_err())
                    }
                }
            }
        }
    }
}

pub enum EitherBody {
    /// The body is available directly
    Body(LuaBody),
    /// The body is stored in Lua Registry
    Registry(Weak<Lua>, RegistryKey),
}

impl EitherBody {
    pub(crate) fn as_userdata<'lua>(&mut self, lua: &'lua Lua) -> LuaResult<AnyUserData<'lua>> {
        match self {
            EitherBody::Body(tmp_body) => {
                let mut body = LuaBody::Empty;
                mem::swap(tmp_body, &mut body);
                // Move body to Lua registry
                let lua_body = lua.create_userdata(body)?;
                let key = lua.create_registry_value(lua_body.clone())?;
                *self = EitherBody::Registry(lua.weak(), key);
                Ok(lua_body)
            }
            EitherBody::Registry(_, key) => lua.registry_value::<AnyUserData>(key),
        }
    }

    pub(crate) async fn buffer(&mut self) -> LuaResult<Option<Bytes>> {
        match self {
            EitherBody::Body(body) => body.buffer().await,
            EitherBody::Registry(lua, key) => {
                let lua = lua.to_strong();
                let ud = lua
                    .registry_value::<AnyUserData>(&key)
                    .expect("Failed to get body from Lua Registry");
                let mut body = ud
                    .borrow_mut::<LuaBody>()
                    .expect("Failed to borrow body from Lua UserData");
                body.buffer().await
            }
        }
    }
}

impl From<EitherBody> for Body {
    #[inline(always)]
    fn from(body: EitherBody) -> Self {
        match body {
            EitherBody::Body(inner) => inner,
            EitherBody::Registry(lua, key) => {
                let lua = lua.to_strong();
                let ud = lua
                    .registry_value::<AnyUserData>(&key)
                    .expect("Failed to get body from Lua Registry");
                ud.take::<LuaBody>()
                    .expect("Failed to take out body from Lua UserData")
            }
        }
        .into()
    }
}

impl From<Bytes> for LuaBody {
    #[inline(always)]
    fn from(bytes: Bytes) -> Self {
        LuaBody::Bytes(bytes)
    }
}

impl From<Body> for LuaBody {
    #[inline(always)]
    fn from(body: Body) -> Self {
        LuaBody::Hyper {
            timeout: None,
            body,
        }
    }
}

/// Converts the body into [`hyper::Body`]
impl From<LuaBody> for Body {
    #[inline(always)]
    fn from(body: LuaBody) -> Self {
        match body {
            LuaBody::Empty => Body::empty(),
            LuaBody::Bytes(bytes) => Body::from(bytes),
            LuaBody::Hyper { body, .. } => body,
        }
    }
}

impl HttpBody for LuaBody {
    type Data = <Body as HttpBody>::Data;
    type Error = <Body as HttpBody>::Error;

    fn poll_data(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Self::Data, Self::Error>>> {
        match &mut *self {
            LuaBody::Empty => Poll::Ready(None),
            LuaBody::Bytes(bytes) => {
                let bytes = bytes.clone();
                *self = LuaBody::Empty;
                Poll::Ready(Some(Ok(bytes)))
            }
            LuaBody::Hyper { body, .. } => Pin::new(body).poll_data(cx),
        }
    }

    fn poll_trailers(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<http::HeaderMap>, Self::Error>> {
        match &mut *self {
            LuaBody::Hyper { body, .. } => Pin::new(body).poll_trailers(cx),
            _ => Poll::Ready(Ok(None)),
        }
    }
}

impl<'lua> FromLua<'lua> for LuaBody {
    fn from_lua(lua_value: Value<'lua>, lua: &'lua Lua) -> LuaResult<Self> {
        match lua_value {
            Value::Nil => Ok(LuaBody::Empty),
            Value::String(s) => Ok(LuaBody::Bytes(Bytes::from(s.as_bytes().to_vec()))),
            Value::Table(t) => {
                let mut data = Vec::new();
                for chunk in t.raw_sequence_values::<LuaString>() {
                    data.extend_from_slice(chunk?.as_bytes());
                }
                Ok(LuaBody::Bytes(Bytes::from(data)))
            }
            Value::Function(f) => {
                let lua = lua.strong();
                let func_key = lua.create_registry_value(f)?;
                let (mut sender, body) = Body::channel();
                // TODO: spawn task via worker?
                tokio::task::spawn_local(async move {
                    let func = lua.registry_value::<Function>(&func_key).unwrap();
                    // Wait fo sender to be ready
                    if let Err(_) = futures::future::poll_fn(|cx| sender.poll_ready(cx)).await {
                        return;
                    }
                    loop {
                        match func.call::<_, Option<LuaString>>(()) {
                            Ok(Some(chunk)) => {
                                let chunk = Bytes::from(chunk.as_bytes().to_vec());
                                if let Err(err) = sender.send_data(chunk).await {
                                    error!("{err}");
                                    return;
                                }
                            }
                            Ok(None) => return,
                            Err(err) => {
                                error!("{err}");
                                sender.abort();
                                return;
                            }
                        }
                    }
                });
                Ok(LuaBody::from(body))
            }
            Value::UserData(ud) => {
                if let Ok(body) = ud.take::<Self>() {
                    Ok(body)
                } else {
                    Err(format!("cannot make body from wrong userdata").to_lua_err())
                }
            }
            val => {
                let err = format!("cannot make body from {}", val.type_name());
                return Err(err.to_lua_err());
            }
        }
    }
}

impl UserData for LuaBody {
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
        // Static constructor
        methods.add_function("new", |_, body: LuaBody| Ok(body));

        methods.add_method_mut("set_timeout", |_, this, secs: f64| {
            match secs {
                _ if secs <= 0. => this.set_timeout(None),
                _ => this.set_timeout(Some(Duration::from_secs_f64(secs))),
            }
            Ok(())
        });

        // Reads the body and trailers and discards them
        methods.add_async_function("discard", |_, ud: AnyUserData| async move {
            let mut this = ud.borrow_mut::<Self>()?;
            while let Some(Ok(_)) = this.data().await {}
            let _ = this.trailers().await;
            *this = LuaBody::Empty;
            Ok(())
        });

        // Reads the body
        // Returns `string` or `nil, error`
        methods.add_async_function("read", |lua, ud: AnyUserData| async move {
            let mut this = ud.borrow_mut::<Self>()?;
            let bytes = lua_try!(this.buffer().await);
            let data = bytes.map(|b| lua.create_string(&b)).transpose()?;
            *this = LuaBody::Empty; // Drop saved data
            Ok(Ok(data))
        });

        // Returns iterator (function) to read body chunk by chunk
        methods.add_function("reader", |lua, ud: AnyUserData| {
            let body_key = Rc::new(lua.create_registry_value(ud)?);
            lua.create_async_function(move |lua, ()| {
                let body_key = body_key.clone();
                async move {
                    let ud = lua.registry_value::<AnyUserData>(&body_key)?;
                    let mut this = ud.borrow_mut::<Self>()?;
                    let bytes = match &mut *this {
                        LuaBody::Hyper {
                            timeout: Some(timeout),
                            body,
                        } => {
                            let start = time::Instant::now();
                            let bytes = match time::timeout(*timeout, body.data()).await {
                                Ok(res) => res,
                                Err(err) => {
                                    *timeout = Duration::new(0, 0);
                                    return Ok(Err(err.to_string()));
                                }
                            };
                            *timeout = timeout.saturating_sub(start.elapsed());
                            lua_try!(bytes.transpose())
                        }
                        _ => {
                            lua_try!(this.data().await.transpose())
                        }
                    };
                    let data = bytes.map(|b| lua.create_string(&b)).transpose()?;
                    Ok(Ok(data))
                }
            })
        });

        // Buffers the body into memory (if not already) and returns the buffered data
        methods.add_async_function("data", |lua, this: AnyUserData| async move {
            let mut this = this.borrow_mut::<Self>()?;
            let bytes = lua_try!(this.buffer().await);
            let data = bytes.map(|b| lua.create_string(&b)).transpose()?;
            Ok(Ok(data))
        });
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::{
        io::{Error as IoError, ErrorKind},
        time::Duration,
    };

    use hyper::Body;
    use mlua::{chunk, Lua, Result as LuaResult};
    use tokio::task::LocalSet;
    use tokio_stream::{self as stream, StreamExt};

    use super::LuaBody;

    #[tokio::test]
    async fn test_empty_body() -> LuaResult<()> {
        let lua = Lua::new();

        let body = LuaBody::Empty;
        lua.load(chunk! {
            assert($body:data() == nil)
            assert($body:read() == nil)
            local reader = $body:reader()
            assert(reader() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_bytes_body() -> LuaResult<()> {
        let lua = Lua::new();

        let body = LuaBody::Bytes("hello, world".into());
        lua.load(chunk! {
            assert($body:data() == "hello, world")
            assert($body:data() == "hello, world")
            // Read must consume body
            assert($body:read() == "hello, world")
            assert($body:read() == nil)
            assert($body:data() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        let body = LuaBody::Bytes("hello, world".into());
        lua.load(chunk! {
            local reader = $body:reader()
            assert(reader() == "hello, world")
            assert(reader() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_body() -> LuaResult<()> {
        let lua = Lua::new();

        fn make_body_stream() -> LuaBody {
            let chunks: Vec<Result<_, IoError>> = vec![Ok("hello"), Ok(", "), Ok("world")];
            let stream = stream::iter(chunks);
            LuaBody::Hyper {
                timeout: None,
                body: Body::wrap_stream(stream),
            }
        }

        let body = make_body_stream();
        lua.load(chunk! {
            assert($body:data() == "hello, world")
            assert($body:data() == "hello, world")
            // Read must consume body
            assert($body:read() == "hello, world")
            assert($body:read() == nil)
            assert($body:data() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        let body = make_body_stream();
        lua.load(chunk! {
            local reader = $body:reader()
            assert(reader() == "hello")
            assert(reader() == ", ")
            assert(reader() == "world")
            assert(reader() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_body_discard() -> LuaResult<()> {
        let lua = Lua::new();

        let body = LuaBody::Bytes("hello, world".into());
        lua.load(chunk! {
            $body:discard()
            assert($body:read() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_body_errors() -> LuaResult<()> {
        let lua = Lua::new();

        fn make_body_stream() -> LuaBody {
            let chunks: Vec<Result<_, IoError>> = vec![
                Ok("hello"),
                Err(IoError::new(ErrorKind::BrokenPipe, "broken pipe")),
            ];
            let stream = stream::iter(chunks);
            LuaBody::Hyper {
                timeout: None,
                body: Body::wrap_stream(stream),
            }
        }

        let body = make_body_stream();
        lua.load(chunk! {
            local _, err = $body:data()
            assert(err:find("broken pipe") ~= nil)
        })
        .exec_async()
        .await
        .unwrap();

        let body = make_body_stream();
        lua.load(chunk! {
            local _, err = $body:read()
            assert(err:find("broken pipe") ~= nil)
        })
        .exec_async()
        .await
        .unwrap();

        let body = make_body_stream();
        lua.load(chunk! {
            local reader = $body:reader()
            assert(reader() == "hello")
            local _, err = reader()
            assert(err:find("broken pipe") ~= nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_body_error_channel() -> LuaResult<()> {
        let lua = Lua::new();

        let (mut sender, body) = Body::channel();
        let body = LuaBody::Hyper {
            timeout: None,
            body,
        };
        tokio::task::spawn(async move {
            sender.send_data("hello".into()).await.unwrap();
            sender.abort();
        });

        lua.load(chunk! {
            local reader = $body:reader()
            assert(reader() == "hello")
            local _, err = reader()
            assert(err:find("aborted") ~= nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_lua_body() -> LuaResult<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals().set("Body", lua.create_proxy::<LuaBody>()?)?;

        let chunk = chunk! {
            local body = Body.new()
            assert(body:data() == nil)

            body = Body.new("hello, world")
            assert(body:data() == "hello, world")

            body = Body.new({"hello", ", ", "world"})
            assert(body:data() == "hello, world")

            local i = 0
            body = Body.new(function()
                i = i + 1
                if i == 1 then return "hello" end
                if i == 2 then return ", " end
                if i == 3 then return "world" end
                return
            end)
            assert(body:read() == "hello, world")
        };

        let local_set = LocalSet::new();
        local_set
            .run_until(lua.load(chunk).exec_async())
            .await
            .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_lua_body_error() -> LuaResult<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals().set("Body", lua.create_proxy::<LuaBody>()?)?;

        let chunk = chunk! {
            local i = 0
            body = Body.new(function()
                i = i + 1
                if i == 1 then return "hello" end
                error("blah")
            end)
            local _, err = body:read()
            assert(err:find("aborted") ~= nil)
        };

        let local_set = LocalSet::new();
        local_set
            .run_until(lua.load(chunk).exec_async())
            .await
            .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_body_timeout() -> LuaResult<()> {
        let lua = Lua::new();

        let chunks: Vec<Result<_, IoError>> = vec![Ok("hello"), Ok(", "), Ok("world")];
        let body = LuaBody::Hyper {
            timeout: Some(Duration::from_millis(10)),
            body: Body::wrap_stream(stream::iter(chunks).throttle(Duration::from_millis(15))),
        };

        lua.load(chunk! {
            local reader = $body:reader()
            assert(reader() == "hello")
            local _, err = reader()
            assert(err:find("deadline") ~= nil)
            // Reset timeout and try again
            $body:set_timeout(0.010)
            assert(reader() == ", ")
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }
}

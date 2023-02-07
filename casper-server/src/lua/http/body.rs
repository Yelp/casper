use std::error::Error as StdError;
use std::fmt;
use std::mem;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use actix_http::body::{self, BodySize, BodyStream, BoxBody, MessageBody};
use actix_http::Payload;
use bytes::Bytes;
use futures::Stream;
use mlua::{
    AnyUserData, ExternalError, FromLua, Function, Lua, RegistryKey, Result as LuaResult,
    String as LuaString, UserData, Value,
};
use tokio::time;
use tracing::error;

use super::super::{LuaExt, WeakLuaExt};
use crate::http::buffer_payload;

// TODO: Limit number of fetched bytes

pub enum LuaBody {
    None,
    Bytes(Bytes),
    Body {
        body: BoxBody,
        timeout: Option<Duration>,
    },
    Payload {
        payload: Payload,
        length: Option<u64>,
        timeout: Option<Duration>,
    },
}

impl LuaBody {
    /// Returns timeout used to fetch whole body
    pub fn timeout(&self) -> Option<Duration> {
        match self {
            LuaBody::Body { timeout, .. } => *timeout,
            LuaBody::Payload { timeout, .. } => *timeout,
            _ => None,
        }
    }

    /// Sets timeout to fetch whole body
    pub fn set_timeout(&mut self, dur: Option<Duration>) {
        match self {
            LuaBody::Body { timeout, .. } => *timeout = dur,
            LuaBody::Payload { timeout, .. } => *timeout = dur,
            _ => {}
        }
    }

    /// Buffers the body into memory and returns the buffered data.
    pub async fn buffer(&mut self) -> LuaResult<Option<Bytes>> {
        match self {
            LuaBody::None => Ok(None),
            LuaBody::Bytes(bytes) => Ok(Some(bytes.clone())),
            LuaBody::Body { body, timeout } => {
                let tmp_body = mem::replace(body, body::None::new().boxed());
                let res = match *timeout {
                    Some(timeout) => time::timeout(timeout, body::to_bytes(tmp_body)).await,
                    None => Ok(body::to_bytes(tmp_body).await),
                };
                match res {
                    Ok(Ok(bytes)) => {
                        *self = LuaBody::Bytes(bytes.clone());
                        Ok(Some(bytes))
                    }
                    Ok(Err(err)) => {
                        *self = LuaBody::None;
                        Err(err.to_string().into_lua_err())
                    }
                    Err(err) => {
                        *self = LuaBody::None;
                        Err(err.into_lua_err())
                    }
                }
            }
            LuaBody::Payload {
                payload, timeout, ..
            } => {
                let res = match *timeout {
                    Some(timeout) => time::timeout(timeout, buffer_payload(payload)).await,
                    None => Ok(buffer_payload(payload).await),
                };
                match res {
                    Ok(Ok(bytes)) => {
                        *self = LuaBody::Bytes(bytes.clone());
                        Ok(Some(bytes))
                    }
                    Ok(Err(err)) => {
                        *self = LuaBody::None;
                        Err(err.to_string().into_lua_err())
                    }
                    Err(err) => {
                        *self = LuaBody::None;
                        Err(err.into_lua_err())
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

impl fmt::Debug for EitherBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EitherBody::Body(_) => f.write_str("EitherBody::Body"),
            EitherBody::Registry(_, _) => f.write_str("EitherBody::Registry"),
        }
    }
}

impl Default for EitherBody {
    #[inline(always)]
    fn default() -> Self {
        EitherBody::Body(LuaBody::None)
    }
}

impl EitherBody {
    pub(crate) fn as_userdata<'lua>(&mut self, lua: &'lua Lua) -> LuaResult<AnyUserData<'lua>> {
        match self {
            EitherBody::Body(tmp_body) => {
                let mut body = LuaBody::None;
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

    #[allow(clippy::await_holding_refcell_ref)]
    pub(crate) async fn buffer(&mut self) -> LuaResult<Option<Bytes>> {
        match self {
            EitherBody::Body(body) => body.buffer().await,
            EitherBody::Registry(lua, key) => {
                let lua = lua.to_strong();
                let ud = lua
                    .registry_value::<AnyUserData>(key)
                    .expect("Failed to get body from Lua Registry");
                let mut body = ud
                    .borrow_mut::<LuaBody>()
                    .expect("Failed to borrow body from Lua UserData");
                body.buffer().await
            }
        }
    }
}

impl From<EitherBody> for LuaBody {
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
    }
}

impl From<String> for LuaBody {
    #[inline(always)]
    fn from(s: String) -> Self {
        LuaBody::Bytes(Bytes::from(s))
    }
}

impl From<Bytes> for LuaBody {
    #[inline(always)]
    fn from(bytes: Bytes) -> Self {
        LuaBody::Bytes(bytes)
    }
}

impl From<BoxBody> for LuaBody {
    #[inline(always)]
    fn from(body: BoxBody) -> Self {
        let body = match body.try_into_bytes() {
            Ok(bytes) => return LuaBody::Bytes(bytes),
            Err(body) => body,
        };

        LuaBody::Body {
            body,
            timeout: None,
        }
    }
}

impl From<(Payload, Option<u64>)> for LuaBody {
    #[inline(always)]
    fn from((payload, length): (Payload, Option<u64>)) -> Self {
        LuaBody::Payload {
            payload,
            length,
            timeout: None,
        }
    }
}

impl MessageBody for LuaBody {
    type Error = Box<dyn StdError>;

    fn size(&self) -> BodySize {
        match self {
            LuaBody::None => BodySize::None,
            LuaBody::Bytes(b) => BodySize::Sized(b.len() as u64),
            LuaBody::Body { body, .. } => body.size(),
            LuaBody::Payload { length, .. } => {
                length.map(BodySize::Sized).unwrap_or(BodySize::Stream)
            }
        }
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        match *self {
            LuaBody::None => Poll::Ready(None),
            LuaBody::Bytes(ref bytes) => {
                let bytes = bytes.clone();
                *self = LuaBody::None;
                Poll::Ready(Some(Ok(bytes)))
            }
            LuaBody::Body { ref mut body, .. } => Pin::new(body).poll_next(cx),
            LuaBody::Payload {
                ref mut payload, ..
            } => match futures::ready!(Pin::new(payload).poll_next(cx)) {
                Some(Ok(bytes)) => Poll::Ready(Some(Ok(bytes))),
                Some(Err(err)) => Poll::Ready(Some(Err(Box::new(err)))),
                None => Poll::Ready(None),
            },
        }
    }

    fn try_into_bytes(self) -> Result<Bytes, Self>
    where
        Self: Sized,
    {
        match self {
            LuaBody::Bytes(bytes) => Ok(bytes),
            _ => Err(self),
        }
    }
}

impl<'lua> FromLua<'lua> for LuaBody {
    fn from_lua(lua_value: Value<'lua>, lua: &'lua Lua) -> LuaResult<Self> {
        match lua_value {
            Value::Nil => Ok(LuaBody::None),
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
                let stream = futures::stream::poll_fn(move |_| {
                    let func = lua.registry_value::<Function>(&func_key).unwrap();
                    match func.call::<_, Option<LuaString>>(()) {
                        Ok(Some(chunk)) => {
                            Poll::Ready(Some(Ok(Bytes::from(chunk.as_bytes().to_vec()))))
                        }
                        Ok(None) => Poll::Ready(None),
                        Err(err) => {
                            error!("{err:#}");
                            Poll::Ready(Some(Err(err)))
                        }
                    }
                });
                let stream = BodyStream::new(stream);
                Ok(LuaBody::from(stream.boxed()))
            }
            Value::UserData(ud) => {
                if let Ok(body) = ud.take::<Self>() {
                    Ok(body)
                } else {
                    Err("cannot make body from wrong userdata".into_lua_err())
                }
            }
            val => {
                let err = format!("cannot make body from {}", val.type_name());
                Err(err.into_lua_err())
            }
        }
    }
}

#[allow(clippy::await_holding_refcell_ref)]
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

        // Discards the body without reading it
        methods.add_async_function("discard", |_, ud: AnyUserData| async move {
            let mut this = ud.borrow_mut::<Self>()?;
            *this = LuaBody::None;
            Ok(())
        });

        // Reads the body
        // Returns `string` or `nil, error`
        methods.add_async_function("read", |lua, ud: AnyUserData| async move {
            let mut this = ud.borrow_mut::<Self>()?;
            let bytes = lua_try!(this.buffer().await);
            let data = bytes.map(|b| lua.create_string(&b)).transpose()?;
            *this = LuaBody::None; // Drop saved data
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
                    let timeout = this.timeout();
                    let next_chunk =
                        futures::future::poll_fn(|cx| Pin::new(&mut *this).poll_next(cx));
                    let bytes = match timeout {
                        Some(timeout) => {
                            let start = time::Instant::now();
                            let bytes = match time::timeout(timeout, next_chunk).await {
                                Ok(res) => res,
                                Err(err) => {
                                    this.set_timeout(Some(Duration::new(0, 0)));
                                    return Ok(Err(err.to_string()));
                                }
                            };
                            this.set_timeout(Some(timeout.saturating_sub(start.elapsed())));
                            lua_try!(bytes.transpose())
                        }
                        None => {
                            lua_try!(next_chunk.await.transpose())
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

    use actix_http::body::{BodyStream, MessageBody};
    use mlua::{chunk, Lua, Result as LuaResult};
    use tokio_stream::{self as stream, StreamExt};

    use super::LuaBody;

    #[actix_web::test]
    async fn test_empty_body() -> LuaResult<()> {
        let lua = Lua::new();

        let body = LuaBody::None;
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

    #[actix_web::test]
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

    #[actix_web::test]
    async fn test_stream_body() -> LuaResult<()> {
        let lua = Lua::new();

        fn make_body_stream() -> LuaBody {
            let chunks: Vec<Result<_, IoError>> =
                vec![Ok("hello".into()), Ok(", ".into()), Ok("world".into())];
            let stream = stream::iter(chunks);
            LuaBody::Body {
                body: BodyStream::new(stream).boxed(),
                timeout: None,
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

    #[actix_web::test]
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

    #[actix_web::test]
    async fn test_body_errors() -> LuaResult<()> {
        let lua = Lua::new();

        fn make_body_stream() -> LuaBody {
            let chunks: Vec<Result<_, IoError>> = vec![
                Ok("hello".into()),
                Err(IoError::new(ErrorKind::BrokenPipe, "broken pipe")),
            ];
            let stream = stream::iter(chunks);
            LuaBody::Body {
                body: BodyStream::new(stream).boxed(),
                timeout: None,
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

    #[actix_web::test]
    async fn test_lua_body() -> LuaResult<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals().set("Body", lua.create_proxy::<LuaBody>()?)?;

        lua.load(chunk! {
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
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[actix_web::test]
    async fn test_lua_body_error() -> LuaResult<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals().set("Body", lua.create_proxy::<LuaBody>()?)?;

        lua.load(chunk! {
            local i = 0
            body = Body.new(function()
                i = i + 1
                if i == 1 then return "hello" end
                error("blah")
            end)
            local _, err = body:read()
            assert(err:find("blah") ~= nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[actix_web::test]
    async fn test_body_timeout() -> LuaResult<()> {
        let lua = Lua::new();

        let chunks: Vec<Result<_, IoError>> =
            vec![Ok("hello".into()), Ok(", ".into()), Ok("world".into())];
        let stream = stream::iter(chunks).throttle(Duration::from_millis(15));
        let body = LuaBody::Body {
            body: BodyStream::new(Box::pin(stream)).boxed(),
            timeout: Some(Duration::from_millis(10)),
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

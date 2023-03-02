use std::error::Error as StdError;
use std::fmt;
use std::mem;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, TryStreamExt};
use mlua::{
    AnyUserData, ExternalError, FromLua, Function, Lua, RegistryKey, Result as LuaResult,
    String as LuaString, UserData, Value,
};
use ntex::http::body::{self, BodySize, BoxedBodyStream, MessageBody, ResponseBody, SizedStream};
use ntex::http::Payload;
use ntex::util::{Bytes, BytesMut};
use tokio::time;
use tracing::error;

use super::super::{LuaExt, WeakLuaExt};
use crate::http::buffer_body;

// TODO: Limit number of fetched bytes

pub enum LuaBody {
    None,
    Bytes(Bytes),
    Body {
        body: Box<dyn MessageBody>,
        timeout: Option<Duration>,
    },
    Payload {
        payload: Payload,
        length: Option<u64>,
        timeout: Option<Duration>,
    },
}

impl LuaBody {
    /// Returns timeout used to fetch the whole body
    pub fn timeout(&self) -> Option<Duration> {
        match self {
            LuaBody::Body { timeout, .. } => *timeout,
            LuaBody::Payload { timeout, .. } => *timeout,
            _ => None,
        }
    }

    /// Sets timeout to fetch the whole body
    pub fn set_timeout(&mut self, dur: Option<Duration>) {
        match self {
            LuaBody::Body { timeout, .. } => *timeout = dur,
            LuaBody::Payload { timeout, .. } => *timeout = dur,
            _ => {}
        }
    }

    /// Buffers the whole body into memory and returns the buffered data.
    pub async fn buffer(&mut self) -> LuaResult<Option<Bytes>> {
        match self {
            LuaBody::None => Ok(None),
            LuaBody::Bytes(bytes) => Ok(Some(bytes.clone())),
            body => {
                let tmp_body = mem::replace(body, LuaBody::None);
                let buffer_fut = buffer_body(tmp_body);
                let res = match body.timeout() {
                    Some(timeout) => time::timeout(timeout, buffer_fut).await,
                    None => Ok(buffer_fut.await),
                };
                match res {
                    Ok(Ok(bytes)) => {
                        *body = LuaBody::Bytes(bytes.clone());
                        Ok(Some(bytes))
                    }
                    Ok(Err(err)) => {
                        *body = LuaBody::None;
                        Err(err.to_string().into_lua_err())
                    }
                    Err(err) => {
                        *body = LuaBody::None;
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
                let body = mem::replace(tmp_body, LuaBody::None);
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

impl From<&'static str> for LuaBody {
    #[inline(always)]
    fn from(s: &'static str) -> Self {
        LuaBody::Bytes(Bytes::from_static(s.as_bytes()))
    }
}

impl From<Bytes> for LuaBody {
    #[inline(always)]
    fn from(bytes: Bytes) -> Self {
        LuaBody::Bytes(bytes)
    }
}

impl<S> From<BoxedBodyStream<S>> for LuaBody
where
    S: Stream<Item = Result<Bytes, Box<dyn StdError>>> + Unpin + 'static,
{
    #[inline(always)]
    fn from(body: BoxedBodyStream<S>) -> Self {
        LuaBody::Body {
            body: Box::new(body),
            timeout: None,
        }
    }
}

impl From<(Payload, Option<u64>)> for LuaBody {
    #[inline(always)]
    fn from((payload, length): (Payload, Option<u64>)) -> Self {
        if length == Some(0) {
            return LuaBody::Bytes(Bytes::new());
        }

        LuaBody::Payload {
            payload,
            length,
            timeout: None,
        }
    }
}

impl<B> From<ResponseBody<B>> for LuaBody
where
    B: MessageBody + 'static,
{
    #[inline(always)]
    fn from(body: ResponseBody<B>) -> Self {
        match body {
            ResponseBody::Body(body) => LuaBody::Body {
                body: Box::new(body),
                timeout: None,
            },
            ResponseBody::Other(body::Body::None) => LuaBody::None,
            ResponseBody::Other(body::Body::Empty) => LuaBody::Bytes(Bytes::new()),
            ResponseBody::Other(body::Body::Bytes(bytes)) => LuaBody::Bytes(bytes),
            ResponseBody::Other(body::Body::Message(body)) => LuaBody::Body {
                body,
                timeout: None,
            },
        }
    }
}

impl From<LuaBody> for body::Body {
    #[inline]
    fn from(value: LuaBody) -> Self {
        match value {
            LuaBody::None => body::Body::None,
            LuaBody::Bytes(bytes) if bytes.is_empty() => body::Body::Empty,
            LuaBody::Bytes(bytes) => body::Body::Bytes(bytes),
            LuaBody::Body { body, .. } => body::Body::Message(body),
            LuaBody::Payload {
                payload, length, ..
            } => {
                let payload = payload.map_err(|err| Box::new(err) as Box<dyn StdError>);
                match length {
                    Some(length) => {
                        body::Body::Message(Box::new(SizedStream::new(length, payload)))
                    }
                    None => body::Body::Message(Box::new(BoxedBodyStream::new(payload))),
                }
            }
        }
    }
}

impl MessageBody for LuaBody {
    fn size(&self) -> BodySize {
        match self {
            LuaBody::None => BodySize::None,
            LuaBody::Bytes(b) if b.is_empty() => BodySize::Empty,
            LuaBody::Bytes(b) => BodySize::Sized(b.len() as u64),
            LuaBody::Body { body, .. } => body.size(),
            LuaBody::Payload { length, .. } => {
                length.map(BodySize::Sized).unwrap_or(BodySize::Stream)
            }
        }
    }

    fn poll_next_chunk(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Box<dyn StdError>>>> {
        match *self {
            LuaBody::None => Poll::Ready(None),
            LuaBody::Bytes(ref bytes) => {
                let bytes = bytes.clone();
                *self = LuaBody::None;
                Poll::Ready(Some(Ok(bytes)))
            }
            LuaBody::Body { ref mut body, .. } => body.poll_next_chunk(cx),
            LuaBody::Payload {
                ref mut payload, ..
            } => match futures::ready!(payload.poll_recv(cx)) {
                Some(Ok(bytes)) => Poll::Ready(Some(Ok(bytes))),
                Some(Err(err)) => Poll::Ready(Some(Err(Box::new(err)))),
                None => Poll::Ready(None),
            },
        }
    }
}

impl<'lua> FromLua<'lua> for LuaBody {
    fn from_lua(lua_value: Value<'lua>, lua: &'lua Lua) -> LuaResult<Self> {
        match lua_value {
            Value::Nil => Ok(LuaBody::None),
            Value::String(s) => Ok(LuaBody::Bytes(Bytes::from(s.as_bytes().to_vec()))),
            Value::Table(t) => {
                let mut data = BytesMut::new();
                for chunk in t.raw_sequence_values::<LuaString>() {
                    data.extend_from_slice(chunk?.as_bytes());
                }
                Ok(LuaBody::Bytes(data.freeze()))
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
                            Poll::Ready(Some(Err(Box::new(err) as Box<dyn StdError>)))
                        }
                    }
                });
                Ok(LuaBody::from(BoxedBodyStream::new(stream)))
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
        // Returns `bytes` (userdata) or `nil, error`
        methods.add_async_function("read", |lua, ud: AnyUserData| async move {
            let mut this = ud.borrow_mut::<Self>()?;
            let bytes = lua_try!(this.buffer().await);
            let data = bytes.map(|b| lua.create_any_userdata(b)).transpose()?;
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
                    let next_chunk = futures::future::poll_fn(|cx| this.poll_next_chunk(cx));
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
                    let data = bytes.map(|b| lua.create_any_userdata(b)).transpose()?;
                    Ok(Ok(data))
                }
            })
        });

        // Buffers the body into memory (if not already) and returns the buffered data
        methods.add_async_function("data", |lua, this: AnyUserData| async move {
            let mut this = this.borrow_mut::<Self>()?;
            let bytes = lua_try!(this.buffer().await);
            let data = bytes.map(|b| lua.create_any_userdata(b)).transpose()?;
            Ok(Ok(data))
        });

        methods.add_async_function("to_string", |lua, this: AnyUserData| async move {
            let mut this = this.borrow_mut::<Self>()?;
            let bytes = lua_try!(this.buffer().await);
            let data = bytes.map(|b| lua.create_string(&b)).transpose()?;
            Ok(Ok(data))
        });
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::io::{Error as IoError, ErrorKind};
    use std::rc::Rc;
    use std::time::Duration;

    use mlua::{chunk, Lua, Result as LuaResult};
    use ntex::http::body::BoxedBodyStream;
    use tokio_stream::{self as stream, StreamExt};

    use super::LuaBody;

    #[ntex::test]
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

    #[ntex::test]
    async fn test_bytes_body() -> LuaResult<()> {
        let lua = Lua::new();
        super::super::super::bytes::register_types(&lua)?;

        let body = LuaBody::from("hello, world");
        lua.load(chunk! {
            assert($body:to_string() == "hello, world")
            assert($body:to_string() == "hello, world")
            // Read must consume body
            assert($body:read():to_string() == "hello, world")
            assert($body:read() == nil)
            assert($body:data() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        let body = LuaBody::from("hello, world");
        lua.load(chunk! {
            local reader = $body:reader()
            assert(reader():to_string() == "hello, world")
            assert(reader() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[ntex::test]
    async fn test_stream_body() -> LuaResult<()> {
        let lua = Lua::new();
        super::super::super::bytes::register_types(&lua)?;

        fn make_body_stream() -> LuaBody {
            let chunks = vec![Ok("hello".into()), Ok(", ".into()), Ok("world".into())];
            let stream = stream::iter(chunks);
            LuaBody::from(BoxedBodyStream::new(stream))
        }

        let body = make_body_stream();
        lua.load(chunk! {
            assert($body:to_string() == "hello, world")
            assert($body:to_string() == "hello, world")
            // Read must consume body
            assert($body:read():to_string() == "hello, world")
            assert($body:read() == nil)
            assert($body:data() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        let body = make_body_stream();
        lua.load(chunk! {
            local reader = $body:reader()
            assert(reader():to_string() == "hello")
            assert(reader():to_string() == ", ")
            assert(reader():to_string() == "world")
            assert(reader() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[ntex::test]
    async fn test_body_discard() -> LuaResult<()> {
        let lua = Lua::new();
        super::super::super::bytes::register_types(&lua)?;

        let body = LuaBody::from("hello, world");
        lua.load(chunk! {
            $body:discard()
            assert($body:read() == nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[ntex::test]
    async fn test_body_errors() -> LuaResult<()> {
        let lua = Lua::new();
        super::super::super::bytes::register_types(&lua)?;

        fn make_body_stream() -> LuaBody {
            let chunks: Vec<Result<_, Box<dyn StdError>>> = vec![
                Ok("hello".into()),
                Err(Box::new(IoError::new(ErrorKind::BrokenPipe, "broken pipe"))),
            ];
            let stream = stream::iter(chunks);
            LuaBody::from(BoxedBodyStream::new(stream))
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
            assert(reader():to_string() == "hello")
            local _, err = reader()
            assert(err:find("broken pipe") ~= nil)
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[ntex::test]
    async fn test_lua_body() -> LuaResult<()> {
        let lua = Rc::new(Lua::new());
        super::super::super::bytes::register_types(&lua)?;
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals().set("Body", lua.create_proxy::<LuaBody>()?)?;

        lua.load(chunk! {
            local body = Body.new()
            assert(body:data() == nil)

            body = Body.new("hello, world")
            assert(body:to_string() == "hello, world")

            body = Body.new({"hello", ", ", "world"})
            assert(body:to_string() == "hello, world")

            local i = 0
            body = Body.new(function()
                i = i + 1
                if i == 1 then return "hello" end
                if i == 2 then return ", " end
                if i == 3 then return "world" end
                return
            end)
            assert(body:read():to_string() == "hello, world")
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }

    #[ntex::test]
    async fn test_lua_body_error() -> LuaResult<()> {
        let lua = Rc::new(Lua::new());
        super::super::super::bytes::register_types(&lua)?;
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

    #[ntex::test]
    async fn test_body_timeout() -> LuaResult<()> {
        let lua = Lua::new();
        super::super::super::bytes::register_types(&lua)?;

        let chunks: Vec<Result<_, Box<dyn StdError>>> =
            vec![Ok("hello".into()), Ok(", ".into()), Ok("world".into())];
        let stream = stream::iter(chunks).throttle(Duration::from_millis(15));
        let mut body = LuaBody::from(BoxedBodyStream::new(Box::pin(stream)));
        body.set_timeout(Some(Duration::from_millis(10)));

        lua.load(chunk! {
            local reader = $body:reader()
            assert(reader():to_string() == "hello")
            local _, err = reader()
            assert(err:find("deadline") ~= nil)
            // Reset timeout and try again
            $body:set_timeout(0.010)
            assert(reader():to_string() == ", ")
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }
}

use std::io::Result as IoResult;
use std::ops::Deref;

use mlua::{
    AnyUserData, Lua, Result, String as LuaString, Table, UserData, UserDataMethods, Value,
};
use tokio::net::{ToSocketAddrs, UdpSocket};

/// Represents Tokio UDP socket for Lua
struct LuaUdpSocket(UdpSocket);

impl Deref for LuaUdpSocket {
    type Target = UdpSocket;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl LuaUdpSocket {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> IoResult<Self> {
        Ok(Self(UdpSocket::bind(addr).await?))
    }
}

#[allow(clippy::await_holding_refcell_ref)]
impl UserData for LuaUdpSocket {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_async_function(
            "connect",
            |_, (this, addr): (AnyUserData, String)| async move {
                let this = this.borrow::<Self>()?;
                lua_try!(this.connect(addr).await);
                Ok(Ok(Value::Boolean(true)))
            },
        );

        methods.add_method("local_addr", |_, this, _: ()| {
            Ok(this.local_addr()?.to_string())
        });

        methods.add_async_function(
            "send",
            |_, (this, buf): (AnyUserData, Option<LuaString>)| async move {
                let this = this.borrow::<Self>()?;
                let n = match buf {
                    Some(buf) => lua_try!(this.send(buf.as_bytes()).await),
                    None => 0,
                };
                Ok(Ok(n))
            },
        );

        methods.add_async_function(
            "send_to",
            |_, (this, dst, buf): (AnyUserData, String, Option<LuaString>)| async move {
                let this = this.borrow::<Self>()?;
                let n = match buf {
                    Some(buf) => lua_try!(this.send_to(buf.as_bytes(), dst).await),
                    None => 0,
                };
                Ok(Ok(n))
            },
        );
    }
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    let bind = lua.create_async_function(|_, addr: Option<String>| async move {
        let addr = addr.unwrap_or_else(|| "0.0.0.0:0".to_string());
        Ok(Ok(lua_try!(LuaUdpSocket::bind(addr).await)))
    })?;

    lua.create_table_from([("bind", bind)])
}

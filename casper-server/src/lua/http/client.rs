use std::time::Duration;

use mlua::{
    AnyUserData, ExternalResult, FromLua, Result as LuaResult, Table, UserData, UserDataMethods,
    Value,
};
use reqwest::{Client, Request};

use super::{LuaRequest, LuaResponse};

#[derive(Debug)]
pub struct LuaHttpClient(Client);

impl LuaHttpClient {
    async fn request(&self, req: LuaRequest) -> LuaResult<LuaResponse> {
        let timeout = req.timeout();
        let mut req: Request = req.try_into().to_lua_err()?;
        *req.timeout_mut() = timeout;
        let resp = self.0.execute(req).await.to_lua_err()?;
        Ok(LuaResponse::from(resp))
    }
}

impl From<Client> for LuaHttpClient {
    fn from(client: Client) -> Self {
        LuaHttpClient(client)
    }
}

impl<'lua> FromLua<'lua> for LuaHttpClient {
    fn from_lua(value: Value<'lua>, lua: &'lua mlua::Lua) -> LuaResult<Self> {
        if value == Value::Nil {
            return Ok(LuaHttpClient(Client::new()));
        }

        let mut builder = Client::builder();
        let params = lua.unpack::<Table>(value)?;

        if let Ok(Some(val)) = params.raw_get("accept_invalid_certs") {
            builder = builder.danger_accept_invalid_certs(val);
        }

        if let Ok(Some(val)) = params.raw_get("gzip") {
            builder = builder.gzip(val);
        }

        if let Ok(Some(val)) = params.raw_get::<_, Option<u64>>("pool_idle_timeout") {
            match val {
                0 => builder = builder.pool_idle_timeout(None),
                _ => builder = builder.pool_idle_timeout(Duration::from_secs(val)),
            }
        }

        Ok(LuaHttpClient(builder.build().to_lua_err()?))
    }
}

impl UserData for LuaHttpClient {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_function("new", |_lua, _params: Value| {
            Ok(LuaHttpClient::from(Client::new()))
        });

        methods.add_async_function(
            "request",
            |lua, (this, params): (AnyUserData, Value)| async move {
                let this = this.borrow::<Self>()?;
                let req = LuaRequest::from_lua(params, lua)?;
                Ok(Ok(lua_try!(this.request(req).await)))
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
    async fn test_client() -> Result<()> {
        let lua = Rc::new(Lua::new());
        lua.set_app_data(Rc::downgrade(&lua));

        lua.globals()
            .set("Client", lua.create_proxy::<LuaHttpClient>()?)?;

        let mock_server = MockServer::start().await;
        let uri = mock_server.uri();
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
            local client = Client.new()
            local response = client:request({
                uri = $uri.."/status",
            })
            assert(response:header("x-test") == "abc")
            assert(response.body:read() == "hello, world!")
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }
}

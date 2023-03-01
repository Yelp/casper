use std::mem;
use std::time::Duration;

use mlua::{
    AnyUserData, ExternalResult, FromLua, Result as LuaResult, Table, UserData, UserDataMethods,
    Value,
};
use ntex::http::client::{Client, Connector};
use ntex::time::Seconds;

use super::{LuaBody, LuaRequest, LuaResponse};

pub struct LuaHttpClient {
    client: Client,
    no_decompress: bool,
}

impl LuaHttpClient {
    async fn request(&self, mut req: LuaRequest) -> LuaResult<LuaResponse> {
        let mut client_req = self.client.request(req.method().clone(), req.uri());
        if self.no_decompress {
            client_req = client_req.no_decompress();
        }
        if let Some(timeout) = req.timeout() {
            client_req = client_req.timeout(timeout);
        }

        *client_req.headers_mut() = mem::take(client_req.headers_mut());

        let resp = client_req
            .send_body(LuaBody::from(req.take_body()))
            .await
            .map_err(|e| e.to_string())
            .into_lua_err()?;

        Ok(LuaResponse::from(resp))
    }
}

impl From<Client> for LuaHttpClient {
    fn from(client: Client) -> Self {
        LuaHttpClient {
            client,
            no_decompress: false,
        }
    }
}

impl<'lua> FromLua<'lua> for LuaHttpClient {
    fn from_lua(value: Value<'lua>, lua: &'lua mlua::Lua) -> LuaResult<Self> {
        if value == Value::Nil {
            return Ok(LuaHttpClient::from(Client::new()));
        }

        let mut client_builder = Client::build();
        let params = lua.unpack::<Table>(value)?;

        let no_decompress = params.raw_get("no_decompress").unwrap_or(false);

        if let Ok(Some(val)) = params.raw_get::<_, Option<u8>>("max_redirects") {
            match val {
                0 => client_builder = client_builder.disable_redirects(),
                _ => client_builder = client_builder.max_redirects(val as usize),
            }
        }

        if let Ok(Some(val)) = params.raw_get::<_, Option<u64>>("timeout") {
            match val {
                0 => client_builder = client_builder.disable_timeout(),
                _ => client_builder = client_builder.timeout(Duration::from_secs(val)),
            }
        }

        // Connector options

        let mut connector = Connector::new();

        if let Ok(Some(val)) = params.raw_get::<_, Option<u64>>("connect_timeout") {
            connector = connector.timeout(Duration::from_secs(val));
        }

        if let Ok(Some(val)) = params.raw_get::<_, Option<u16>>("keep_alive") {
            connector = connector.keep_alive(Seconds::new(val));
        }

        if let Ok(Some(val)) = params.raw_get::<_, Option<u16>>("lifetime") {
            connector = connector.lifetime(Seconds::new(val));
        }

        if let Ok(Some(val)) = params.raw_get::<_, Option<u64>>("max_connections") {
            connector = connector.limit(val as usize);
        }

        Ok(LuaHttpClient {
            client: client_builder.connector(connector.finish()).finish(),
            no_decompress,
        })
    }
}

#[allow(clippy::await_holding_refcell_ref)]
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

    #[ntex::test]
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
            assert(response.body:to_string() == "hello, world!")
        })
        .exec_async()
        .await
        .unwrap();

        Ok(())
    }
}

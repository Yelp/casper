use std::mem;
use std::time::Duration;

use mlua::{
    ExternalError, FromLua, Lua, Result as LuaResult, Table, UserData, UserDataMethods, Value,
};
use ntex::http::client::{Client, Connector};
use ntex::time::Seconds;
use tracing::{debug, instrument};

use super::{LuaBody, LuaRequest, LuaResponse};

pub struct LuaHttpClient {
    client: Client,
    no_decompress: bool,
}

impl LuaHttpClient {
    #[instrument(skip_all, fields(method = %req.method(), uri = %req.uri()))]
    async fn request(&self, mut req: LuaRequest) -> LuaResult<LuaResponse> {
        let mut client_req = self.client.request(req.method().clone(), req.uri());
        if self.no_decompress {
            client_req = client_req.no_decompress();
        }
        if let Some(timeout) = req.timeout() {
            client_req = client_req.timeout(timeout);
        }

        *client_req.headers_mut() = mem::take(req.headers_mut());

        let resp = client_req
            .send_body(LuaBody::from(req.take_body()))
            .await
            .map_err(|err| err.to_string());

        let resp = match resp {
            Ok(resp) => resp,
            Err(err) => {
                debug!(error = &err, "request error");
                return Err(err.into_lua_err());
            }
        };

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
    fn from_lua(value: Value<'lua>, lua: &'lua Lua) -> LuaResult<Self> {
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

impl UserData for LuaHttpClient {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_function("new", |_lua, _params: Value| {
            Ok(LuaHttpClient::from(Client::new()))
        });

        methods.add_async_method("request", |_, this, req| async move {
            Ok(Ok(lua_try!(this.request(req).await)))
        });
    }
}

#[cfg(test)]
mod tests {
    use mlua::{chunk, Lua, Result};
    use ntex::web::{self, test, App};

    use super::*;

    #[ntex::test]
    async fn test_client() -> Result<()> {
        let lua = Lua::new();

        lua.globals()
            .set("Client", lua.create_proxy::<LuaHttpClient>()?)?;

        let mock_server = test::server(|| {
            App::new().service(
                web::resource("/status").to(|req: web::HttpRequest| async move {
                    web::HttpResponse::Ok()
                        .header("x-test", "abc")
                        .if_some(req.headers().get("hello"), |val, resp| {
                            resp.header("x-origin-hello", val);
                        })
                        .body("hello, world!")
                }),
            )
        });
        let uri = format!("http://{}", mock_server.addr());

        lua.load(chunk! {
            local client = Client.new()
            local response = client:request({
                uri = $uri.."/status",
                headers = {["hello"] = "world"},
            })
            assert(response:header("x-test") == "abc")
            assert(response:header("x-origin-hello") == "world")
            assert(response.body:to_string() == "hello, world!")
        })
        .exec_async()
        .await
    }
}

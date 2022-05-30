use std::collections::HashSet;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Instant;

use anyhow::Result;
use hyper::{header, Body, Request, Response};
use mlua::{FromLua, Function, Lua, RegistryKey, Table, Value, Variadic};
use scopeguard::defer;
use tracing::warn;

use crate::http::{proxy_to_downstream, ProxyError};
use crate::request::LuaRequest;
use crate::response::LuaResponse;
use crate::worker::WorkerData;

#[allow(clippy::await_holding_refcell_ref)]
pub(crate) async fn handler(
    lua: Rc<Lua>,
    data: Rc<WorkerData>,
    req: Request<Body>,
    remote_addr: SocketAddr,
    ctx_key: Rc<RegistryKey>,
) -> Result<Response<Body>> {
    let middleware_list = &data.middleware;

    // Get Lua context table
    let ctx = lua.registry_value::<Table>(&ctx_key)?;

    let lua_req = lua.create_userdata(LuaRequest::new(req, remote_addr))?;
    let mut early_resp = None;

    // Process a chain of Lua's `on_request` actions
    let mut process_level = middleware_list.len();
    let mut skip_middleware = HashSet::new();
    for (i, on_request) in middleware_list
        .iter()
        .enumerate()
        .filter_map(|(i, it)| it.on_request.as_ref().map(|r| (i, r)))
    {
        let start = Instant::now();
        let name = middleware_list[i].name.clone();
        defer! {
            middleware_histogram_rec!(start, "name" => name.clone(), "phase" => "on_request");
        }

        let on_request: Function = lua.registry_value(on_request)?;
        match on_request
            .call_async::<_, Value>((lua_req.clone(), ctx.clone()))
            .await
        {
            // Early Response?
            Ok(Value::UserData(resp)) => {
                if resp.is::<LuaResponse>() {
                    early_resp = Some(resp);
                    // Skip next middleware
                    process_level = i + 1;
                    break;
                }
            }
            Ok(_) => {}
            Err(err) => {
                // Skip faulty middleware and stop processing
                warn!("middleware '{name}' on-request error: {:?}", err);
                process_level = i;
                break;
            }
        }
    }

    // If we got early Response, use it
    // Otherwise proxy to a downstream service
    let lua_resp = match early_resp {
        Some(resp) => resp,
        None => {
            let lua_req = lua_req.take::<LuaRequest>()?;

            // Take out the original request from Lua
            let timeout = lua_req.timeout();
            let (req, dst) = lua_req.into_parts();

            let lua_resp = match proxy_to_downstream(req, dst, timeout).await {
                Ok(resp) => {
                    let mut lua_resp = LuaResponse::new(resp);
                    lua_resp.is_proxied = true;
                    lua_resp
                }
                Err(err) if matches!(err, ProxyError::Uri(..)) => {
                    let resp = Response::builder()
                        .status(500)
                        .header(header::CONTENT_TYPE, "text/plan")
                        .body(Body::from(format!("invalid destination: {err}")))?;
                    LuaResponse::new(resp)
                }
                Err(err) if matches!(err, ProxyError::Timeout(..)) => {
                    let resp = Response::builder()
                        .status(504)
                        .header(header::CONTENT_TYPE, "text/plan")
                        .body(Body::from(err.to_string()))?;
                    LuaResponse::new(resp)
                }
                Err(err) => {
                    let resp = Response::builder()
                        .status(502)
                        .header(header::CONTENT_TYPE, "text/plan")
                        .body(Body::from(err.to_string()))?;
                    LuaResponse::new(resp)
                }
            };
            lua.create_userdata(lua_resp)?
        }
    };

    // Process a chain of Lua's `on_response` actions up to the `process_level`
    // We need to do this in reverse order
    for (i, on_response) in middleware_list
        .iter()
        .enumerate()
        .take(process_level)
        .rev()
        .filter_map(|(i, it)| it.on_response.as_ref().map(|r| (i, r)))
    {
        let start = Instant::now();
        let name = middleware_list[i].name.clone();
        defer! {
            middleware_histogram_rec!(start, "name" => name.clone(), "phase" => "on_response");
        }

        let on_response: Function = lua.registry_value(on_response)?;
        if let Err(err) = on_response
            .call_async::<_, Value>((lua_resp.clone(), ctx.clone()))
            .await
        {
            warn!("middleware '{name}' on-response error: {:?}", err);
            skip_middleware.insert(i);
        }
    }

    // Extract response and check the `use_after_response` flag
    // If it's set, we must clone response and pass it next to `after_response` handler
    let (resp, resp_key) = {
        let mut resp = lua_resp.borrow_mut::<LuaResponse>()?;
        if resp.use_after_response {
            let key = lua.create_registry_value(lua_resp.clone())?;
            (resp.clone().await?.into_inner(), Some(key))
        } else {
            drop(resp);
            (lua_resp.take::<LuaResponse>()?.into_inner(), None)
        }
    };

    // Spawn Lua's post actions
    let lua = lua.clone();
    tokio::task::spawn_local(async move {
        let ctx = get_registry::<Table>(&lua, &ctx_key);

        // Execute `after_response` actions
        for (i, after_response) in data
            .middleware
            .iter()
            .enumerate()
            .take(process_level)
            .rev()
            .filter_map(|(i, it)| it.after_response.as_ref().map(|r| (i, r)))
        {
            if skip_middleware.contains(&i) {
                continue;
            }
            let start = Instant::now();
            let name = data.middleware[i].name.clone();
            defer! {
                middleware_histogram_rec!(start, "name" => name.clone(), "phase" => "after_response");
            }

            let mut args = Variadic::new();
            args.push(Value::Table(ctx.clone()));
            if let Some(resp_key) = resp_key.as_ref() {
                args.push(get_registry(&lua, resp_key));
            }

            if let Ok(after_response) = lua.registry_value::<Function>(after_response) {
                if let Err(err) = after_response.call_async::<_, ()>(args).await {
                    warn!("middleware '{name}' after-response error: {:?}", err);
                }
            }
        }
    });

    Ok(resp)
}

fn get_registry<'lua, T: FromLua<'lua>>(lua: &'lua Lua, key: &RegistryKey) -> T {
    lua.registry_value(key)
        .expect("Unable to get Lua registry value")
}

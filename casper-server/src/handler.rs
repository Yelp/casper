use std::rc::Rc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use hyper::{header, Body, Response};
use mlua::{Function, Value};
use scopeguard::defer;

use crate::http::{proxy_to_upstream, ProxyError};
use crate::lua::{LuaRequest, LuaResponse};
use crate::types::LuaContext;
use crate::worker::WorkerContext;

#[allow(clippy::await_holding_refcell_ref)]
pub(crate) async fn handler(
    worker_ctx: WorkerContext,
    req: LuaRequest,
    lua_ctx: LuaContext,
) -> Result<LuaResponse> {
    let lua = Rc::clone(&worker_ctx.lua);

    // Get Lua context table
    let ctx = lua_ctx.get(&lua);

    let lua_req = lua.create_userdata(req)?;
    let mut early_resp = None;

    // Process a chain of Lua's `on_request` actions
    let mut process_level = worker_ctx.middleware.len();
    for (i, middleware, on_request) in worker_ctx
        .middleware
        .iter()
        .enumerate()
        .filter_map(|(i, mw)| mw.on_request.as_ref().map(|r| (i, mw, r)))
    {
        let start = Instant::now();
        let name = middleware.name.clone();
        defer! {
            middleware_histogram_rec!(start, "name" => name.clone(), "phase" => "on_request");
        }

        let on_request: Function = lua.registry_value(on_request)?;
        match on_request
            .call_async::<_, Value>((lua_req.clone(), ctx.clone()))
            .await
        {
            // Early Response?
            Ok(Value::UserData(resp)) if resp.is::<LuaResponse>() => {
                early_resp = Some(resp);
                // Skip next middleware
                process_level = i + 1;
                break;
            }
            Ok(Value::Nil) => {}
            Ok(r) => {
                middleware_error_counter_add!(1, "name" => name.clone(), "phase" => "on_request");
                return Err(anyhow!(
                    "middleware '{name}'::on-request invalid return type: {}",
                    r.type_name()
                ));
            }
            Err(err) => {
                middleware_error_counter_add!(1, "name" => name.clone(), "phase" => "on_request");
                return Err(anyhow!("middleware '{name}'::on-request error: {err:?}"));
            }
        }
    }

    // If we got early Response, use it
    // Otherwise proxy to an upstream service
    let lua_resp = match early_resp {
        Some(resp) => resp,
        None => {
            let req = lua_req.take::<LuaRequest>()?;
            let client = worker_ctx.http_client.clone();

            let lua_resp = match proxy_to_upstream(client, req).await {
                Ok(resp) => resp,
                Err(err) if matches!(err, ProxyError::Uri(..)) => {
                    let resp = Response::builder()
                        .status(500)
                        .header(header::CONTENT_TYPE, "text/plan")
                        .body(Body::from(format!("invalid upstream: {err}")))?;
                    LuaResponse::from(resp)
                }
                Err(err) if matches!(err, ProxyError::Timeout(..)) => {
                    let resp = Response::builder()
                        .status(504)
                        .header(header::CONTENT_TYPE, "text/plan")
                        .body(Body::from(err.to_string()))?;
                    LuaResponse::from(resp)
                }
                Err(err) => {
                    let resp = Response::builder()
                        .status(502)
                        .header(header::CONTENT_TYPE, "text/plan")
                        .body(Body::from(err.to_string()))?;
                    LuaResponse::from(resp)
                }
            };

            lua.create_userdata(lua_resp)?
        }
    };

    // Process a chain of Lua's `on_response` actions up to the `process_level`
    // We need to do this in reverse order
    for (middleware, on_response) in worker_ctx
        .middleware
        .iter()
        .take(process_level)
        .rev()
        .filter_map(|mw| mw.on_response.as_ref().map(|r| (mw, r)))
    {
        let start = Instant::now();
        let name = middleware.name.clone();
        defer! {
            middleware_histogram_rec!(start, "name" => name.clone(), "phase" => "on_response");
        }

        let on_response: Function = lua.registry_value(on_response)?;
        if let Err(err) = on_response
            .call_async::<_, ()>((lua_resp.clone(), ctx.clone()))
            .await
        {
            middleware_error_counter_add!(1, "name" => name.clone(), "phase" => "on_response");
            return Err(anyhow!("middleware '{name}'::on-response error: {err:?}"));
        }
    }

    Ok(lua_resp.take::<LuaResponse>()?)
}

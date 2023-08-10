use std::collections::HashMap;
use std::time::Instant;

use anyhow::{anyhow, Result};
use mlua::Value;
use ntex::http::StatusCode;
use ntex::web::error::InternalError;
use ntex::web::types::State;
use opentelemetry::{Key as OTKey, Value as OTValue};
use scopeguard::defer;
use tracing::error;

use crate::context::AppContext;
use crate::lua::{LuaBody, LuaRequest, LuaResponse};
use crate::types::LuaContext;

pub(crate) async fn handler(
    req: LuaRequest,
    app_ctx: State<AppContext>,
) -> Result<LuaResponse, InternalError<anyhow::Error>> {
    let start = Instant::now();
    let _req_guard = active_request_guard!();
    let lua = &app_ctx.lua;

    // Create labels container for metrics
    let mut attrs_map: HashMap<OTKey, OTValue> = HashMap::new();
    attrs_map.insert("method".into(), req.method().to_string().into());

    // Create Lua context table
    let lua_ctx = LuaContext::new(lua);

    // Execute inner handler to get response
    let mut resp_result = handler_inner(req, app_ctx, lua_ctx.clone()).await;

    // Collect response labels
    match resp_result {
        Ok(ref mut resp) => {
            // Save lua context table (used by logger)
            resp.extensions_mut().insert(lua_ctx);

            attrs_map.insert("status".into(), (resp.status().as_u16() as i64).into());
            // Read labels set by Lua and attach them
            if let Some(lua_labels) = resp.take_labels() {
                for (k, v) in lua_labels {
                    attrs_map.insert(k, v);
                }
            }
        }
        Err(ref err) => {
            attrs_map.insert("status".into(), 0.into());
            error!(error = ?err, "handler error");
        }
    }
    requests_counter_inc!(attrs_map);
    requests_histogram_rec!(start, attrs_map);

    resp_result.map_err(|err| InternalError::new(err, StatusCode::INTERNAL_SERVER_ERROR))
}

pub(crate) async fn handler_inner(
    req: LuaRequest,
    app_ctx: State<AppContext>,
    lua_ctx: LuaContext,
) -> Result<LuaResponse> {
    let lua = app_ctx.lua.clone();

    let lua_req = lua.create_userdata(req)?;
    let mut early_resp = None;

    // Process a chain of Lua's `on_request` actions
    let mut process_level = app_ctx.filters.len();
    for (i, filter, on_request) in app_ctx
        .filters
        .iter()
        .enumerate()
        .filter_map(|(i, flt)| flt.on_request.as_ref().map(|r| (i, flt, r)))
    {
        let start = Instant::now();
        let name = &filter.name;
        defer! {
            filter_histogram_rec!(start, "name" => name.clone(), "phase" => "on_request");
        }

        match on_request
            .call_async::<_, Value>((lua_req.clone(), lua_ctx.clone()))
            .await
        {
            // Early Response?
            Ok(Value::UserData(resp)) if resp.is::<LuaResponse>() => {
                early_resp = Some(resp);
                // Skip next filter
                process_level = i + 1;
                break;
            }
            Ok(Value::Nil) => {}
            Ok(r) => {
                filter_error_counter_add!(1, "name" => name.clone(), "phase" => "on_request");
                return Err(anyhow!(
                    "filter '{name}'::on-request error: invalid return type '{}'",
                    r.type_name()
                ));
            }
            Err(err) => {
                filter_error_counter_add!(1, "name" => name.clone(), "phase" => "on_request");
                return Err(anyhow!("filter '{name}'::on-request error: {err:#}"));
            }
        }
    }

    // If we got early Response, use it
    // Otherwise call handler function
    let lua_resp = match (early_resp, &app_ctx.handler) {
        (Some(resp), _) => resp,
        (None, Some(handler)) => match handler.call_async((lua_req.clone(), lua_ctx.clone())).await
        {
            Ok(Value::UserData(resp)) if resp.is::<LuaResponse>() => resp,
            Ok(r) => {
                handler_error_counter_add!(1);
                return Err(anyhow!(
                    "handler error: invalid return type '{}'",
                    r.type_name()
                ));
            }
            Err(err) => {
                handler_error_counter_add!(1);
                return Err(anyhow!("handler panic: {err:#}"));
            }
        },
        (None, None) => {
            let mut resp = LuaResponse::new(LuaBody::Bytes("Not Found".into()));
            *resp.status_mut() = StatusCode::NOT_FOUND;
            lua.create_userdata(resp)?
        }
    };

    // Try to consume LuaRequest (important!) to not wait garbage collection
    drop(lua_req.take::<LuaRequest>());

    // Process a chain of Lua's `on_response` actions up to the `process_level`
    // We need to do this in reverse order
    for (filter, on_response) in app_ctx
        .filters
        .iter()
        .take(process_level)
        .rev()
        .filter_map(|flt| flt.on_response.as_ref().map(|r| (flt, r)))
    {
        let start = Instant::now();
        let name = filter.name.clone();
        defer! {
            filter_histogram_rec!(start, "name" => name.clone(), "phase" => "on_response");
        }

        if let Err(err) = on_response
            .call_async::<_, ()>((lua_resp.clone(), lua_ctx.clone()))
            .await
        {
            filter_error_counter_add!(1, "name" => name.clone(), "phase" => "on_response");
            return Err(anyhow!("filter '{name}'::on-response error: {err:#}"));
        }
    }

    let resp = lua_resp.take::<LuaResponse>()?;

    Ok(resp)
}

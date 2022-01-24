use std::collections::HashSet;
use std::rc::Rc;

use anyhow::Result;
use http::header::{self, HeaderName};
use http::uri::Scheme;
use http::HeaderMap;
use hyper::{client::HttpConnector, Body, Client, Request, Response, Uri};
use mlua::{Function as LuaFunction, Lua, RegistryKey, Table as LuaTable, Value, Variadic};
use once_cell::sync::Lazy;
use tracing::{error, instrument, warn};

use crate::request::LuaRequest;
use crate::response::LuaResponse;
use crate::worker::WorkerData;

pub static CLIENT: Lazy<Client<HttpConnector>> = Lazy::new(Client::new);

static HOP_BY_HOP_HEADERS: Lazy<[HeaderName; 8]> = Lazy::new(|| {
    [
        header::CONNECTION,
        HeaderName::from_static("keep-alive"),
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ]
});

fn filter_hop_headers(headers: &mut HeaderMap) {
    for header in &*HOP_BY_HOP_HEADERS {
        headers.remove(header);
    }
}

#[instrument(skip_all, fields(http.uri = %req.uri(), http.method = %req.method()))]
pub(crate) async fn handler(
    lua: Rc<Lua>,
    data: Rc<WorkerData>,
    req: Request<Body>,
    ctx_key: Rc<RegistryKey>,
) -> Result<Response<Body>> {
    match handler_inner(lua, data, req, ctx_key).await {
        Ok(res) => Ok(res),
        Err(err) => {
            error!("{:?}", err);
            Err(err)
        }
    }
}

pub(crate) async fn handler_inner(
    lua: Rc<Lua>,
    data: Rc<WorkerData>,
    req: Request<Body>,
    ctx_key: Rc<RegistryKey>,
) -> Result<Response<Body>> {
    let middleware_list = &data.middleware;

    // Get Lua context table
    let ctx = lua.registry_value::<LuaTable>(&ctx_key)?;

    let lua_req = lua.create_userdata(LuaRequest::new(req))?;
    let mut early_resp = None;

    // Process a chain of Lua's `on_request` actions
    let mut process_level = middleware_list.len();
    let mut skip_middleware = HashSet::new();
    for (i, on_request) in middleware_list
        .iter()
        .enumerate()
        .filter_map(|(i, it)| it.on_request.as_ref().map(|r| (i, r)))
    {
        let on_request: LuaFunction = lua.registry_value(on_request)?;
        match on_request
            .call_async::<_, Value>((lua_req.clone(), ctx.clone()))
            .await
        {
            // Early Response?
            Ok(Value::UserData(resp)) => {
                if resp.is::<LuaResponse>() {
                    early_resp = Some(resp);
                    process_level = i + 1;
                    break;
                }
            }
            Ok(_) => {}
            Err(err) => {
                // Skip faulty middleware
                warn!("middleware on-request error: {:?}", err);
                skip_middleware.insert(i);
            }
        }
    }

    // If we got early Response, use it
    // Otherwise proxy to a downstream service
    let lua_resp = match early_resp {
        Some(resp) => resp,
        None => {
            // Take out the original request from Lua
            let (mut req, body, dst) = lua_req.take::<LuaRequest>()?.into_parts();

            // If body was read by Lua, set it back again
            if let Some(bytes) = body {
                *req.body_mut() = Body::from(bytes);
            }

            let resp = proxy_to_downstream(req, dst).await?;
            let mut lua_resp = LuaResponse::new(resp);
            lua_resp.is_proxied = true;
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
        if skip_middleware.contains(&i) {
            continue;
        }
        let on_response: LuaFunction = lua.registry_value(on_response)?;
        if let Err(err) = on_response
            .call_async::<_, Value>((lua_resp.clone(), ctx.clone()))
            .await
        {
            warn!("middleware on-response error: {:?}", err);
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

    // Spawn Lua's `after_response` actions
    let lua = lua.clone();
    tokio::task::spawn_local(async move {
        let ctx: LuaTable = lua.registry_value(&ctx_key).unwrap();

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

            let mut args = Variadic::new();
            args.push(Value::Table(ctx.clone()));
            if let Some(resp_key) = resp_key.as_ref() {
                args.push(
                    lua.registry_value(resp_key)
                        .expect("cannot fetch response from the Lua registry"),
                );
            }

            if let Ok(after_response) = lua.registry_value::<LuaFunction>(after_response) {
                if let Err(err) = after_response.call_async::<_, ()>(args).await {
                    warn!("middleware after-response error: {:?}", err);
                }
            }
        }

        lua.expire_registry_values();
    });

    Ok(resp)
}

async fn proxy_to_downstream(mut req: Request<Body>, dst: Option<Uri>) -> Result<Response<Body>> {
    // Set destination to forward request
    let mut parts = req.uri().clone().into_parts();
    if let Some(dst_parts) = dst.map(|dst| dst.into_parts()) {
        if let Some(scheme) = dst_parts.scheme {
            parts.scheme = Some(scheme);
        }
        if let Some(authority) = dst_parts.authority {
            parts.authority = Some(authority);
        }
        if let Some(path_and_query) = dst_parts.path_and_query {
            parts.path_and_query = Some(path_and_query);
        }
    }
    // Set scheme to http if not set
    if parts.scheme.is_none() {
        parts.scheme = Some(Scheme::HTTP);
    }
    *req.uri_mut() = Uri::from_parts(parts)?;

    // Proxy to the downstream service
    filter_hop_headers(req.headers_mut());
    let resp = CLIENT.request(req).await?;

    Ok(resp)
}

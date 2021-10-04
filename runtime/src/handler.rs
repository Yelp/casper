use std::rc::Rc;

use http::header::{self, HeaderName};
use http::uri::Scheme;
use http::HeaderMap;
use hyper::{Body, Request, Response, Uri};
use mlua::{Function as LuaFunction, Lua, Table as LuaTable, Value};
use once_cell::sync::Lazy;

use crate::request::LuaRequest;
use crate::response::LuaResponse;
use crate::worker::WorkerData;
use crate::CLIENT;

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

pub(crate) async fn handler(
    lua: Rc<Lua>,
    data: Rc<WorkerData>,
    req: Request<Body>,
) -> Result<Response<Body>, anyhow::Error> {
    let middleware_list = &data.middleware;

    // Create Lua context table
    let ctx = lua.create_table()?;
    let ctx_key = lua.create_registry_value(ctx.clone())?;

    let lua_req = lua.create_userdata(LuaRequest::new(req))?;
    let mut early_resp = None;

    // Process a chain of Lua's `on_request` actions
    for on_request in middleware_list
        .iter()
        .filter_map(|it| it.on_request.as_ref())
    {
        let on_request: LuaFunction = lua.registry_value(on_request)?;
        match on_request
            .call_async::<_, Value>((lua_req.clone(), ctx.clone()))
            .await
        {
            Ok(Value::UserData(resp)) => {
                if resp.is::<LuaResponse<Body>>() {
                    early_resp = Some(resp);
                    break;
                }
            }
            Ok(_) => {}
            Err(err) => println!("middleware error: {}", err),
        }
    }

    // If we got early Response, use it
    // Otherwise proxy to a downstream service
    let lua_resp = match early_resp {
        Some(resp) => resp,
        None => {
            // Take out the original request from Lua
            let (mut req, body, dst) = lua_req.take::<LuaRequest<Body>>()?.into_parts();

            // If body was read by Lua, set it back again
            if let Some(bytes) = body {
                *req.body_mut() = Body::from(bytes);
            }

            let resp = proxy_to_downstream(req, dst).await?;
            lua.create_userdata(LuaResponse::new(resp))?
        }
    };

    // Process a chain of Lua's `on_response` actions
    for on_response in middleware_list
        .iter()
        .filter_map(|it| it.on_response.as_ref())
    {
        let on_response: LuaFunction = lua.registry_value(on_response)?;
        match on_response
            .call_async::<_, Value>((lua_resp.clone(), ctx.clone()))
            .await
        {
            _ => {}
        }
    }

    // Spawn Lua's `after_response` actions
    let lua = lua.clone();
    tokio::task::spawn_local(async move {
        let ctx: LuaTable = lua.registry_value(&ctx_key).unwrap();

        for after_response in data
            .middleware
            .iter()
            .filter_map(|it| it.after_response.as_ref())
        {
            if let Ok(after_response) = lua.registry_value::<LuaFunction>(after_response) {
                let _ = after_response.call_async::<_, ()>(ctx.clone()).await;
            }
        }
    });

    let resp = lua_resp.take::<LuaResponse<Body>>()?.into_inner();
    Ok(resp)
}

async fn proxy_to_downstream(
    mut req: Request<Body>,
    dst: Option<Uri>,
) -> anyhow::Result<Response<Body>> {
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

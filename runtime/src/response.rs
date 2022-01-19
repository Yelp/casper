use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use http::HeaderMap;
use hyper::{
    header::{HeaderName, HeaderValue},
    Body, Response, StatusCode,
};
use mlua::{
    ExternalError, ExternalResult, Lua, Result as LuaResult, String as LuaString,
    Table as LuaTable, ToLua, UserData, UserDataFields, UserDataMethods, Value as LuaValue,
};

pub struct LuaResponse {
    response: Response<Body>,
    pub use_after_response: bool,
    pub is_proxied: bool,
    pub is_cached: bool,
}

impl LuaResponse {
    pub fn new(response: Response<Body>) -> Self {
        LuaResponse {
            response,
            use_after_response: false,
            is_proxied: false,
            is_cached: false,
        }
    }

    pub fn into_inner(self) -> Response<Body> {
        self.response
    }

    pub fn response_mut(&mut self) -> &mut Response<Body> {
        &mut self.response
    }
}

impl Deref for LuaResponse {
    type Target = Response<Body>;

    fn deref(&self) -> &Self::Target {
        &self.response
    }
}

impl DerefMut for LuaResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.response
    }
}

impl UserData for LuaResponse {
    fn add_fields<'lua, F: UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("is_proxied", |_, this| Ok(this.is_proxied));
        fields.add_field_method_get("is_cached", |_, this| Ok(this.is_cached));

        fields.add_field_method_get("status", |_, this| Ok(this.status().as_u16()));
        fields.add_field_method_set("status", |_, this, status: u16| {
            *this.status_mut() = StatusCode::from_u16(status).to_lua_err()?;
            Ok(())
        });
    }

    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("header", |lua, this, name: String| {
            if let Some(val) = this.headers().get(name) {
                return lua.create_string(val.as_bytes()).map(LuaValue::String);
            }
            Ok(LuaValue::Nil)
        });

        methods.add_method("header_all", |lua, this, name: String| {
            let vals = this.headers().get_all(name);
            let vals = vals
                .into_iter()
                .map(|val| lua.create_string(val.as_bytes()))
                .collect::<LuaResult<Vec<_>>>()?;
            if vals.is_empty() {
                return Ok(LuaValue::Nil);
            }
            vals.to_lua(lua)
        });

        methods.add_method("header_cnt", |_, this, name: String| {
            Ok(this.headers().get_all(name).into_iter().count())
        });

        methods.add_method_mut("del_header", |_, this, name: String| {
            this.headers_mut().remove(name);
            Ok(())
        });

        methods.add_method_mut(
            "add_header",
            |_, this, (name, value): (String, LuaString)| {
                let name = HeaderName::from_bytes(name.as_bytes()).to_lua_err()?;
                let value = HeaderValue::from_bytes(value.as_bytes()).to_lua_err()?;
                this.headers_mut().append(name, value);
                Ok(())
            },
        );

        methods.add_method_mut(
            "set_header",
            |_, this, (name, value): (String, LuaString)| {
                let name = HeaderName::from_bytes(name.as_bytes()).to_lua_err()?;
                let value = HeaderValue::from_bytes(value.as_bytes()).to_lua_err()?;
                this.headers_mut().insert(name, value);
                Ok(())
            },
        );

        methods.add_method("headers", |lua, this, ()| {
            let mut headers = HashMap::new();
            for (name, value) in this.headers() {
                headers
                    .entry(name.to_string())
                    .or_insert_with(Vec::new)
                    .push(lua.create_string(value.as_bytes())?);
            }
            Ok(headers)
        });

        methods.add_method_mut("set_headers", |lua, this, headers: LuaTable| {
            let mut new_headers = HeaderMap::new();
            for kv in headers.pairs::<String, LuaValue>() {
                let (name, value) = kv?;
                let name = HeaderName::from_bytes(name.as_bytes()).to_lua_err()?;

                // Maybe `value` is a list of header values
                if let LuaValue::Table(values) = value {
                    for value in values.raw_sequence_values::<LuaString>() {
                        let value = HeaderValue::from_bytes(value?.as_bytes()).to_lua_err()?;
                        new_headers.append(name.clone(), value);
                    }
                } else {
                    let value = lua.unpack::<LuaString>(value)?;
                    let value = HeaderValue::from_bytes(value.as_bytes()).to_lua_err()?;
                    new_headers.append(name, value);
                }
            }
            *this.headers_mut() = new_headers;
            Ok(())
        });

        methods.add_method_mut("use_after_response", |_, this, value: bool| {
            this.use_after_response = value;
            Ok(())
        });
    }
}

impl LuaResponse {
    // Constructor
    pub fn constructor(lua: &Lua, (arg, body): (LuaValue, LuaValue)) -> LuaResult<Self> {
        let res = match arg {
            LuaValue::Integer(_) => {
                let status: u16 = lua.unpack(arg)?;
                let res = Response::builder().status(status);
                match body {
                    LuaValue::Nil => res.body(Body::empty()),
                    LuaValue::String(b) => res.body(Body::from(b.as_bytes().to_vec())),
                    _ => {
                        let err = format!("invalid body type: {}", body.type_name());
                        return Err(err.to_lua_err());
                    }
                }
            }
            LuaValue::Table(params) => {
                let mut res = Response::builder();

                // Set status
                if let Some(status) = params.raw_get::<_, Option<u16>>("status")? {
                    res = res.status(status);
                }

                // Append headers
                if let Some(headers) = params.raw_get::<_, Option<LuaTable>>("headers")? {
                    for kv in headers.pairs::<String, LuaValue>() {
                        let (name, value) = kv?;
                        // Maybe `value` is a list of header values
                        if let LuaValue::Table(values) = value {
                            for value in values.raw_sequence_values::<LuaString>() {
                                res = res.header(name.clone(), value?.as_bytes());
                            }
                        } else {
                            let value = lua.unpack::<LuaString>(value)?;
                            res = res.header(name, value.as_bytes());
                        }
                    }
                }

                // Set body
                if let Some(body) = params.raw_get::<_, Option<LuaString>>("body")? {
                    res.body(Body::from(body.as_bytes().to_vec()))
                } else {
                    res.body(Body::empty())
                }
            }
            _ => {
                let err = format!("invalid arg type: {}", arg.type_name());
                return Err(err.to_lua_err());
            }
        }
        .to_lua_err()?;
        Ok(LuaResponse::new(res))
    }

    pub async fn clone(&mut self) -> hyper::Result<Self> {
        let bytes = hyper::body::to_bytes(self.body_mut()).await?;
        *self.body_mut() = Body::from(bytes.clone());

        let mut resp_builder = Response::builder().status(self.status());
        *resp_builder.headers_mut().expect("invalid response") = self.headers().clone();

        let mut resp = LuaResponse::new(
            resp_builder
                .body(Body::from(bytes))
                .expect("cannot build response"),
        );
        resp.is_cached = self.is_cached;
        resp.is_proxied = self.is_proxied;

        Ok(resp)
    }
}

// #[cfg(test)]
// mod tests {
//     use mlua::{ExternalResult, Lua, LuaSerdeExt, Result, ToLua, Value};

//     #[test]
//     fn test_uri() -> Result<()> {
//         let lua = Lua::new();
//         // lua.globals().set("Url", LuaUrl("http:/".parse().unwrap()))?;

//         let query = "a=b&a=c&c=d&d";
//         let table = lua.create_table()?;
//         for (k, v) in form_urlencoded::parse(query.as_bytes()) {
//             match table.raw_get::<_, Option<Value>>(&*k)? {
//                 None => table.raw_set(k, v)?,
//                 Some(Value::Table(t)) => {
//                     t.raw_insert(t.raw_len() + 1, v)?;
//                 }
//                 Some(val) => {
//                     let inner_table = lua.create_sequence_from([val, v.to_lua(&lua)?])?;
//                     table.raw_set(k, inner_table)?;
//                 }
//             }
//         }

//         let x = serde_json::to_value(table).unwrap();
//         println!("{}", x);

//         Ok(())
//     }
// }

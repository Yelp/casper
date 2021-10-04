use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};

use bytes::BufMut;
use http::Request;
use hyper::body::HttpBody;
use hyper::{
    header::{HeaderName, HeaderValue},
    Method, Uri,
};
use mlua::{
    AnyUserData, ExternalResult, Result as LuaResult, String as LuaString, Table, ToLua, UserData,
    UserDataFields, UserDataMethods, Value,
};

pub struct LuaRequest<T> {
    req: Request<T>,
    body: Option<Vec<u8>>,
    destination: Option<Uri>,
}

impl<T> LuaRequest<T> {
    pub fn new(request: Request<T>) -> Self {
        LuaRequest {
            req: request,
            body: None,
            destination: None,
        }
    }

    pub fn into_parts(self) -> (Request<T>, Option<Vec<u8>>, Option<Uri>) {
        (self.req, self.body, self.destination)
    }
}

impl<T> Deref for LuaRequest<T> {
    type Target = Request<T>;

    fn deref(&self) -> &Self::Target {
        &self.req
    }
}

impl<T> DerefMut for LuaRequest<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.req
    }
}

impl<T> UserData for LuaRequest<T>
where
    T: HttpBody<Error = hyper::Error> + Unpin + 'static,
{
    fn add_fields<'lua, F: UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("method", |_, this| Ok(this.method().to_string()));
        fields.add_field_method_set("method", |_, this, method: String| {
            *this.method_mut() = Method::from_bytes(method.as_bytes()).to_lua_err()?;
            Ok(())
        });

        fields.add_field_method_get("uri", |_, this| Ok(this.uri().to_string()));
        fields.add_field_method_set("uri", |_, this, uri: String| {
            *this.uri_mut() = Uri::try_from(uri.as_bytes()).to_lua_err()?;
            Ok(())
        });
    }

    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("set_destination", |_, this, dst: String| {
            this.destination = Some(dst.parse().to_lua_err()?);
            Ok(())
        });

        methods.add_method("header", |lua, this, name: String| {
            if let Some(val) = this.headers().get(name) {
                return lua.create_string(val.as_bytes()).map(Value::String);
            }
            Ok(Value::Nil)
        });

        methods.add_method("header_all", |lua, this, name: String| {
            let vals = this.headers().get_all(name);
            let vals = vals
                .into_iter()
                .map(|val| lua.create_string(val.as_bytes()))
                .collect::<LuaResult<Vec<_>>>()?;
            if vals.is_empty() {
                return Ok(Value::Nil);
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
                    .or_insert(Vec::new())
                    .push(lua.create_string(value.as_bytes())?);
            }
            Ok(headers)
        });

        methods.add_method("uri_args", |lua, this, ()| {
            let table = lua.create_table()?;
            if let Some(query) = this.uri().query() {
                for (k, v) in form_urlencoded::parse(query.as_bytes()) {
                    match table.raw_get::<_, Option<Value>>(&*k)? {
                        None => table.raw_set(k, v)?,
                        Some(Value::Table(t)) => {
                            t.raw_insert(t.raw_len() + 1, v)?;
                        }
                        Some(val) => {
                            let inner_table = lua.create_sequence_from([val, v.to_lua(lua)?])?;
                            table.raw_set(k, inner_table)?;
                        }
                    }
                }
            }
            Ok(table)
        });

        methods.add_method_mut("set_uri_args", |_lua, _this, _args: Table| {
            // TODO
            Ok(())
        });

        methods.add_async_function("read_body", |lua, this: AnyUserData| async move {
            let mut this = this.borrow_mut::<Self>()?;
            let body = this.body_mut();

            let mut vec = Vec::new();
            while let Some(buf) = body.data().await {
                vec.put(buf.to_lua_err()?);
            }

            let lua_body = lua.create_string(&vec)?;
            this.body = Some(vec);

            Ok(Value::String(lua_body))
        });
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

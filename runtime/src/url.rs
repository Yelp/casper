// struct LuaUrl(url::Url);

// impl Deref for LuaUrl {
//     type Target = url::Url;

//     #[inline]
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl UserData for LuaUrl {
//     fn add_fields<'lua, F: UserDataFields<'lua, Self>>(fields: &mut F) {
//         fields.add_field_method_get("scheme", |_, this| Ok(String::from(this.0.scheme())));
//         fields.add_field_method_set("scheme", |_, this, scheme: String| {
//             this.0
//                 .set_scheme(&scheme)
//                 .map_err(|_| "invalid scheme".to_lua_err())
//         });

//         fields.add_field_method_get("host", |_, this| Ok(this.0.host_str().map(String::from)));
//         fields.add_field_method_set("host", |_, this, host: String| {
//             this.0.set_host(Some(&host)).to_lua_err()
//         });

//         fields.add_field_method_get("port", |_, this| Ok(this.0.port()));
//         fields.add_field_method_set("port", |_, this, port: Option<u16>| {
//             this.0
//                 .set_port(port)
//                 .map_err(|_| "invalid port".to_lua_err())
//         });

//         fields.add_field_method_get("path", |_, this| Ok(String::from(this.0.path())));
//         fields.add_field_method_set("path", |_, this, path: String| Ok(this.0.set_path(&path)));

//         //
//         // Query
//         //
//         fields.add_field_method_get("query", |_, this| Ok(this.0.query().map(String::from)));
//         fields.add_field_method_set("query", |_, this, query: Option<String>| {
//             Ok(this.0.set_query(query.as_deref()))
//         });
//     }

//     fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
//         methods.add_method("query_table", |lua, this, ()| {
//             let table = lua.create_table()?;
//             for (k, v) in this.0.query_pairs() {
//                 match table.raw_get::<_, Option<Value>>(&*k)? {
//                     None => table.raw_set(k, v)?,
//                     Some(Value::Table(t)) => {
//                         t.raw_insert(t.raw_len() + 1, v)?;
//                     }
//                     Some(val) => {
//                         let inner_table = lua.create_sequence_from([val, v.to_lua(lua)?])?;
//                         table.raw_set(k, inner_table)?;
//                     }
//                 }
//             }
//             Ok(table)
//         });

//         methods.add_meta_function(MetaMethod::Call, |_, url: String| {
//             let url = url.parse().to_lua_err()?;
//             Ok(LuaUrl(url))
//         })
//     }
// }

// #[cfg(test)]
// mod tests {
//     use mlua::{ExternalResult, Lua, LuaSerdeExt, Result, ToLua, Value};

//     #[test]
//     fn test_uri() -> Result<()> {
//         let lua = Lua::new();
//         // lua.globals().set("Url", LuaUrl("http:/".parse().unwrap()))?;

//         let query = "a=b&a=c&c=d";
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

//         Ok(())
//     }
// }

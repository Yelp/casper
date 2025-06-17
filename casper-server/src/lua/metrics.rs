use mlua::{Lua, Result, Table, UserData, UserDataMethods, Value};

use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;

struct U64Counter(Counter<u64>);

impl UserData for U64Counter {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method(
            "add",
            |_, this, (value, attributes): (u64, Option<Table>)| {
                this.0.add(value, &from_lua_attributes(attributes)?);
                Ok(())
            },
        );
    }
}

fn from_lua_attributes(attributes: Option<Table>) -> Result<Vec<KeyValue>> {
    let mut attrs = Vec::new();
    if let Some(attributes) = attributes {
        attributes.for_each::<String, Value>(|k, v| {
            match v {
                Value::Boolean(b) => {
                    attrs.push(KeyValue::new(k, b));
                }
                Value::Integer(i) => {
                    attrs.push(KeyValue::new(k, i));
                }
                Value::Number(n) => {
                    attrs.push(KeyValue::new(k, n));
                }
                Value::String(v) => {
                    attrs.push(KeyValue::new(k, v.to_string_lossy()));
                }
                _ => {}
            }
            Ok(())
        })?;
    }
    Ok(attrs)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    let metrics = lua.create_table()?;

    for (name, counter) in &crate::metrics::global().counters {
        metrics.raw_set(name.as_str(), U64Counter(counter.clone()))?;
    }

    Ok(metrics)
}

use mlua::{Lua, Result, Table, UserData, UserDataMethods, Value};

use opentelemetry::metrics::Counter;
use opentelemetry::{Context, KeyValue};

struct U64Counter(Counter<u64>);

impl UserData for U64Counter {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method(
            "add",
            |_, this, (value, attributes): (u64, Option<Table>)| {
                let cx = Context::current();
                this.0.add(&cx, value, &from_lua_attributes(attributes)?);
                Ok(())
            },
        );
    }
}

fn from_lua_attributes(attributes: Option<Table>) -> Result<Vec<KeyValue>> {
    let mut attrs = Vec::new();
    if let Some(attributes) = attributes {
        for kv in attributes.pairs::<String, Value>() {
            match kv? {
                (k, Value::Boolean(b)) => {
                    attrs.push(KeyValue::new(k, b));
                }
                (k, Value::Integer(i)) => {
                    attrs.push(KeyValue::new(k, i as i64));
                }
                (k, Value::Number(n)) => {
                    attrs.push(KeyValue::new(k, n));
                }
                (k, Value::String(v)) => {
                    attrs.push(KeyValue::new(k, v.to_string_lossy().into_owned()));
                }
                _ => {}
            }
        }
    }
    Ok(attrs)
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    let metrics = lua.create_table()?;

    let counters = crate::metrics::METRICS.counters.lock();
    for (name, counter) in &*counters {
        metrics.raw_set(name.as_str(), U64Counter(counter.clone()))?;
    }

    Ok(metrics)
}

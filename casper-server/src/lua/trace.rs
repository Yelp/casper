use mlua::{Lua, Result, Table, UserData, UserDataMethods, Value as LuaValue};
use opentelemetry::trace::get_active_span;
use opentelemetry::KeyValue;

// Handler to a current span (resolved on each method call)
#[derive(Debug)]
struct LuaCurrentSpan;

impl UserData for LuaCurrentSpan {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("set_attribute", |_, _, (key, value): (String, LuaValue)| {
            get_active_span(|span| {
                match value {
                    LuaValue::Integer(i) => span.set_attribute(KeyValue::new(key, i as i64)),
                    LuaValue::Number(n) => span.set_attribute(KeyValue::new(key, n)),
                    LuaValue::Boolean(b) => span.set_attribute(KeyValue::new(key, b)),
                    // TODO: Support opentelemetry::Value::Array variant
                    val => span.set_attribute(KeyValue::new(key, val.to_string()?)),
                }
                Ok(())
            })
        });
    }
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([("current_span", LuaCurrentSpan)])
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures_util::future::BoxFuture;
    use mlua::{chunk, Lua, Result};
    use opentelemetry::sdk::export::trace::{ExportResult, SpanData, SpanExporter};
    use opentelemetry::sdk::trace::{Tracer, TracerProvider};
    use opentelemetry::trace::{Tracer as _, TracerProvider as _};
    use opentelemetry::Key;

    #[test]
    fn test_module() -> Result<()> {
        let lua = Lua::new();
        let trace = super::create_module(&lua)?;

        let (tracer, provider, exporter) = build_test_tracer();
        tracer.in_span("root", |_cx| {
            lua.load(chunk! {
                local span = $trace.current_span
                span:set_attribute("foo", 1)
                span:set_attribute("hello", "world")
            })
            .exec()
            .unwrap();
        });

        drop(provider); // flush all spans
        let spans = exporter.0.lock().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0]
                .attributes
                .get(&Key::from_static_str("foo"))
                .unwrap(),
            &opentelemetry::Value::I64(1)
        );
        assert_eq!(
            spans[0]
                .attributes
                .get(&Key::from_static_str("hello"))
                .unwrap(),
            &opentelemetry::Value::String("world".into())
        );

        Ok(())
    }

    fn build_test_tracer() -> (Tracer, TracerProvider, TestExporter) {
        let exporter = TestExporter::default();
        let provider = TracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        (tracer, provider, exporter)
    }

    #[derive(Clone, Default, Debug)]
    struct TestExporter(Arc<Mutex<Vec<SpanData>>>);

    impl SpanExporter for TestExporter {
        fn export(&mut self, mut batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
            let spans = self.0.clone();
            Box::pin(async move {
                if let Ok(mut inner) = spans.lock() {
                    inner.append(&mut batch);
                }
                Ok(())
            })
        }
    }
}

use itertools::Itertools;
use mlua::{Lua, Result, Table, Table as LuaTable, UserData, UserDataMethods, Value as LuaValue};
use opentelemetry::global::BoxedSpan;
use opentelemetry::trace::{
    get_active_span, Span, SpanKind, Status as SpanStatus, Tracer as _, TracerProvider as _,
};
use opentelemetry::{global, KeyValue};

// Handler to a current span (resolved on each method call)
#[derive(Debug)]
struct LuaCurrentSpan;

impl UserData for LuaCurrentSpan {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        // Record an event in the context of this (current) span.
        methods.add_method(
            "add_event",
            |_, _, (name, attributes): (String, Option<LuaTable>)| {
                get_active_span(|span| {
                    span.add_event(name, attributes.map(into_attributes).unwrap_or_default());
                    Ok(())
                })
            },
        );

        // Set an attribute of this (current) span.
        methods.add_method("set_attribute", |_, _, (key, value)| {
            get_active_span(|span| {
                span.set_attribute(into_keyvalue(key, value)?);
                Ok(())
            })
        });

        // Set multiple attributes of this (current) span.
        methods.add_method("set_attributes", |_, _, attributes: LuaTable| {
            get_active_span(|span| {
                span.set_attributes(into_attributes(attributes));
                Ok(())
            })
        });

        // Set the status of this (current) span.
        methods.add_method("set_status", |_, _, status: Option<String>| {
            get_active_span(move |span| {
                span.set_status(into_status(status));
                Ok(())
            })
        });

        // Update the name of this (current) span.
        methods.add_method_mut("update_name", |_, _, name: String| {
            get_active_span(|span| {
                span.update_name(name);
                Ok(())
            })
        });
    }
}

struct LuaSpan(BoxedSpan);

impl LuaSpan {
    /// Create a new span
    fn new(_: &Lua, (name, kind): (String, Option<String>)) -> Result<LuaSpan> {
        let tracer = global::tracer_provider()
            .tracer_builder("casper-opentelemetry")
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(opentelemetry_semantic_conventions::SCHEMA_URL)
            .build();

        let mut builder = tracer.span_builder(name);
        match kind.as_deref() {
            Some("client") => builder = builder.with_kind(SpanKind::Client),
            Some("server") => builder = builder.with_kind(SpanKind::Server),
            Some("producer") => builder = builder.with_kind(SpanKind::Producer),
            Some("consumer") => builder = builder.with_kind(SpanKind::Consumer),
            _ => {} // internal (default)
        }
        Ok(LuaSpan(builder.start(&tracer)))
    }
}

impl UserData for LuaSpan {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        // Record an event in the context of this span.
        methods.add_method_mut(
            "add_event",
            |_, this, (name, attributes): (String, Option<LuaTable>)| {
                this.0
                    .add_event(name, attributes.map(into_attributes).unwrap_or_default());
                Ok(())
            },
        );

        // Set an attribute of this span.
        methods.add_method_mut("set_attribute", |_, this, (key, value)| {
            this.0.set_attribute(into_keyvalue(key, value)?);
            Ok(())
        });

        // Set multiple attributes of this span.
        methods.add_method_mut("set_attributes", |_, this, attributes: LuaTable| {
            this.0.set_attributes(into_attributes(attributes));
            Ok(())
        });

        // Set the status of this span.
        methods.add_method_mut("set_status", |_, this, status: Option<String>| {
            this.0.set_status(into_status(status));
            Ok(())
        });

        // Update the span name.
        methods.add_method_mut("update_name", |_, this, name: String| {
            this.0.update_name(name);
            Ok(())
        });

        // Signals that the operation described by this span has now ended.
        methods.add_method_mut("finish", |_, this, ()| {
            this.0.end();
            Ok(())
        });
    }
}

fn into_keyvalue(key: LuaValue, value: LuaValue) -> Result<KeyValue> {
    let key = key.to_string()?;
    match value {
        LuaValue::Integer(i) => Ok(KeyValue::new(key, i as i64)),
        LuaValue::Number(n) => Ok(KeyValue::new(key, n)),
        LuaValue::Boolean(b) => Ok(KeyValue::new(key, b)),
        // TODO: Support opentelemetry::Value::Array variant
        val => Ok(KeyValue::new(key, val.to_string()?)),
    }
}

fn into_attributes(attributes: LuaTable) -> Vec<KeyValue> {
    attributes
        .pairs()
        .map_ok(|(key, value)| into_keyvalue(key, value))
        .flatten()
        .filter_map(|kv| kv.ok())
        .collect()
}

fn into_status(status: Option<String>) -> SpanStatus {
    match status {
        None => SpanStatus::Unset,
        Some(msg) if msg.to_ascii_lowercase() == "ok" => SpanStatus::Ok,
        Some(err) => SpanStatus::error(err),
    }
}

pub fn create_module(lua: &Lua) -> Result<Table> {
    lua.create_table_from([
        (
            "current_span",
            LuaValue::UserData(lua.create_userdata(LuaCurrentSpan)?),
        ),
        (
            "new_span",
            LuaValue::Function(lua.create_function(LuaSpan::new)?),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures_util::future::BoxFuture;
    use mlua::{chunk, Lua, Result};
    use opentelemetry::trace::{SpanKind, Status as SpanStatus, Tracer as _, TracerProvider as _};
    use opentelemetry::{global, KeyValue};
    use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
    use opentelemetry_sdk::trace::{Tracer, TracerProvider};
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_current_span() -> Result<()> {
        let lua = Lua::new();
        let trace = super::create_module(&lua)?;

        let (tracer, provider, exporter) = build_test_tracer();
        global::set_tracer_provider(provider);

        tracer.in_span("root", |_cx| {
            lua.load(chunk! {
                local span = $trace.current_span
                span:set_attribute("foo", 1)
                span:set_attribute("hello", "world")
                span:set_status("ok")
                span:add_event("event", { bar = 1 })
            })
            .exec()
            .unwrap();
        });

        global::shutdown_tracer_provider(); // flush all spans
        let spans = exporter.0.lock().unwrap();
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, "root");
        assert!(span.attributes.contains(&KeyValue::new("foo", 1)));
        assert!(span.attributes.contains(&KeyValue::new("hello", "world")));
        assert_eq!(span.status, SpanStatus::Ok);

        // Events
        assert_eq!(span.events.len(), 1);
        let event = span.events.iter().next().unwrap();
        assert_eq!(&event.name, "event");
        assert_eq!(event.attributes[0], KeyValue::new("bar", 1));

        Ok(())
    }

    #[test]
    #[serial]
    fn test_current_span_update_name() -> Result<()> {
        let lua = Lua::new();
        let trace = super::create_module(&lua)?;

        let (tracer, provider, exporter) = build_test_tracer();
        global::set_tracer_provider(provider);

        tracer.in_span("root", |_cx| {
            lua.load(chunk! {
                $trace.current_span:update_name("new_root")
            })
            .exec()
            .unwrap();
        });

        global::shutdown_tracer_provider(); // flush all spans
        let spans = exporter.0.lock().unwrap();
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, "new_root");

        Ok(())
    }

    #[test]
    #[serial]
    fn test_new_span() -> Result<()> {
        let lua = Lua::new();
        let trace = super::create_module(&lua)?;

        let (tracer, provider, exporter) = build_test_tracer();
        global::set_tracer_provider(provider);

        tracer.in_span("root", |_cx| {
            lua.load(chunk! {
                local span = $trace.new_span("TBC", "client")
                span:update_name("child")
                span:set_attributes({ foo = 1, hello = "world" })
                span:set_status("ok")
                span:add_event("event", { bar = 1 })
                span:finish()
            })
            .exec()
            .unwrap();
        });

        global::shutdown_tracer_provider(); // flush all spans
        let spans = exporter.0.lock().unwrap();
        assert_eq!(spans.len(), 2);
        let span = &spans[0];
        assert_eq!(span.name, "child");
        assert_eq!(span.span_kind, SpanKind::Client);
        assert!(span.attributes.contains(&KeyValue::new("foo", 1)));
        assert!(span.attributes.contains(&KeyValue::new("hello", "world")));
        assert_eq!(span.status, SpanStatus::Ok);

        // Events
        assert_eq!(span.events.len(), 1);
        let event = span.events.iter().next().unwrap();
        assert_eq!(&event.name, "event");
        assert_eq!(event.attributes[0], KeyValue::new("bar", 1));

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

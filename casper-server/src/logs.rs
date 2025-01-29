use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer as _;

use crate::config::Config;

pub fn init(_config: &Config) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer().with_filter(filter_fn(|metadata| {
        if metadata.target().starts_with("opentelemetry")
            && metadata.name() == "BatchSpanProcessor.Flush.ExportError"
        {
            // Throttle error logging (temporary workaround)
            // Ideally we need a rate limiting Layer
            static LAST_ERROR_TIMESTAMP: AtomicI64 = AtomicI64::new(0);
            let now = SystemTime::now();
            let now = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
            return LAST_ERROR_TIMESTAMP
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                    if (now - prev).abs() > 1 {
                        Some(now)
                    } else {
                        None
                    }
                })
                .is_ok();
        }
        true
    }));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}

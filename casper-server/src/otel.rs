use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry::global::Error as OtelGlobalError;
use opentelemetry::trace::TraceError;
use opentelemetry_sdk::runtime::TrySendError;

use Ordering::Relaxed;

fn error_handler(err: OtelGlobalError) {
    match err {
        OtelGlobalError::Trace(ref err @ TraceError::Other(ref trace_err))
            if trace_err.is::<TrySendError>() =>
        {
            // Log this error only once per sec to avoid spamming the logs
            static LAST_ERROR_TIMESTAMP: AtomicI64 = AtomicI64::new(0);
            let now = SystemTime::now();
            let now = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
            let res = LAST_ERROR_TIMESTAMP.fetch_update(Relaxed, Relaxed, |prev| {
                if (now - prev).abs() > 1 {
                    Some(now)
                } else {
                    None
                }
            });
            if res.is_ok() {
                eprintln!("OpenTelemetry trace error: {err}");
            }
        }
        OtelGlobalError::Trace(err) => eprintln!("OpenTelemetry trace error: {err}"),
        OtelGlobalError::Metric(err) => eprintln!("OpenTelemetry metrics error: {err}"),
        _ => eprintln!("OpenTelemetry error: {err}"),
    }
}

pub fn set_error_handler() {
    opentelemetry::global::set_error_handler(error_handler)
        .expect("Failed to set opentelemetry global error handler");
}

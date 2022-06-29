use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Automatically decrements a worker's job count when a job finishes (when
/// this gets dropped).
pub(crate) struct JobCountGuard(pub(crate) Arc<AtomicUsize>);

impl JobCountGuard {
    pub fn inc(counter: &Arc<AtomicUsize>) -> Self {
        let counter = Arc::clone(counter);
        counter.fetch_add(1, Ordering::SeqCst);
        JobCountGuard(counter)
    }
}

impl Drop for JobCountGuard {
    fn drop(&mut self) {
        // Decrement the job count
        let previous_value = self.0.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(previous_value >= 1);
    }
}

//! Process-wide counters reported by `State::log_memory_stats`.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Writer threads spawned by `BlockPrinter::print` that have not finished writing to their fifo.
pub static PENDING_BLOCK_WRITES: AtomicUsize = AtomicUsize::new(0);
pub static PENDING_ACCOUNT_BLOCK_WRITES: AtomicUsize = AtomicUsize::new(0);
/// Base64 payload bytes held by writer threads that have encoded but not yet written.
pub static PENDING_WRITE_BYTES: AtomicUsize = AtomicUsize::new(0);

pub static UPDATE_ACCOUNT: Timing = Timing::new("update_account");
pub static NOTIFY_TRANSACTION: Timing = Timing::new("notify_transaction");
pub static NOTIFY_BLOCK_METADATA: Timing = Timing::new("notify_block_metadata");
pub static UPDATE_SLOT_STATUS: Timing = Timing::new("update_slot_status");
/// Time spent waiting for the state lock, across every callback.
pub static STATE_LOCK_WAIT: Timing = Timing::new("state_lock_wait");
/// Blocking `getBlock` calls made by `State::cache_block_from_rpc`.
pub static RPC_BLOCK_FETCH: Timing = Timing::new("rpc_block_fetch");

/// Call count, total and max duration since the last `take`.
pub struct Timing {
    name: &'static str,
    calls: AtomicU64,
    total_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl Timing {
    const fn new(name: &'static str) -> Self {
        Timing {
            name,
            calls: AtomicU64::new(0),
            total_nanos: AtomicU64::new(0),
            max_nanos: AtomicU64::new(0),
        }
    }

    /// Records the time until the returned guard is dropped.
    pub fn start(&'static self) -> TimingGuard {
        TimingGuard {
            timing: self,
            started: Instant::now(),
        }
    }

    pub fn record_since(&self, started: Instant) {
        let nanos = started.elapsed().as_nanos() as u64;
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.total_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.max_nanos.fetch_max(nanos, Ordering::Relaxed);
    }

    /// Returns `name(calls=.. total_ms=.. max_ms=..)` and resets the counters.
    pub fn take(&self) -> String {
        let calls = self.calls.swap(0, Ordering::Relaxed);
        let total_nanos = self.total_nanos.swap(0, Ordering::Relaxed);
        let max_nanos = self.max_nanos.swap(0, Ordering::Relaxed);
        format!(
            "{}(calls={} total_ms={} max_ms={:.3})",
            self.name,
            calls,
            total_nanos / 1_000_000,
            max_nanos as f64 / 1_000_000.0
        )
    }
}

pub struct TimingGuard {
    timing: &'static Timing,
    started: Instant,
}

impl Drop for TimingGuard {
    fn drop(&mut self) {
        self.timing.record_since(self.started);
    }
}

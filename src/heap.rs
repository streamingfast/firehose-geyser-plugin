//! The plugin's heap: the bytes it has allocated and not freed. The plugin shares its process
//! with Agave's own heap, so this is the only way to tell how much of the process is the plugin.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

use mimalloc::MiMalloc;

/// Counters are striped so threads allocating in parallel do not contend on one cache line.
const STRIPES: usize = 64;

#[repr(align(128))]
struct Stripe(AtomicIsize);

static LIVE_BYTES: [Stripe; STRIPES] = [const { Stripe(AtomicIsize::new(0)) }; STRIPES];
static NEXT_STRIPE: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    // Const-initialized and without destructor, so it never allocates and stays usable while
    // the thread exits
    static STRIPE: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[inline]
fn count(bytes: isize) {
    let stripe = STRIPE
        .try_with(|stripe| {
            if stripe.get() == usize::MAX {
                stripe.set(NEXT_STRIPE.fetch_add(1, Ordering::Relaxed) % STRIPES);
            }
            stripe.get()
        })
        .unwrap_or(0);
    LIVE_BYTES[stripe].0.fetch_add(bytes, Ordering::Relaxed);
}

/// mimalloc, counting the bytes allocated and not freed yet.
pub struct CountingMiMalloc;

unsafe impl GlobalAlloc for CountingMiMalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = MiMalloc.alloc(layout);
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = MiMalloc.alloc_zeroed(layout);
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        MiMalloc.dealloc(ptr, layout);
        count(-(layout.size() as isize));
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = MiMalloc.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            count(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

/// Bytes the plugin allocated and has not freed.
pub fn live_bytes() -> usize {
    let live: isize = LIVE_BYTES
        .iter()
        .map(|stripe| stripe.0.load(Ordering::Relaxed))
        .sum();
    live.max(0) as usize
}

/// Returns to the OS the memory mimalloc holds but no longer uses, including what threads that
/// have exited freed. Expensive: it walks every heap.
pub fn release_free_memory() {
    // SAFETY: no preconditions
    unsafe { libmimalloc_sys::mi_collect(true) }
}

/// Anonymous memory of the whole validator process, resident and swapped, in bytes, from
/// `/proc/self/status`. `None` where that file does not exist.
pub fn process_anonymous_memory() -> Option<(u64, u64)> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|value| {
                value
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .map(|kb| kb * 1024)
    };
    Some((field("RssAnon:")?, field("VmSwap:")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_bytes_include_held_allocations() {
        let held = vec![1u8; 256 << 20];
        assert!(live_bytes() >= held.len());
        drop(held);
    }
}

//! Prefetch hints: start bringing memory into the CPU's caches before it
//! is read, so that several cache misses overlap instead of following one
//! another.
//!
//! A point read is a chain of dependent loads (a filter line, the index, a
//! cache entry, the block, the entries in it), each a likely miss on data
//! larger than the CPU's caches. Where the next addresses are known early,
//! asking for them all at once turns a chain of misses into one wait.

/// Start fetching the cache line holding `at`. Compiled out under Miri and
/// off x86_64.
#[inline(always)]
pub fn prefetch_line(at: *const u8) {
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        // SAFETY: a prefetch is a hint. It never faults, whatever the
        // address, and has no effect a program can observe besides
        // timing; the pointer is not dereferenced.
        #[allow(unused_unsafe)]
        unsafe {
            _mm_prefetch::<_MM_HINT_T0>(at.cast())
        };
    }
    #[cfg(not(all(target_arch = "x86_64", not(miri))))]
    let _ = at;
}

/// Start fetching every cache line of `bytes`.
#[inline]
pub fn prefetch(bytes: &[u8]) {
    let (start, len) = (bytes.as_ptr(), bytes.len());
    if len == 0 {
        return;
    }
    let mut at = 0;
    while at + 256 <= len {
        prefetch_line(start.wrapping_add(at));
        prefetch_line(start.wrapping_add(at + 64));
        prefetch_line(start.wrapping_add(at + 128));
        prefetch_line(start.wrapping_add(at + 192));
        at += 256;
    }
    while at < len {
        prefetch_line(start.wrapping_add(at));
        at += 64;
    }
    // The last byte's line, which steps from an unaligned start can miss.
    prefetch_line(start.wrapping_add(len - 1));
}

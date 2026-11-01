//! The synchronisation the commit path is built on, swappable for `loom`'s.
//!
//! Normally these are `parking_lot`'s lock and condition variable and the
//! standard atomics. Built with `--cfg loom`, they are `loom`'s, behind the
//! same interface, so [`crate::commit_queue`] can be model-checked: `loom`
//! runs its tests under every interleaving of their threads.

use std::time::Duration;

#[cfg(not(loom))]
pub use parking_lot::{Condvar, Mutex};
#[cfg(not(loom))]
pub use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
#[cfg(not(loom))]
pub use std::sync::Arc;

#[cfg(loom)]
pub use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
#[cfg(loom)]
pub use loom::sync::Arc;
#[cfg(loom)]
pub use shim::{Condvar, Mutex};

/// `loom`'s lock and condition variable with `parking_lot`'s interface.
#[cfg(loom)]
mod shim {
    use std::ops::{Deref, DerefMut};

    pub struct Mutex<T>(loom::sync::Mutex<T>);

    pub struct MutexGuard<'a, T>(Option<loom::sync::MutexGuard<'a, T>>);

    impl<T> Mutex<T> {
        pub fn new(value: T) -> Self {
            Mutex(loom::sync::Mutex::new(value))
        }

        pub fn lock(&self) -> MutexGuard<'_, T> {
            MutexGuard(Some(self.0.lock().expect("not poisoned")))
        }
    }

    impl<T> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &T {
            self.0.as_ref().expect("held")
        }
    }

    impl<T> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            self.0.as_mut().expect("held")
        }
    }

    pub struct Condvar(loom::sync::Condvar);

    impl Condvar {
        pub fn new() -> Self {
            Condvar(loom::sync::Condvar::new())
        }

        pub fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
            let held = guard.0.take().expect("held");
            guard.0 = Some(self.0.wait(held).expect("not poisoned"));
        }

        pub fn notify_all(&self) {
            self.0.notify_all();
        }
    }
}

/// How long a waiter spins, yielding, before it sleeps on a condition
/// variable. Waking a sleeping thread costs tens of microseconds — as much
/// as the work it waits for — and most waits here end sooner than this.
const SPIN: Duration = Duration::from_micros(50);

/// Wait for `done`, at most [`SPIN`], without sleeping: a few busy spins,
/// then yielding the core on every check, so a waiting writer hands its CPU
/// to the flush and compaction threads instead of burning it. Returns
/// whether `done` became true.
///
/// Under `loom` it only checks once: spinning multiplies the interleavings
/// without adding any, and every caller falls back on a condition variable.
#[cfg(not(loom))]
pub fn spin_until(done: impl Fn() -> bool) -> bool {
    use std::sync::atomic::Ordering;
    use std::sync::OnceLock;
    use std::time::Instant;
    // At most one spinner per core. Past that (SoliDB's SDBQL writes run 64
    // writers on 8 cores), spinners only take the CPU from the threads they
    // wait for: the profile was kernel, clock reads and sched_yield, at 4.6x
    // RocksDB's CPU per insert. The rest go straight to sleep.
    static SPINNERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    static CORES: OnceLock<usize> = OnceLock::new();
    let cores = *CORES.get_or_init(|| std::thread::available_parallelism().map_or(4, |n| n.get()));
    if SPINNERS.fetch_add(1, Ordering::Relaxed) >= cores {
        SPINNERS.fetch_sub(1, Ordering::Relaxed);
        return done();
    }
    let spun = (|| {
        for _ in 0..64 {
            if done() {
                return true;
            }
            std::hint::spin_loop();
        }
        let start = Instant::now();
        while start.elapsed() < SPIN {
            if done() {
                return true;
            }
            std::thread::yield_now();
        }
        done()
    })();
    SPINNERS.fetch_sub(1, Ordering::Relaxed);
    spun
}

#[cfg(loom)]
pub fn spin_until(done: impl Fn() -> bool) -> bool {
    let _ = SPIN;
    done()
}

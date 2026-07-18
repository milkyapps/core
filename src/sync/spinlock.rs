use crate::sync::atomic::AtomicUsize;
use std::{hint::spin_loop, sync::atomic::Ordering};

/// Spinlock guarantees unique access by spinning the CPU.
pub struct Spinlock {
    #[cfg(not(loom))]
    lock: AtomicUsize,
    #[cfg(loom)]
    inner: loom::sync::Mutex<()>,
}

impl Default for Spinlock {
    #[cfg(not(loom))]
    fn default() -> Self {
        Self {
            lock: AtomicUsize::default(),
        }
    }

    #[cfg(loom)]
    fn default() -> Self {
        Self {
            inner: loom::sync::Mutex::new(()),
        }
    }
}

impl Spinlock {
    /// Spins until the exclusive access is guaranteed.
    #[cfg(not(loom))]
    pub fn lock(&self) -> Guard<'_> {
        while self
            .lock
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            spin_loop();
        }

        Guard { lock: self }
    }

    #[cfg(loom)]
    pub fn lock(&self) -> Guard<'_> {
        let guard = self.lock.lock().unwrap();
        Guard { guard }
    }
}

/// Spinlock RAII guard.
pub struct Guard<'a> {
    #[cfg(not(loom))]
    lock: &'a Spinlock,
    #[cfg(loom)]
    guard: loom::sync::MutexGuard<'a>,
}

impl Drop for Guard<'_> {
    #[cfg(not(loom))]
    fn drop(&mut self) {
        unsafe {
            self.lock
                .lock
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                .unwrap_unchecked();
        }
    }

    #[cfg(loom)]
    fn drop(&mut self) {
        self.guard.drop();
    }
}

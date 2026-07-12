//! Re-exports [`std::sync`] (and [`std::cell::UnsafeCell`]) in normal builds, and
//! the equivalent [`loom`] primitives when the `loom` cfg is enabled.

pub mod channel;

// `UnsafeCell` is always re-exported from `std`: loom's `UnsafeCell` exposes a
// different API (its `get` returns `ConstPtr`/`MutPtr` rather than `*mut T`) and
// the hazard pointers code relies on `*mut T` from `get`. The concurrent
// mutation of the cell's contents happens through the (loom-modeled) atomics
// stored inside it, so keeping `std::cell::UnsafeCell` here does not weaken
// loom's modeling.
pub use std::cell::UnsafeCell;

#[cfg(not(loom))]
#[allow(unused_imports)]
pub(crate) use std::sync::{
    Arc, Barrier,
    atomic::{self, AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

#[cfg(loom)]
#[allow(unused_imports)]
pub use loom::sync::{
    Arc,
    atomic::{self, AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

// `loom::sync::Barrier` is currently only a stub that panics, so under the
// `loom` cfg a real implementation backed by `loom::sync::Mutex` and
// `loom::sync::Condvar` is provided instead.
#[cfg(loom)]
mod barrier {
    //! A loom-modelable [`Barrier`].
    //!
    //! `loom::sync::Barrier` is currently only a stub that panics, so a real
    //! implementation backed by `loom::sync::Mutex` and `loom::sync::Condvar` is
    //! provided here, mirroring [`std::sync::Barrier`].

    use loom::sync::{Condvar, Mutex};

    struct State {
        /// Number of threads still required to reach the barrier.
        count: usize,
        /// Current barrier generation, incremented each time the barrier is tripped.
        generation: usize,
    }

    /// A reusable barrier, mirroring [`std::sync::Barrier`].
    pub struct Barrier {
        n: usize,
        state: Mutex<State>,
        cvar: Condvar,
    }

    impl Barrier {
        /// Creates a new barrier that blocks until `n` threads have called
        /// [`Barrier::wait`].
        #[must_use]
        pub fn new(n: usize) -> Barrier {
            Barrier {
                n,
                state: Mutex::new(State {
                    count: 0,
                    generation: 0,
                }),
                cvar: Condvar::new(),
            }
        }

        /// Blocks the current thread until all `n` threads have called `wait`.
        ///
        /// # Panics
        ///
        /// Panics if the internal [`Mutex`] or [`Condvar`] is poisoned. This never
        /// happens under loom (which runs on a single OS thread), and mirrors the
        /// poisoning behavior of [`std::sync::Barrier`].
        pub fn wait(&self) {
            if self.n == 0 {
                return;
            }

            let mut guard = self.state.lock().unwrap();
            let generation = guard.generation;
            guard.count += 1;
            if guard.count == self.n {
                guard.count = 0;
                guard.generation = generation + 1;
                self.cvar.notify_all();
            } else {
                while guard.generation == generation {
                    guard = self.cvar.wait(guard).unwrap();
                }
            }
        }
    }
}

#[cfg(loom)]
pub use barrier::*;

/// Runs `f` inside a loom model when the `loom` cfg is enabled, otherwise
/// runs `f` directly. Test bodies run inside the model so the loom-modeled
/// atomics have a runtime to execute against.
#[cfg(loom)]
#[cfg(test)]
pub(crate) fn model<F>(f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    loom::model(f);
}

#[cfg(not(loom))]
#[cfg(test)]
pub(crate) fn model<F>(f: F)
where
    F: FnOnce(),
{
    f();
}

#[cfg(test)]
mod tests {
    use crate::sync::{Barrier, atomic::AtomicUsize, model};
    use std::sync::atomic::Ordering;

    /// A barrier must be reusable across generations: N threads passing it
    /// G times must all complete (loom flags a deadlock if the custom
    /// barrier impl hangs).
    #[test]
    fn barrier_reusable_across_generations() {
        model(|| {
            const N: usize = 3;
            const GENS: usize = 3;
            let barrier = Barrier::new(N);
            let phase = AtomicUsize::new(0);

            crate::thread::scope(|scope| {
                for _ in 0..N {
                    scope.spawn(|| {
                        for _ in 0..GENS {
                            barrier.wait();
                            phase.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                }
            });

            assert_eq!(
                phase.load(Ordering::SeqCst),
                N * GENS,
                "all threads must complete all generations (no deadlock)"
            );
        });
    }

    /// No thread may pass the barrier before all N have arrived. Each thread
    /// increments `before` *then* waits; after `wait` returns, `before` must
    /// read N. A barrier that releases early would let a thread observe
    /// `before < N` and panic.
    #[test]
    fn barrier_no_thread_passes_before_all_arrive() {
        model(|| {
            const N: usize = 3;
            let barrier = Barrier::new(N);
            let before = AtomicUsize::new(0);

            crate::thread::scope(|scope| {
                for _ in 0..N {
                    scope.spawn(|| {
                        before.fetch_add(1, Ordering::SeqCst);
                        barrier.wait();
                        assert_eq!(
                            before.load(Ordering::SeqCst),
                            N,
                            "a thread passed the barrier before all threads arrived"
                        );
                    });
                }
            });
        });
    }

    /// A barrier of size 1 is a no-op pass-through and must not deadlock.
    #[test]
    fn barrier_of_one_does_not_block() {
        model(|| {
            let barrier = Barrier::new(1);
            let count = AtomicUsize::new(0);
            crate::thread::scope(|scope| {
                scope.spawn(|| {
                    for _ in 0..4 {
                        barrier.wait();
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                });
            });
            assert_eq!(count.load(Ordering::SeqCst), 4);
        });
    }

    /// A barrier of size 0 is a degenerate no-op: there are zero threads to
    /// synchronize, so `wait` must return immediately.
    #[test]
    fn barrier_of_zero_is_no_op() {
        model(|| {
            use std::sync::Arc;
            use std::sync::mpsc;
            use std::time::{Duration, Instant};

            let barrier = Arc::new(Barrier::new(0));
            let (tx, rx) = mpsc::channel();

            let barrier_clone = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier_clone.wait();
                let _ = tx.send(());
            });

            let start = Instant::now();
            let result = rx.recv_timeout(Duration::from_millis(200));
            assert!(
                result.is_ok(),
                "Barrier::new(0) should return immediately, but wait() deadlocked for at least {:?}",
                start.elapsed()
            );
        });
    }
}
